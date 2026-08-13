// Package embedding 管理可复用的文本向量客户端及其调用边界。
package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"

	openaiembedding "github.com/cloudwego/eino-ext/components/embedding/openai"
	einocomponent "github.com/cloudwego/eino/components/embedding"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
)

var (
	// ErrNilContext 表示向量调用没有可用的 context。
	ErrNilContext = errors.New("Embedding context 不能为空")
	// ErrInvalidConfig 表示客户端配置不满足模型或并发约束。
	ErrInvalidConfig = errors.New("Embedding Client 配置无效")
	// ErrInvalidInput 表示待向量化文本为空或包含空白项。
	ErrInvalidInput = errors.New("Embedding 输入无效")
	// ErrRequest 表示模型服务调用失败。
	ErrRequest = errors.New("Embedding 请求失败")
	// ErrInvalidResponse 表示模型响应缺少用量或向量不满足契约。
	ErrInvalidResponse = errors.New("Embedding 响应无效")
)

const (
	// ModelTextEmbeddingV4 是 Client 唯一支持的模型空间。
	ModelTextEmbeddingV4 = appconfig.RequiredEmbeddingModel
	maxResponseBytes     = 8 << 20
)

// OperationError 隐藏服务端错误正文，同时保留 errors.Is/errors.As 错误链。
type OperationError struct {
	operation error
	cause     error
}

func (e *OperationError) Error() string {
	if e == nil || e.operation == nil {
		return ""
	}
	return e.operation.Error()
}

func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *OperationError) Is(target error) bool {
	return e != nil && target == e.operation
}

func (e *OperationError) GoString() string {
	return fmt.Sprintf("OperationError{%s}", e.Error())
}

// Result 保存一条输入对应的向量和服务端计量结果。
type Result struct {
	Vector     []float64
	TokenCount int
}

// Embedder 是 Client 对 Eino Embedding 组件依赖的最小接口。
type Embedder interface {
	EmbedStrings(ctx context.Context, texts []string, opts ...einocomponent.Option) ([][]float64, error)
}

// Client 为每条输入生成固定维度向量并保留精确 Token 用量。
type Client struct {
	embedder    Embedder
	dimensions  int
	concurrency int
}

// New 创建 text-embedding-v4 的 OpenAI 兼容客户端。
func New(ctx context.Context, config appconfig.Embedding) (*Client, error) {
	return newWithTransport(ctx, config, http.DefaultTransport)
}

func newWithTransport(
	ctx context.Context,
	config appconfig.Embedding,
	transport http.RoundTripper,
) (*Client, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if isNil(transport) {
		return nil, ErrInvalidConfig
	}
	dimensions := config.Dimensions()
	format := openaiembedding.EmbeddingEncodingFormatFloat
	httpClient := &http.Client{
		Timeout: config.Timeout(),
		Transport: &usageTransport{
			base: transport,
		},
	}
	embedder, err := openaiembedding.NewEmbedder(ctx, &openaiembedding.EmbeddingConfig{
		APIKey:         config.Key().Reveal(),
		BaseURL:        config.BaseURL(),
		Model:          config.Model(),
		Dimensions:     &dimensions,
		HTTPClient:     httpClient,
		EncodingFormat: &format,
	})
	if err != nil {
		return nil, operationError(ErrInvalidConfig, err)
	}
	return newClient(embedder, dimensions, config.BatchSize())
}

func newClient(embedder Embedder, dimensions, concurrency int) (*Client, error) {
	if isNil(embedder) || dimensions < 1 || concurrency < 1 || concurrency > appconfig.MaxEmbeddingBatchSize {
		return nil, ErrInvalidConfig
	}
	return &Client{
		embedder:    embedder,
		dimensions:  dimensions,
		concurrency: concurrency,
	}, nil
}

// Embed 为每条文本分别调用模型，使服务端请求级 Token 用量可准确映射到单条结果。
func (c *Client) Embed(ctx context.Context, texts []string) ([]Result, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if c == nil || isNil(c.embedder) || c.dimensions < 1 || c.concurrency < 1 {
		return nil, ErrInvalidConfig
	}
	if err := validateTexts(texts); err != nil {
		return nil, err
	}

	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]Result, len(texts))
	sem := make(chan struct{}, c.concurrency)
	var group sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for index, text := range texts {
		if requestCtx.Err() != nil {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-requestCtx.Done():
			break
		}
		if requestCtx.Err() != nil {
			break
		}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-sem }()
			result, err := c.embedOne(requestCtx, text)
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			results[index] = result
		}()
	}
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (c *Client) embedOne(ctx context.Context, text string) (Result, error) {
	usage := &usageCapture{}
	callCtx := context.WithValue(ctx, usageContextKey{}, usage)
	vectors, err := c.embedder.EmbedStrings(callCtx, []string{text})
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return Result{}, contextErr
		}
		return Result{}, operationError(ErrRequest, err)
	}
	if len(vectors) != 1 || len(vectors[0]) != c.dimensions || usage.promptTokens < 1 {
		return Result{}, ErrInvalidResponse
	}
	return Result{
		Vector:     append([]float64(nil), vectors[0]...),
		TokenCount: usage.promptTokens,
	}, nil
}

type usageContextKey struct{}

type usageCapture struct {
	promptTokens int
}

type usageTransport struct {
	base http.RoundTripper
}

func (t *usageTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil || response == nil || response.Body == nil {
		return response, err
	}
	capture, ok := request.Context().Value(usageContextKey{}).(*usageCapture)
	if !ok || capture == nil {
		return response, nil
	}
	if response.ContentLength > maxResponseBytes {
		_ = response.Body.Close()
		return nil, ErrInvalidResponse
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		_ = response.Body.Close()
		return nil, err
	}
	if err := response.Body.Close(); err != nil {
		return nil, err
	}
	if len(body) > maxResponseBytes {
		return nil, ErrInvalidResponse
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	var payload struct {
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &payload) == nil {
		capture.promptTokens = payload.Usage.PromptTokens
	}
	return response, nil
}

func validateConfig(config appconfig.Embedding) error {
	if strings.TrimSpace(config.BaseURL()) == "" ||
		strings.TrimSpace(config.Key().Reveal()) == "" ||
		config.Model() != ModelTextEmbeddingV4 ||
		config.Dimensions() != appconfig.RequiredEmbeddingDimensions ||
		config.Timeout() <= 0 ||
		config.BatchSize() < 1 ||
		config.BatchSize() > appconfig.MaxEmbeddingBatchSize {
		return ErrInvalidConfig
	}
	return nil
}

func validateTexts(texts []string) error {
	if len(texts) == 0 {
		return ErrInvalidInput
	}
	for _, text := range texts {
		if strings.TrimSpace(text) == "" {
			return ErrInvalidInput
		}
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

func operationError(operation, cause error) error {
	if cause == nil {
		return operation
	}
	return &OperationError{operation: operation, cause: cause}
}
