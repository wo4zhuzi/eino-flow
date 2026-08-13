package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	einocomponent "github.com/cloudwego/eino/components/embedding"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
)

type stubEmbedder struct {
	mu        sync.Mutex
	dimension int
	active    int
	maxActive int
	requests  []string
	err       error
}

func (s *stubEmbedder) EmbedStrings(
	ctx context.Context,
	texts []string,
	_ ...einocomponent.Option,
) ([][]float64, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.mu.Lock()
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.requests = append(s.requests, texts...)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()

	vectors := make([][]float64, len(texts))
	for index, text := range texts {
		vectors[index] = make([]float64, s.dimension)
		vectors[index][0] = float64(len(text))
	}
	recordUsage(ctx, len([]rune(texts[0])))
	return vectors, nil
}

func recordUsage(ctx context.Context, promptTokens int) {
	if capture, ok := ctx.Value(usageContextKey{}).(*usageCapture); ok && capture != nil {
		capture.promptTokens = promptTokens
	}
}

func TestClientEmbedsEachInputWithExactUsageAndStableOrder(t *testing.T) {
	embedder := &stubEmbedder{dimension: 3}
	client, err := newClient(embedder, 3, 2)
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	results, err := client.Embed(context.Background(), []string{"甲", "second", "第三条"})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	wantTokens := []int{1, 6, 3}
	wantFirst := []float64{float64(len("甲")), float64(len("second")), float64(len("第三条"))}
	for index, result := range results {
		if result.TokenCount != wantTokens[index] || len(result.Vector) != 3 || result.Vector[0] != wantFirst[index] {
			t.Fatalf("result[%d] = %#v", index, result)
		}
	}
	embedder.mu.Lock()
	defer embedder.mu.Unlock()
	if len(embedder.requests) != 3 || embedder.maxActive > 2 {
		t.Fatalf("requests=%#v maxActive=%d", embedder.requests, embedder.maxActive)
	}
}

func TestClientRejectsInvalidBoundaries(t *testing.T) {
	embedder := &stubEmbedder{dimension: 3}
	if _, err := newClient(nil, 3, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("newClient(nil) error = %v", err)
	}
	if _, err := newClient(embedder, 3, 11); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("newClient(concurrency=11) error = %v", err)
	}
	client, err := newClient(embedder, 3, 1)
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	if _, err := client.Embed(nil, []string{"text"}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Embed(nil) error = %v", err)
	}
	for _, texts := range [][]string{nil, {}, {""}, {"valid", "  "}} {
		if _, err := client.Embed(context.Background(), texts); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Embed(%#v) error = %v", texts, err)
		}
	}
	var unavailable *Client
	if _, err := unavailable.Embed(context.Background(), []string{"text"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Client.Embed() error = %v", err)
	}
}

func TestClientPreservesRequestErrorsAndValidatesResponse(t *testing.T) {
	cause := errors.New("provider unavailable with sensitive response text")
	client, err := newClient(&stubEmbedder{dimension: 3, err: cause}, 3, 1)
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	if _, err := client.Embed(context.Background(), []string{"text"}); !errors.Is(err, ErrRequest) || !errors.Is(err, cause) {
		t.Fatalf("Embed(request error) = %v", err)
	} else if err.Error() != ErrRequest.Error() {
		t.Fatalf("Embed(request error) leaked cause: %v", err)
	} else if formatted := fmt.Sprintf("%#v", err); strings.Contains(formatted, cause.Error()) {
		t.Fatalf("Embed(request error) GoString leaked cause: %s", formatted)
	}

	client, err = newClient(&stubEmbedder{dimension: 2}, 3, 1)
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	if _, err := client.Embed(context.Background(), []string{"text"}); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Embed(invalid response) = %v", err)
	}
}

func TestClientPreservesContextCancellation(t *testing.T) {
	client, err := newClient(&blockingEmbedder{}, 3, 1)
	if err != nil {
		t.Fatalf("newClient() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Embed(ctx, []string{"text"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Embed(canceled) error = %v", err)
	}
}

type blockingEmbedder struct{}

func (*blockingEmbedder) EmbedStrings(
	ctx context.Context,
	_ []string,
	_ ...einocomponent.Option,
) ([][]float64, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestUsageTransportRejectsOversizedResponse(t *testing.T) {
	transport := &usageTransport{base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: maxResponseBytes + 1,
			Body:          io.NopCloser(strings.NewReader("{}")),
			Request:       request,
		}, nil
	})}
	request, err := http.NewRequestWithContext(
		context.WithValue(context.Background(), usageContextKey{}, &usageCapture{}),
		http.MethodPost,
		"https://embedding.example/v1/embeddings",
		nil,
	)
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("RoundTrip() error = %v", err)
	}
}

func TestClientUsesOpenAICompatibleProtocolAndCapturesProviderUsage(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/embeddings" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		var payload struct {
			Model      string   `json:"model"`
			Input      []string `json:"input"`
			Dimensions int      `json:"dimensions"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.Model != "text-embedding-v4" || payload.Dimensions != 1536 || len(payload.Input) != 1 || payload.Input[0] != "测试文本" {
			t.Fatalf("payload = %#v", payload)
		}
		vector := make([]float64, 1536)
		vector[0] = 0.25
		var response strings.Builder
		if err := json.NewEncoder(&response).Encode(map[string]any{
			"data": []any{map[string]any{
				"embedding": vector,
				"index":     0,
				"object":    "embedding",
			}},
			"model":  "text-embedding-v4",
			"object": "list",
			"usage":  map[string]int{"prompt_tokens": 7, "total_tokens": 7},
		}); err != nil {
			t.Fatalf("encode response: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(response.String())),
			Request:    request,
		}, nil
	})

	client, err := newWithTransport(
		context.Background(),
		embeddingTestConfig(t, "https://embedding.example/v1"),
		transport,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	results, err := client.Embed(context.Background(), []string{"测试文本"})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if len(results) != 1 || len(results[0].Vector) != 1536 || results[0].Vector[0] != 0.25 || results[0].TokenCount != 7 {
		t.Fatalf("results = %#v", results)
	}
}

type roundTripFunc func(request *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func embeddingTestConfig(t *testing.T, baseURL string) appconfig.Embedding {
	t.Helper()
	values := map[string]string{
		appconfig.EnvPostgresHost:       "127.0.0.1",
		appconfig.EnvPostgresUser:       "application",
		appconfig.EnvPostgresPassword:   "database-secret",
		appconfig.EnvPostgresDB:         "knowledge",
		appconfig.EnvEmbeddingBaseURL:   baseURL,
		appconfig.EnvEmbeddingKey:       "test-key",
		appconfig.EnvEmbeddingModel:     "text-embedding-v4",
		appconfig.EnvEmbeddingTimeout:   time.Second.String(),
		appconfig.EnvEmbeddingBatchSize: "2",
	}
	configuration, err := appconfig.LoadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("config.LoadFrom() error = %v", err)
	}
	return configuration.Embedding()
}
