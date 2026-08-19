// Package retrieval 实现 RAG 查询向量化与可信作用域内的证据召回。
package retrieval

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

const (
	// MaxQueryRunes 是单次查询允许的最大 Unicode 字符数。
	MaxQueryRunes = 4096
	// MaxTopK 是单次查询允许返回的最大候选数。
	MaxTopK = 20
)

var (
	// ErrNilContext 表示检索操作没有可用的 context。
	ErrNilContext = errors.New("检索 context 不能为空")
	// ErrInvalidRequest 表示查询文本、可信作用域或 TopK 不满足契约。
	ErrInvalidRequest = errors.New("检索请求无效")
	// ErrInvalidDependencies 表示检索工作流缺少必需依赖或模型配置无效。
	ErrInvalidDependencies = errors.New("检索工作流依赖无效")
	// ErrInvalidEmbedding 表示查询向量不满足模型空间契约。
	ErrInvalidEmbedding = errors.New("查询 Embedding 无效")
	// ErrInvalidEvidence 表示 Store 返回的证据不满足检索契约。
	ErrInvalidEvidence = errors.New("检索证据无效")
	// ErrStoreUnavailable 表示 Index Store 暂时无法执行检索。
	ErrStoreUnavailable = errors.New("Index Store 暂时不可用")
	// ErrWorkflowUnavailable 表示检索工作流尚未初始化。
	ErrWorkflowUnavailable = errors.New("检索工作流未初始化")
)

// Request 描述一次单知识库向量召回请求。
type Request struct {
	RunID string           `json:"run_id"`
	Query string           `json:"query"`
	Scope indexstore.Scope `json:"scope"`
	TopK  int              `json:"top_k"`
}

// SearchRequest 是 Store 执行一次作用域内向量检索所需的完整输入。
type SearchRequest struct {
	Scope    indexstore.Scope
	ModelKey indexstore.ModelKey
	Vector   []float64
	Limit    int
}

// Evidence 是与数据库映射细节无关的原始向量命中。
type Evidence struct {
	Candidate      indexstore.CandidateID `json:"candidate"`
	DocumentID     string                 `json:"document_id"`
	SourceURI      string                 `json:"source_uri"`
	SourceName     string                 `json:"source_name"`
	Kind           string                 `json:"kind"`
	Sequence       int                    `json:"sequence"`
	Content        string                 `json:"content"`
	TokenCount     int                    `json:"token_count"`
	SourceUnitIDs  []string               `json:"source_unit_ids"`
	Metadata       json.RawMessage        `json:"metadata"`
	CosineDistance float64                `json:"cosine_distance"`
}

// Result 描述一次基础向量召回的最终结果。
type Result struct {
	Workflow                 string              `json:"workflow"`
	RunID                    string              `json:"run_id"`
	Status                   string              `json:"status"`
	ModelKey                 indexstore.ModelKey `json:"model_key"`
	QueryEmbeddingTokenCount int                 `json:"query_embedding_token_count"`
	Evidence                 []Evidence          `json:"evidence"`
}

// Embedder 定义查询工作流需要的文本向量生成能力。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([]embedding.Result, error)
}

// Store 定义检索使用方需要的最小 Index Store 能力。
type Store interface {
	Search(ctx context.Context, request SearchRequest) ([]Evidence, error)
}

// Config 保存由启动层提供的查询模型空间配置。
type Config struct {
	Model indexstore.ModelProfile
}

// Dependencies 保存由应用启动层创建和管理的检索依赖。
type Dependencies struct {
	Embedder Embedder
	Store    Store
	Config   Config
}

type workflowState struct {
	request                  Request
	modelKey                 indexstore.ModelKey
	queryVector              []float64
	queryEmbeddingTokenCount int
	evidence                 []Evidence
}

type workflowNodes struct {
	embedder Embedder
	store    Store
	model    indexstore.ModelProfile
	modelKey indexstore.ModelKey
}
