package indexing

import (
	"context"
	"errors"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

const (
	// StageStatusCompleted 表示阶段已执行真实逻辑并成功完成。
	StageStatusCompleted = "completed"
)

var (
	ErrNilContext          = errors.New("context 不能为空")
	ErrInvalidDependencies = errors.New("索引工作流依赖无效")
	ErrInvalidChunkConfig  = errors.New("Chunk 配置无效")
	// ErrInvalidRequest 表示请求缺少可信索引目标字段。
	ErrInvalidRequest = errors.New("索引工作流请求无效")
	// ErrInvalidEmbedding 表示 Embedder 或 Store 返回了不满足工作流契约的数据。
	ErrInvalidEmbedding    = errors.New("Embedding 结果无效")
	ErrInvalidRunID        = errors.New("run_id 不能为空")
	ErrWorkflowUnavailable = errors.New("索引工作流未初始化")
)

// Ingestor 定义工作流依赖的最小文档摄取能力。
type Ingestor interface {
	Ingest(ctx context.Context, uri string) (*ingestion.Result, error)
}

// Chunker 定义从标准摄取结果生成 Chunk 的最小能力。
type Chunker interface {
	Chunk(ctx context.Context, result *ingestion.Result) (*chunking.Result, error)
}

// Embedder 定义工作流需要的文本向量生成能力。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([]embedding.Result, error)
}

// BuildConfig 保存由启动层提供的可复现构建配置。
type BuildConfig struct {
	Chunk ChunkConfig
	Model ModelProfile
}

// Dependencies 保存由应用启动层创建和管理的外部依赖。
type Dependencies struct {
	Ingestor    Ingestor
	Chunker     Chunker
	Embedder    Embedder
	Store       Store
	BuildConfig BuildConfig
}

// IndexTarget 保存一次构建的可信作用域、版本和文档标识。
type IndexTarget struct {
	SetID           indexstore.SetID `json:"set_id"`
	TenantID        string           `json:"tenant_id"`
	KnowledgeBaseID string           `json:"knowledge_base_id"`
	DocumentID      string           `json:"document_id"`
	CanonicalURI    string           `json:"canonical_uri"`
	SourceName      string           `json:"source_name"`
	Title           string           `json:"title,omitempty"`
}

// Request 是一次文档索引工作流请求。
type Request struct {
	RunID     string      `json:"run_id"`
	SourceURI string      `json:"source_uri"`
	Index     IndexTarget `json:"index"`
}

// SourceInfo 是摄取结果附加索引标识后的数据源快照。
type SourceInfo struct {
	URI         string `json:"uri"`
	ResolvedURI string `json:"resolved_uri,omitempty"`
	FileName    string `json:"file_name"`
	Extension   string `json:"extension"`
	MIMEType    string `json:"mime_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
	DocumentID  string `json:"document_id"`
	VersionID   string `json:"version_id"`
}

// StageResult 描述一个工作流阶段的执行状态和结果摘要。
type StageResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

// IndexResult 描述已持久化并发布的索引版本。
type IndexResult struct {
	SetID                   indexstore.SetID `json:"set_id"`
	EmbeddingModel          string           `json:"embedding_model"`
	VectorDimension         int              `json:"vector_dimension"`
	ChunkCount              int              `json:"chunk_count"`
	EmbeddingCount          int              `json:"embedding_count"`
	GeneratedEmbeddingCount int              `json:"generated_embedding_count"`
	ReusedEmbeddingCount    int              `json:"reused_embedding_count"`
	ValidationPassed        bool             `json:"validation_passed"`
	Published               bool             `json:"published"`
}

// Result 是文档索引工作流的最终输出。
type Result struct {
	Workflow string               `json:"workflow"`
	RunID    string               `json:"run_id"`
	Status   string               `json:"status"`
	Source   SourceInfo           `json:"source"`
	Parser   ingestion.ParserInfo `json:"parser"`
	Chunking *chunking.Result     `json:"chunking"`
	Stages   []StageResult        `json:"stages"`
	Index    IndexResult          `json:"index"`
}

type workflowState struct {
	request          Request
	source           SourceInfo
	ingested         *ingestion.Result
	chunking         *chunking.Result
	build            BuildData
	pendingEmbedding []EmbeddingInput
	embeddingRecords []EmbeddingRecord
	stages           []StageResult
	index            IndexResult
}
