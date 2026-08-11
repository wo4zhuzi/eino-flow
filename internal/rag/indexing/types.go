package indexing

import (
	"context"
	"errors"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
)

const (
	StageStatusCompleted = "completed"
	StageStatusSimulated = "simulated"
)

var (
	ErrNilContext          = errors.New("context 不能为空")
	ErrInvalidDependencies = errors.New("索引工作流依赖无效")
	ErrInvalidChunkConfig  = errors.New("Chunk 配置无效")
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

// Dependencies 保存由应用启动层创建和管理的外部依赖。
type Dependencies struct {
	Ingestor Ingestor
	Chunker  Chunker
}

// Request 是一次文档索引工作流请求。
type Request struct {
	RunID     string `json:"run_id"`
	SourceURI string `json:"source_uri"`
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

// StageResult 明确标记一个阶段是真实完成还是模拟执行。
type StageResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

// PlaceholderResult 描述尚未接入的 Chunk 下游能力。
type PlaceholderResult struct {
	Simulated             bool   `json:"simulated"`
	EmbeddingModel        string `json:"embedding_model"`
	VectorDimension       int    `json:"vector_dimension"`
	PersistedRecordCount  int    `json:"persisted_record_count"`
	ValidationPassed      bool   `json:"validation_passed"`
	PublishedIndexVersion string `json:"published_index_version"`
}

// Result 是文档索引工作流的最终输出。
type Result struct {
	Workflow    string               `json:"workflow"`
	RunID       string               `json:"run_id"`
	Status      string               `json:"status"`
	Source      SourceInfo           `json:"source"`
	Parser      ingestion.ParserInfo `json:"parser"`
	Chunking    *chunking.Result     `json:"chunking"`
	Stages      []StageResult        `json:"stages"`
	Placeholder PlaceholderResult    `json:"placeholder"`
}

type workflowState struct {
	request     Request
	source      SourceInfo
	ingested    *ingestion.Result
	chunking    *chunking.Result
	stages      []StageResult
	placeholder PlaceholderResult
}
