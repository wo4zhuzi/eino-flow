package indexing

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

const (
	// DistanceCosine 表示模型空间使用余弦距离。
	DistanceCosine = "cosine"
	// EmbeddingInputPolicyV1 是当前 Embedding 文本拼装规则的稳定版本。
	EmbeddingInputPolicyV1 = "v1"
)

var (
	// ErrInvalidBuild 表示索引构建数据不满足领域契约，不应直接重试。
	ErrInvalidBuild = errors.New("索引构建数据无效")
	// ErrBuildConflict 表示同一个 Set ID 已关联另一份不可兼容的构建数据。
	ErrBuildConflict = errors.New("索引构建冲突")
	// ErrStoreUnavailable 表示 Index Store 暂时无法完成操作，可以按任务策略重试。
	ErrStoreUnavailable = errors.New("Index Store 暂时不可用")
)

// Document 描述一次构建所引用的逻辑文档与安全来源。
type Document struct {
	ID            string
	SourceURI     string
	SourceName    string
	Title         string
	ContentSHA256 string
}

// Profile 标识一份可复现的 Chunk 配置语义。
type Profile struct {
	Name    string
	Version string
}

// ModelProfile 描述影响向量兼容性的全部稳定模型属性。
type ModelProfile struct {
	Model         string
	Dimensions    int
	Distance      string
	ConfigVersion string
}

// ParentChildConfig 是 Parent-child 策略的完整入库配置快照。
type ParentChildConfig struct {
	ParentMaxRunes int
	ChildMaxRunes  int
}

// StructureAwareConfig 是 Structure-aware 策略的完整入库配置快照。
type StructureAwareConfig struct {
	MaxRunes       int
	MinRunes       int
	HeadingContext string
}

// BuildSpec 保存不能从 Chunk Metadata 推导的可信构建参数。
type BuildSpec struct {
	SetID          indexstore.SetID
	Scope          indexstore.Scope
	Document       Document
	Profile        Profile
	Model          ModelProfile
	ParentChild    *ParentChildConfig
	StructureAware *StructureAwareConfig
}

// SetRecord 描述一个处于 building 状态的索引版本。
type SetRecord struct {
	ID           indexstore.SetID
	Scope        indexstore.Scope
	Document     Document
	StrategyName string
	Profile      Profile
	Config       json.RawMessage
}

// ChunkRecord 是与数据库实现无关的可持久化 Chunk。
type ChunkRecord struct {
	Candidate       indexstore.CandidateID
	Kind            string
	Level           int
	ParentChunkID   *string
	PreviousChunkID *string
	NextChunkID     *string
	Sequence        int
	Content         string
	CharacterCount  int
	TokenCount      int
	SourceUnitIDs   []string
	Metadata        json.RawMessage
}

// EmbeddingInput 是需要交给 Embedder 的最终确定性输入。
type EmbeddingInput struct {
	Candidate   indexstore.CandidateID
	ModelKey    indexstore.ModelKey
	Text        string
	InputSHA256 string
}

// EmbeddingRecord 是模型调用完成后交给 Index Store 的写入记录。
type EmbeddingRecord struct {
	EmbeddingInput
	TokenCount int
	Vector     []float64
}

// BuildData 是一次 building Set 写入所需的完整领域数据。
type BuildData struct {
	Set             SetRecord
	Chunks          []ChunkRecord
	EmbeddingInputs []EmbeddingInput
}

// Store 定义索引构建使用方当前需要的最小 Index Store 能力。
type Store interface {
	PrepareBuild(ctx context.Context, build BuildData) ([]EmbeddingInput, error)
	SaveEmbeddings(ctx context.Context, setID indexstore.SetID, records []EmbeddingRecord) error
	Validate(ctx context.Context, setID indexstore.SetID) error
	Publish(ctx context.Context, setID indexstore.SetID) error
}
