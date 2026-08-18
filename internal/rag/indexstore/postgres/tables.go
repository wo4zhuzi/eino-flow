package postgres

import (
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgvector/pgvector-go"
)

const (
	chunkSetsTable       = "chunk_sets"
	chunksTable          = "chunks"
	chunkEmbeddingsTable = "chunk_embeddings"
)

type chunkSetModel struct {
	ID              string     `gorm:"column:id;type:uuid;primaryKey"`
	TenantID        string     `gorm:"column:tenant_id"`
	KnowledgeBaseID string     `gorm:"column:knowledge_base_id"`
	DocumentID      string     `gorm:"column:document_id"`
	SourceURI       string     `gorm:"column:source_uri"`
	SourceName      string     `gorm:"column:source_name"`
	ContentSHA256   string     `gorm:"column:content_sha256"`
	StrategyName    string     `gorm:"column:strategy_name"`
	ProfileName     string     `gorm:"column:profile_name"`
	ProfileVersion  string     `gorm:"column:profile_version"`
	Config          []byte     `gorm:"column:config;type:jsonb"`
	Status          string     `gorm:"column:status"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	ActivatedAt     *time.Time `gorm:"column:activated_at"`
}

func (chunkSetModel) TableName() string {
	return chunkSetsTable
}

type chunkModel struct {
	ChunkSetID      string    `gorm:"column:chunk_set_id;type:uuid;primaryKey"`
	ChunkID         string    `gorm:"column:chunk_id;primaryKey"`
	Kind            string    `gorm:"column:kind"`
	Level           int       `gorm:"column:level"`
	ParentChunkID   *string   `gorm:"column:parent_chunk_id"`
	PreviousChunkID *string   `gorm:"column:previous_chunk_id"`
	NextChunkID     *string   `gorm:"column:next_chunk_id"`
	Sequence        int       `gorm:"column:sequence"`
	Content         string    `gorm:"column:content"`
	CharacterCount  int       `gorm:"column:character_count"`
	TokenCount      int       `gorm:"column:token_count"`
	SourceUnitIDs   textArray `gorm:"column:source_unit_ids;type:text[]"`
	Metadata        []byte    `gorm:"column:metadata;type:jsonb"`
	CreatedAt       time.Time `gorm:"column:created_at"`
}

func (chunkModel) TableName() string {
	return chunksTable
}

type chunkEmbeddingModel struct {
	ChunkSetID          string          `gorm:"column:chunk_set_id;type:uuid;primaryKey"`
	ChunkID             string          `gorm:"column:chunk_id;primaryKey"`
	ModelKey            string          `gorm:"column:model_key;primaryKey"`
	EmbeddingText       string          `gorm:"column:embedding_text"`
	EmbeddingTokenCount int             `gorm:"column:embedding_token_count"`
	InputSHA256         string          `gorm:"column:input_sha256"`
	Embedding           pgvector.Vector `gorm:"column:embedding;type:vector(1536)"`
	Searchable          bool            `gorm:"column:searchable"`
	CreatedAt           time.Time       `gorm:"column:created_at"`
	UpdatedAt           time.Time       `gorm:"column:updated_at"`
}

func (chunkEmbeddingModel) TableName() string {
	return chunkEmbeddingsTable
}

type textArray []string

var textArrayMapPool = sync.Pool{New: func() any { return pgtype.NewMap() }}

func (values textArray) Value() (driver.Value, error) {
	typeMap := textArrayMapPool.Get().(*pgtype.Map)
	defer textArrayMapPool.Put(typeMap)
	encoded, err := typeMap.Encode(
		pgtype.TextArrayOID,
		pgtype.TextFormatCode,
		pgtype.FlatArray[string](values),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("编码 PostgreSQL text[]: %w", err)
	}
	return encoded, nil
}

func (values *textArray) Scan(source any) error {
	if values == nil {
		return fmt.Errorf("扫描 PostgreSQL text[]: 目标不能为空")
	}
	if source == nil {
		*values = nil
		return nil
	}
	var encoded []byte
	switch typed := source.(type) {
	case string:
		encoded = []byte(typed)
	case []byte:
		encoded = typed
	default:
		return fmt.Errorf("扫描 PostgreSQL text[]: 不支持来源类型 %T", source)
	}

	typeMap := textArrayMapPool.Get().(*pgtype.Map)
	defer textArrayMapPool.Put(typeMap)
	var decoded pgtype.FlatArray[string]
	if err := typeMap.Scan(pgtype.TextArrayOID, pgtype.TextFormatCode, encoded, &decoded); err != nil {
		return fmt.Errorf("扫描 PostgreSQL text[]: %w", err)
	}
	*values = append((*values)[:0], decoded...)
	return nil
}
