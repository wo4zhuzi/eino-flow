package postgres

import (
	"time"

	"github.com/pgvector/pgvector-go"
)

const (
	chunkSetsTable       = "chunk_sets"
	chunksTable          = "chunks"
	chunkEmbeddingsTable = "chunk_embeddings"
)

type chunkSet struct {
	ID              string     `gorm:"column:id;type:uuid;primaryKey"`
	TenantID        string     `gorm:"column:tenant_id"`
	KnowledgeBaseID string     `gorm:"column:knowledge_base_id"`
	DocumentID      string     `gorm:"column:document_id"`
	StrategyName    string     `gorm:"column:strategy_name"`
	ProfileName     string     `gorm:"column:profile_name"`
	ProfileVersion  string     `gorm:"column:profile_version"`
	Config          []byte     `gorm:"column:config;type:jsonb"`
	Status          string     `gorm:"column:status"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	ActivatedAt     *time.Time `gorm:"column:activated_at"`
}

func (chunkSet) TableName() string {
	return chunkSetsTable
}

type chunk struct {
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
	SourceUnitIDs   []string  `gorm:"column:source_unit_ids;type:text[]"`
	Metadata        []byte    `gorm:"column:metadata;type:jsonb"`
	CreatedAt       time.Time `gorm:"column:created_at"`
}

func (chunk) TableName() string {
	return chunksTable
}

type chunkEmbedding struct {
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

func (chunkEmbedding) TableName() string {
	return chunkEmbeddingsTable
}
