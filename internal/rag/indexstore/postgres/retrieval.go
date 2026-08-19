package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pgvector/pgvector-go"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
)

var _ retrieval.Store = (*Store)(nil)

// Search 在可信作用域内返回已发布索引的最近向量证据。
func (s *Store) Search(ctx context.Context, request retrieval.SearchRequest) ([]retrieval.Evidence, error) {
	if ctx == nil {
		return nil, retrieval.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, &storeOperationError{operation: retrieval.ErrStoreUnavailable, cause: ErrUnavailable}
	}
	if err := retrieval.ValidateSearchRequest(request); err != nil {
		return nil, err
	}

	vector := make([]float32, len(request.Vector))
	for index, value := range request.Vector {
		vector[index] = float32(value)
	}
	rows := make([]retrievalRow, 0, request.Limit)
	err := s.db.WithContext(ctx).Raw(fmt.Sprintf(`
SELECT
    ce.chunk_set_id,
    ce.chunk_id,
    cs.document_id,
    cs.source_uri,
    cs.source_name,
    c.kind,
    c.sequence,
    c.content,
    c.token_count,
    c.source_unit_ids,
    c.metadata,
    ce.embedding <=> ? AS cosine_distance
FROM %s AS ce
JOIN %s AS cs ON cs.id = ce.chunk_set_id
JOIN %s AS c
  ON c.chunk_set_id = ce.chunk_set_id
 AND c.chunk_id = ce.chunk_id
WHERE cs.tenant_id = ?
  AND cs.knowledge_base_id = ?
  AND cs.status = 'active'
  AND ce.searchable = true
  AND ce.model_key = ?
ORDER BY cosine_distance ASC, ce.chunk_set_id ASC, ce.chunk_id ASC
LIMIT ?`, s.tables.ChunkEmbeddings, s.tables.ChunkSets, s.tables.Chunks),
		pgvector.NewVector(vector),
		request.Scope.TenantID,
		request.Scope.KnowledgeBaseID,
		string(request.ModelKey),
		request.Limit,
	).Scan(&rows).Error
	if err != nil {
		return nil, classifyRetrievalStoreError(err)
	}

	evidence := make([]retrieval.Evidence, len(rows))
	for index, row := range rows {
		evidence[index] = retrieval.Evidence{
			Candidate: indexstore.CandidateID{
				SetID:   indexstore.SetID(row.ChunkSetID),
				ChunkID: row.ChunkID,
			},
			DocumentID:     row.DocumentID,
			SourceURI:      row.SourceURI,
			SourceName:     row.SourceName,
			Kind:           row.Kind,
			Sequence:       row.Sequence,
			Content:        row.Content,
			TokenCount:     row.TokenCount,
			SourceUnitIDs:  append([]string(nil), row.SourceUnitIDs...),
			Metadata:       append(json.RawMessage(nil), row.Metadata...),
			CosineDistance: row.CosineDistance,
		}
	}
	if err := retrieval.ValidateEvidence(evidence, request.Limit); err != nil {
		return nil, err
	}
	return evidence, nil
}

type retrievalRow struct {
	ChunkSetID     string    `gorm:"column:chunk_set_id"`
	ChunkID        string    `gorm:"column:chunk_id"`
	DocumentID     string    `gorm:"column:document_id"`
	SourceURI      string    `gorm:"column:source_uri"`
	SourceName     string    `gorm:"column:source_name"`
	Kind           string    `gorm:"column:kind"`
	Sequence       int       `gorm:"column:sequence"`
	Content        string    `gorm:"column:content"`
	TokenCount     int       `gorm:"column:token_count"`
	SourceUnitIDs  textArray `gorm:"column:source_unit_ids"`
	Metadata       []byte    `gorm:"column:metadata"`
	CosineDistance float64   `gorm:"column:cosine_distance"`
}

func classifyRetrievalStoreError(err error) error {
	if err == nil || isContextError(err) {
		return err
	}
	return &storeOperationError{operation: retrieval.ErrStoreUnavailable, cause: err}
}
