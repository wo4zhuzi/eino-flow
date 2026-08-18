package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"

	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const publishLockNamespace = "eino-flow:index-publish:v1"

// Publish 校验持久化构建快照，并在同一事务中原子切换作用域内的 active Set。
func (s *Store) Publish(ctx context.Context, setID indexstore.SetID) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrUnavailable
	}
	if !uuidPattern.MatchString(string(setID)) {
		return invalidBuild("chunk_set_id 必须是 UUID")
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		scopeSet, err := s.findPublishSet(tx, setID, false)
		if err != nil {
			return err
		}
		lockKey := publishAdvisoryLockKey(
			indexstore.Scope{TenantID: scopeSet.TenantID, KnowledgeBaseID: scopeSet.KnowledgeBaseID},
			scopeSet.DocumentID,
			scopeSet.StrategyName,
		)
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", lockKey).Error; err != nil {
			return err
		}

		target, err := s.findPublishSet(tx, setID, true)
		if err != nil {
			return err
		}
		if !samePublishScope(scopeSet, target) {
			return buildConflict("目标 Set 的发布作用域发生变化")
		}
		if target.Status != setStatusBuilding || target.ActivatedAt != nil {
			return buildConflict("目标 Set 不是可发布的 building 状态")
		}
		if err := s.validateProfileConfig(tx, target); err != nil {
			return err
		}
		embeddingCount, err := s.validatePublishSnapshot(tx, target)
		if err != nil {
			return err
		}

		activeSets, err := s.lockActiveSets(tx, target)
		if err != nil {
			return err
		}
		if len(activeSets) > 1 {
			return buildConflict("发布作用域存在多个 active Set")
		}
		if len(activeSets) == 1 {
			activeID := activeSets[0].ID
			if err := tx.Table(s.tables.ChunkEmbeddings).
				Where("chunk_set_id = ? AND searchable = ?", activeID, true).
				Update("searchable", false).Error; err != nil {
				return err
			}
			result := tx.Table(s.tables.ChunkSets).
				Where("id = ? AND status = ?", activeID, setStatusActive).
				Update("status", setStatusRetired)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return buildConflict("旧 active Set 状态发生变化")
			}
		}

		result := tx.Table(s.tables.ChunkEmbeddings).
			Where("chunk_set_id = ? AND searchable = ?", string(setID), false).
			Update("searchable", true)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != int64(embeddingCount) {
			return buildConflict("目标 Set 的 Embedding 状态发生变化")
		}
		result = tx.Table(s.tables.ChunkSets).
			Where("id = ? AND status = ?", string(setID), setStatusBuilding).
			Updates(map[string]any{
				"status":       setStatusActive,
				"activated_at": gorm.Expr("CURRENT_TIMESTAMP"),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return buildConflict("目标 Set 状态发生变化")
		}
		return nil
	})
	return classifyStoreError(err)
}

func (s *Store) findPublishSet(tx *gorm.DB, setID indexstore.SetID, lock bool) (chunkSetModel, error) {
	query := tx.Table(s.tables.ChunkSets).Where("id = ?", string(setID))
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var set chunkSetModel
	if err := query.Take(&set).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return chunkSetModel{}, buildConflict("目标 Set 不存在")
	} else if err != nil {
		return chunkSetModel{}, err
	}
	return set, nil
}

func (s *Store) validateProfileConfig(tx *gorm.DB, target chunkSetModel) error {
	type profileConfigRow struct {
		Config []byte `gorm:"column:config"`
	}
	var rows []profileConfigRow
	if err := tx.Table(s.tables.ChunkSets).
		Select("config").
		Where(
			"strategy_name = ? AND profile_name = ? AND profile_version = ?",
			target.StrategyName,
			target.ProfileName,
			target.ProfileVersion,
		).
		Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		if !jsonEqual(target.Config, row.Config) {
			return invalidBuild("Profile 与规范化配置不一致")
		}
	}
	return nil
}

func (s *Store) validatePublishSnapshot(tx *gorm.DB, target chunkSetModel) (int, error) {
	type publishChunkRow struct {
		ChunkSetID        string  `gorm:"column:chunk_set_id"`
		ChunkID           string  `gorm:"column:chunk_id"`
		Kind              string  `gorm:"column:kind"`
		Level             int     `gorm:"column:level"`
		ParentChunkID     *string `gorm:"column:parent_chunk_id"`
		PreviousChunkID   *string `gorm:"column:previous_chunk_id"`
		NextChunkID       *string `gorm:"column:next_chunk_id"`
		Sequence          int     `gorm:"column:sequence"`
		Content           string  `gorm:"column:content"`
		CharacterCount    int     `gorm:"column:character_count"`
		TokenCount        int     `gorm:"column:token_count"`
		SourceUnitIDsJSON []byte  `gorm:"column:source_unit_ids_json"`
		Metadata          []byte  `gorm:"column:metadata"`
	}
	var chunks []publishChunkRow
	if err := tx.Table(s.tables.Chunks).
		Select(`chunk_set_id, chunk_id, kind, level, parent_chunk_id, previous_chunk_id,
			next_chunk_id, sequence, content, character_count, token_count,
			to_json(source_unit_ids) AS source_unit_ids_json, metadata`).
		Where("chunk_set_id = ?", target.ID).
		Order("sequence ASC").Find(&chunks).Error; err != nil {
		return 0, err
	}
	var embeddings []chunkEmbeddingModel
	if err := tx.Table(s.tables.ChunkEmbeddings).
		Where("chunk_set_id = ?", target.ID).
		Order("chunk_id ASC, model_key ASC").Find(&embeddings).Error; err != nil {
		return 0, err
	}

	build := indexing.BuildData{
		Set: indexing.SetRecord{
			ID:    indexstore.SetID(target.ID),
			Scope: indexstore.Scope{TenantID: target.TenantID, KnowledgeBaseID: target.KnowledgeBaseID},
			Document: indexing.Document{
				ID:            target.DocumentID,
				SourceURI:     target.SourceURI,
				SourceName:    target.SourceName,
				ContentSHA256: target.ContentSHA256,
			},
			StrategyName: target.StrategyName,
			Profile:      indexing.Profile{Name: target.ProfileName, Version: target.ProfileVersion},
			Config:       append([]byte(nil), target.Config...),
		},
		Chunks:          make([]indexing.ChunkRecord, len(chunks)),
		EmbeddingInputs: make([]indexing.EmbeddingInput, len(embeddings)),
	}
	for index, chunk := range chunks {
		var sourceUnitIDs []string
		if err := json.Unmarshal(chunk.SourceUnitIDsJSON, &sourceUnitIDs); err != nil {
			return 0, invalidBuild("Chunk 来源单元数组无效")
		}
		build.Chunks[index] = indexing.ChunkRecord{
			Candidate:       indexstore.CandidateID{SetID: build.Set.ID, ChunkID: chunk.ChunkID},
			Kind:            chunk.Kind,
			Level:           chunk.Level,
			ParentChunkID:   cloneStringPointer(chunk.ParentChunkID),
			PreviousChunkID: cloneStringPointer(chunk.PreviousChunkID),
			NextChunkID:     cloneStringPointer(chunk.NextChunkID),
			Sequence:        chunk.Sequence,
			Content:         chunk.Content,
			CharacterCount:  chunk.CharacterCount,
			TokenCount:      chunk.TokenCount,
			SourceUnitIDs:   sourceUnitIDs,
			Metadata:        append([]byte(nil), chunk.Metadata...),
		}
	}
	records := make([]indexing.EmbeddingRecord, len(embeddings))
	for index, embedding := range embeddings {
		if embedding.Searchable {
			return 0, invalidBuild("building Set 包含可检索 Embedding")
		}
		candidate := indexstore.CandidateID{SetID: build.Set.ID, ChunkID: embedding.ChunkID}
		input := indexing.EmbeddingInput{
			Candidate:   candidate,
			ModelKey:    indexstore.ModelKey(embedding.ModelKey),
			Text:        embedding.EmbeddingText,
			InputSHA256: embedding.InputSHA256,
		}
		build.EmbeddingInputs[index] = input
		vector := embedding.Embedding.Slice()
		values := make([]float64, len(vector))
		for valueIndex, value := range vector {
			values[valueIndex] = float64(value)
			if math.IsNaN(values[valueIndex]) || math.IsInf(values[valueIndex], 0) {
				return 0, invalidBuild("Embedding 向量包含非有限值")
			}
		}
		records[index] = indexing.EmbeddingRecord{
			EmbeddingInput: input,
			TokenCount:     embedding.EmbeddingTokenCount,
			Vector:         values,
		}
	}
	if err := indexing.ValidateBuildData(build); err != nil {
		return 0, err
	}
	if err := validateEmbeddingRecords(build.Set.ID, records); err != nil {
		return 0, err
	}
	return len(embeddings), nil
}

func (s *Store) lockActiveSets(tx *gorm.DB, target chunkSetModel) ([]chunkSetModel, error) {
	var active []chunkSetModel
	err := tx.Table(s.tables.ChunkSets).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(
			"tenant_id = ? AND knowledge_base_id = ? AND document_id = ? AND strategy_name = ? AND status = ?",
			target.TenantID,
			target.KnowledgeBaseID,
			target.DocumentID,
			target.StrategyName,
			setStatusActive,
		).
		Find(&active).Error
	return active, err
}

func samePublishScope(left, right chunkSetModel) bool {
	return left.TenantID == right.TenantID && left.KnowledgeBaseID == right.KnowledgeBaseID &&
		left.DocumentID == right.DocumentID && left.StrategyName == right.StrategyName
}

func publishAdvisoryLockKey(scope indexstore.Scope, documentID, strategyName string) int64 {
	digest := sha256.New()
	for _, field := range []string{
		publishLockNamespace,
		scope.TenantID,
		scope.KnowledgeBaseID,
		documentID,
		strategyName,
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(field))
	}
	sum := digest.Sum(nil)
	return int64(binary.BigEndian.Uint64(sum[:8]))
}
