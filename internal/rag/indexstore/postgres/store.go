package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/pgvector/pgvector-go"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	setStatusBuilding = "building"
	setStatusActive   = "active"
	setStatusRetired  = "retired"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Store 实现索引构建需要的 PostgreSQL 持久化能力。
type Store struct {
	db     *gorm.DB
	tables tables
}

var _ indexing.Store = (*Store)(nil)

// NewStore 创建只访问指定 schema 的索引写入 Store。
func NewStore(db *gorm.DB, schema string) (*Store, error) {
	if db == nil {
		return nil, ErrUnavailable
	}
	resolved, err := newTables(schema)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, tables: resolved}, nil
}

// PrepareBuild 在短事务中对齐 building Set 和完整 Chunk 快照，并返回仍需调用模型的输入。
func (s *Store) PrepareBuild(ctx context.Context, build indexing.BuildData) ([]indexing.EmbeddingInput, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, ErrUnavailable
	}
	if err := indexing.ValidateBuildData(build); err != nil {
		return nil, err
	}
	if !uuidPattern.MatchString(string(build.Set.ID)) {
		return nil, invalidBuild("chunk_set_id 必须是 UUID")
	}

	missing := make([]indexing.EmbeddingInput, 0, len(build.EmbeddingInputs))
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.ensureBuildingSet(tx, build.Set); err != nil {
			return err
		}
		if err := s.reconcileChunks(tx, build); err != nil {
			return err
		}
		existing, err := s.embeddingHashes(tx, build.Set.ID, build.EmbeddingInputs)
		if err != nil {
			return err
		}
		for _, input := range build.EmbeddingInputs {
			key := embeddingKey(input.Candidate.ChunkID, input.ModelKey)
			if existing[key] == input.InputSHA256 {
				continue
			}
			missing = append(missing, cloneEmbeddingInput(input))
		}
		return nil
	})
	if err != nil {
		return nil, classifyStoreError(err)
	}
	return missing, nil
}

// SaveEmbeddings 在独立短事务中 Upsert 模型结果，构建阶段始终保持不可检索。
func (s *Store) SaveEmbeddings(
	ctx context.Context,
	setID indexstore.SetID,
	records []indexing.EmbeddingRecord,
) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrUnavailable
	}
	if err := validateEmbeddingRecords(setID, records); err != nil {
		return err
	}

	models := make([]chunkEmbeddingModel, len(records))
	for index, record := range records {
		vector := make([]float32, len(record.Vector))
		for valueIndex, value := range record.Vector {
			vector[valueIndex] = float32(value)
		}
		models[index] = chunkEmbeddingModel{
			ChunkSetID:          string(setID),
			ChunkID:             record.Candidate.ChunkID,
			ModelKey:            string(record.ModelKey),
			EmbeddingText:       record.Text,
			EmbeddingTokenCount: record.TokenCount,
			InputSHA256:         record.InputSHA256,
			Embedding:           pgvector.NewVector(vector),
			Searchable:          false,
		}
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.requireBuildingSet(tx, setID); err != nil {
			return err
		}
		return tx.Table(s.tables.ChunkEmbeddings).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "chunk_set_id"}, {Name: "chunk_id"}, {Name: "model_key"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"embedding_text",
				"embedding_token_count",
				"input_sha256",
				"embedding",
				"searchable",
				"updated_at",
			}),
		}).Create(&models).Error
	})
	return classifyStoreError(err)
}

func (s *Store) ensureBuildingSet(tx *gorm.DB, set indexing.SetRecord) error {
	model := chunkSetModel{
		ID:              string(set.ID),
		TenantID:        set.Scope.TenantID,
		KnowledgeBaseID: set.Scope.KnowledgeBaseID,
		DocumentID:      set.Document.ID,
		SourceURI:       set.Document.SourceURI,
		SourceName:      set.Document.SourceName,
		ContentSHA256:   set.Document.ContentSHA256,
		StrategyName:    set.StrategyName,
		ProfileName:     set.Profile.Name,
		ProfileVersion:  set.Profile.Version,
		Config:          append([]byte(nil), set.Config...),
		Status:          setStatusBuilding,
	}
	if err := tx.Table(s.tables.ChunkSets).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(&model).Error; err != nil {
		return err
	}

	var persisted chunkSetModel
	if err := tx.Table(s.tables.ChunkSets).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", string(set.ID)).Take(&persisted).Error; err != nil {
		return err
	}
	if persisted.Status != setStatusBuilding || persisted.ActivatedAt != nil || !sameSet(persisted, model) {
		return buildConflict("Set ID 已关联另一份构建快照或不可写状态")
	}
	return tx.Table(s.tables.ChunkEmbeddings).
		Where("chunk_set_id = ? AND searchable = ?", string(set.ID), true).
		Update("searchable", false).Error
}

func (s *Store) requireBuildingSet(tx *gorm.DB, setID indexstore.SetID) error {
	var persisted struct {
		Status      string
		ActivatedAt *time.Time
	}
	err := tx.Table(s.tables.ChunkSets).
		Select("status", "activated_at").
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", string(setID)).Take(&persisted).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return buildConflict("目标 Set 不存在")
	}
	if err != nil {
		return err
	}
	if persisted.Status != setStatusBuilding || persisted.ActivatedAt != nil {
		return buildConflict("目标 Set 不是可写的 building 状态")
	}
	return nil
}

func (s *Store) reconcileChunks(tx *gorm.DB, build indexing.BuildData) error {
	chunkIDs := make([]string, len(build.Chunks))
	models := make([]chunkModel, len(build.Chunks))
	for index, chunk := range build.Chunks {
		chunkIDs[index] = chunk.Candidate.ChunkID
		models[index] = chunkModel{
			ChunkSetID:      string(build.Set.ID),
			ChunkID:         chunk.Candidate.ChunkID,
			Kind:            chunk.Kind,
			Level:           chunk.Level,
			ParentChunkID:   cloneStringPointer(chunk.ParentChunkID),
			PreviousChunkID: cloneStringPointer(chunk.PreviousChunkID),
			NextChunkID:     cloneStringPointer(chunk.NextChunkID),
			Sequence:        chunk.Sequence,
			Content:         chunk.Content,
			CharacterCount:  chunk.CharacterCount,
			TokenCount:      chunk.TokenCount,
			SourceUnitIDs:   textArray(append([]string(nil), chunk.SourceUnitIDs...)),
			Metadata:        append([]byte(nil), chunk.Metadata...),
		}
	}
	if err := tx.Table(s.tables.Chunks).
		Where("chunk_set_id = ? AND chunk_id NOT IN ?", string(build.Set.ID), chunkIDs).
		Delete(&chunkModel{}).Error; err != nil {
		return err
	}
	if err := tx.Table(s.tables.Chunks).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "chunk_set_id"}, {Name: "chunk_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"kind",
			"level",
			"parent_chunk_id",
			"previous_chunk_id",
			"next_chunk_id",
			"sequence",
			"content",
			"character_count",
			"token_count",
			"source_unit_ids",
			"metadata",
		}),
	}).Create(&models).Error; err != nil {
		return err
	}

	modelKey := string(build.EmbeddingInputs[0].ModelKey)
	embeddedChunkIDs := make([]string, len(build.EmbeddingInputs))
	for index, input := range build.EmbeddingInputs {
		embeddedChunkIDs[index] = input.Candidate.ChunkID
	}
	return tx.Table(s.tables.ChunkEmbeddings).
		Where(
			"chunk_set_id = ? AND (model_key <> ? OR chunk_id NOT IN ?)",
			string(build.Set.ID),
			modelKey,
			embeddedChunkIDs,
		).
		Delete(&chunkEmbeddingModel{}).Error
}

func (s *Store) embeddingHashes(
	tx *gorm.DB,
	setID indexstore.SetID,
	inputs []indexing.EmbeddingInput,
) (map[string]string, error) {
	type hashRow struct {
		ChunkID     string `gorm:"column:chunk_id"`
		ModelKey    string `gorm:"column:model_key"`
		InputSHA256 string `gorm:"column:input_sha256"`
	}
	chunkIDs := make([]string, len(inputs))
	for index, input := range inputs {
		chunkIDs[index] = input.Candidate.ChunkID
	}
	var rows []hashRow
	err := tx.Table(s.tables.ChunkEmbeddings).
		Select("chunk_id", "model_key", "input_sha256").
		Where(
			"chunk_set_id = ? AND model_key = ? AND chunk_id IN ?",
			string(setID),
			string(inputs[0].ModelKey),
			chunkIDs,
		).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	hashes := make(map[string]string, len(rows))
	for _, row := range rows {
		hashes[embeddingKey(row.ChunkID, indexstore.ModelKey(row.ModelKey))] = row.InputSHA256
	}
	return hashes, nil
}

func validateEmbeddingRecords(setID indexstore.SetID, records []indexing.EmbeddingRecord) error {
	if !uuidPattern.MatchString(string(setID)) {
		return invalidBuild("chunk_set_id 必须是 UUID")
	}
	if len(records) == 0 {
		return invalidBuild("Embedding 写入记录不能为空")
	}
	seen := make(map[string]struct{}, len(records))
	for index, record := range records {
		if record.Candidate.SetID != setID || strings.TrimSpace(record.Candidate.ChunkID) == "" ||
			record.Candidate.ChunkID != strings.TrimSpace(record.Candidate.ChunkID) ||
			strings.TrimSpace(string(record.ModelKey)) == "" || strings.TrimSpace(record.Text) == "" ||
			record.InputSHA256 != sha256Hex(record.Text) || record.TokenCount < 1 ||
			len(record.Vector) != appconfig.RequiredEmbeddingDimensions {
			return invalidBuild("第 %d 条 Embedding 写入记录无效", index+1)
		}
		for _, value := range record.Vector {
			if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(float32(value)), 0) {
				return invalidBuild("第 %d 条 Embedding 向量包含非有限值", index+1)
			}
		}
		key := embeddingKey(record.Candidate.ChunkID, record.ModelKey)
		if _, exists := seen[key]; exists {
			return invalidBuild("Embedding 写入记录 %q 重复", record.Candidate.ChunkID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func sameSet(left, right chunkSetModel) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID &&
		left.KnowledgeBaseID == right.KnowledgeBaseID && left.DocumentID == right.DocumentID &&
		left.SourceURI == right.SourceURI && left.SourceName == right.SourceName &&
		left.ContentSHA256 == right.ContentSHA256 && left.StrategyName == right.StrategyName &&
		left.ProfileName == right.ProfileName && left.ProfileVersion == right.ProfileVersion &&
		jsonEqual(left.Config, right.Config)
}

func jsonEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil &&
		bytes.Equal(mustJSON(leftValue), mustJSON(rightValue))
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func cloneEmbeddingInput(input indexing.EmbeddingInput) indexing.EmbeddingInput {
	return indexing.EmbeddingInput{
		Candidate:   input.Candidate,
		ModelKey:    input.ModelKey,
		Text:        input.Text,
		InputSHA256: input.InputSHA256,
	}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func embeddingKey(chunkID string, modelKey indexstore.ModelKey) string {
	return chunkID + "\x00" + string(modelKey)
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest)
}

func invalidBuild(detail string, args ...any) error {
	return fmt.Errorf("%w: %s", indexing.ErrInvalidBuild, fmt.Sprintf(detail, args...))
}

func buildConflict(detail string) error {
	return fmt.Errorf("%w: %s", indexing.ErrBuildConflict, detail)
}

func classifyStoreError(err error) error {
	if err == nil || errors.Is(err, indexing.ErrInvalidBuild) || errors.Is(err, indexing.ErrBuildConflict) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &storeOperationError{cause: err}
}

type storeOperationError struct {
	cause error
}

func (*storeOperationError) Error() string {
	return indexing.ErrStoreUnavailable.Error()
}

func (e *storeOperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *storeOperationError) Is(target error) bool {
	return e != nil && target == indexing.ErrStoreUnavailable
}
