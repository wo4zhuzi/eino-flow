package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"gorm.io/gorm"
)

func TestValidateConfiguredDatabase(t *testing.T) {
	if os.Getenv("EINO_FLOW_POSTGRES_INTEGRATION") != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_POSTGRES_INTEGRATION=1")
	}
	configuration, err := appconfig.Load()
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := dbpostgres.Open(ctx, configuration.PostgreSQL())
	if err != nil {
		t.Fatalf("postgres.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("postgres.Close() error = %v", err)
		}
	})
	db, err := client.DB()
	if err != nil {
		t.Fatalf("postgres.DB() error = %v", err)
	}
	validator, err := NewValidator(db, configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("NewValidator() error = %v", err)
	}
	if err := validator.Validate(ctx); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestStoreConfiguredDatabaseLifecycle(t *testing.T) {
	if os.Getenv("EINO_FLOW_POSTGRES_INTEGRATION") != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_POSTGRES_INTEGRATION=1")
	}
	configuration, err := appconfig.Load()
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := dbpostgres.Open(ctx, configuration.PostgreSQL())
	if err != nil {
		t.Fatalf("postgres.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("postgres.Close() error = %v", err)
		}
	})
	db, err := client.DB()
	if err != nil {
		t.Fatalf("postgres.DB() error = %v", err)
	}
	store, err := NewStore(db, configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	tables, err := newTables(configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("newTables() error = %v", err)
	}

	build := validStoreBuild()
	build.Set.ID = integrationUUID(t)
	build.Set.Scope.TenantID = "integration-" + string(build.Set.ID)
	for index := range build.Chunks {
		build.Chunks[index].Candidate.SetID = build.Set.ID
	}
	for index := range build.EmbeddingInputs {
		build.EmbeddingInputs[index].Candidate.SetID = build.Set.ID
	}
	secondChunk := build.Chunks[1]
	secondChunk.Candidate.ChunkID = "child-2"
	secondChunk.Sequence = 3
	secondChunk.Content = "第二子块"
	secondChunk.CharacterCount = 4
	build.Chunks = append(build.Chunks, secondChunk)
	secondInput := build.EmbeddingInputs[0]
	secondInput.Candidate.ChunkID = secondChunk.Candidate.ChunkID
	secondInput.Text = "安全标题\n\n第二子块"
	secondInput.InputSHA256 = storeHash(secondInput.Text)
	build.EmbeddingInputs = append(build.EmbeddingInputs, secondInput)

	activeID := integrationUUID(t)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := db.WithContext(cleanupCtx).Table(tables.ChunkSets).
			Where("id IN ?", []string{string(build.Set.ID), string(activeID)}).
			Delete(&chunkSetModel{}).Error; err != nil {
			t.Errorf("清理集成测试 Set 失败: %v", err)
		}
	})
	insertIntegrationActiveSet(t, ctx, db, tables, build, activeID)

	missing, err := store.PrepareBuild(ctx, build)
	if err != nil {
		t.Fatalf("PrepareBuild(first) error = %v", err)
	}
	if len(missing) != 2 {
		t.Fatalf("PrepareBuild(first) missing = %d, want 2", len(missing))
	}
	firstRecord := integrationEmbeddingRecord(missing[0])
	if err := store.SaveEmbeddings(ctx, build.Set.ID, []indexing.EmbeddingRecord{firstRecord}); err != nil {
		t.Fatalf("SaveEmbeddings(partial) error = %v", err)
	}

	missing, err = store.PrepareBuild(ctx, build)
	if err != nil {
		t.Fatalf("PrepareBuild(partial retry) error = %v", err)
	}
	if len(missing) != 1 || missing[0].Candidate.ChunkID != secondInput.Candidate.ChunkID {
		t.Fatalf("PrepareBuild(partial retry) missing = %#v", missing)
	}
	if err := store.SaveEmbeddings(ctx, build.Set.ID, []indexing.EmbeddingRecord{
		integrationEmbeddingRecord(missing[0]),
	}); err != nil {
		t.Fatalf("SaveEmbeddings(recovery) error = %v", err)
	}
	missing, err = store.PrepareBuild(ctx, build)
	if err != nil {
		t.Fatalf("PrepareBuild(final retry) error = %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("PrepareBuild(final retry) missing = %#v, want empty", missing)
	}

	var building chunkSetModel
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Where("id = ?", string(build.Set.ID)).Take(&building).Error; err != nil {
		t.Fatalf("query building Set error = %v", err)
	}
	var chunkCount int64
	if err := db.WithContext(ctx).Table(tables.Chunks).
		Where("chunk_set_id = ?", string(build.Set.ID)).Count(&chunkCount).Error; err != nil {
		t.Fatalf("count chunks error = %v", err)
	}
	var embeddingCount int64
	if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).
		Where("chunk_set_id = ? AND searchable = false", string(build.Set.ID)).Count(&embeddingCount).Error; err != nil {
		t.Fatalf("count embeddings error = %v", err)
	}
	if building.Status != setStatusBuilding || building.ActivatedAt != nil || chunkCount != 3 || embeddingCount != 2 {
		t.Fatalf(
			"building snapshot = status:%s activated:%v chunks:%d embeddings:%d",
			building.Status,
			building.ActivatedAt,
			chunkCount,
			embeddingCount,
		)
	}

	var active chunkSetModel
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Where("id = ?", string(activeID)).Take(&active).Error; err != nil {
		t.Fatalf("query active Set error = %v", err)
	}
	var activeSearchable bool
	if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).
		Select("searchable").
		Where("chunk_set_id = ?", string(activeID)).
		Scan(&activeSearchable).Error; err != nil {
		t.Fatalf("query active Embedding error = %v", err)
	}
	if active.Status != "active" || active.ActivatedAt == nil || !activeSearchable {
		t.Fatalf("active Set changed: status:%s activated:%v searchable:%v", active.Status, active.ActivatedAt, activeSearchable)
	}
}

func insertIntegrationActiveSet(
	t *testing.T,
	ctx context.Context,
	db *gorm.DB,
	tables tables,
	build indexing.BuildData,
	activeID indexstore.SetID,
) {
	t.Helper()
	activatedAt := time.Now().UTC()
	set := chunkSetModel{
		ID:              string(activeID),
		TenantID:        build.Set.Scope.TenantID,
		KnowledgeBaseID: build.Set.Scope.KnowledgeBaseID,
		DocumentID:      build.Set.Document.ID,
		SourceURI:       build.Set.Document.SourceURI,
		SourceName:      build.Set.Document.SourceName,
		ContentSHA256:   build.Set.Document.ContentSHA256,
		StrategyName:    build.Set.StrategyName,
		ProfileName:     build.Set.Profile.Name,
		ProfileVersion:  build.Set.Profile.Version,
		Config:          append([]byte(nil), build.Set.Config...),
		Status:          "active",
		ActivatedAt:     &activatedAt,
	}
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Create(&set).Error; err != nil {
		t.Fatalf("create active Set error = %v", err)
	}
	parentID := build.Chunks[0].Candidate.ChunkID
	chunks := []chunkModel{
		{
			ChunkSetID:     string(activeID),
			ChunkID:        parentID,
			Kind:           "parent",
			Level:          0,
			Sequence:       1,
			Content:        "旧父块",
			CharacterCount: 3,
			TokenCount:     2,
			SourceUnitIDs:  []string{"unit-old"},
			Metadata:       []byte(`{}`),
		},
		{
			ChunkSetID:     string(activeID),
			ChunkID:        "child-old",
			Kind:           "child",
			Level:          1,
			ParentChunkID:  &parentID,
			Sequence:       2,
			Content:        "旧子块",
			CharacterCount: 3,
			TokenCount:     2,
			SourceUnitIDs:  []string{"unit-old"},
			Metadata:       []byte(`{}`),
		},
	}
	if err := db.WithContext(ctx).Table(tables.Chunks).Create(&chunks).Error; err != nil {
		t.Fatalf("create active Chunks error = %v", err)
	}
	vector := make([]float32, appconfig.RequiredEmbeddingDimensions)
	embedding := chunkEmbeddingModel{
		ChunkSetID:          string(activeID),
		ChunkID:             "child-old",
		ModelKey:            string(testModelKey),
		EmbeddingText:       "旧子块",
		EmbeddingTokenCount: 2,
		InputSHA256:         storeHash("旧子块"),
		Embedding:           pgvector.NewVector(vector),
		Searchable:          true,
	}
	if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).Create(&embedding).Error; err != nil {
		t.Fatalf("create active Embedding error = %v", err)
	}
}

func integrationEmbeddingRecord(input indexing.EmbeddingInput) indexing.EmbeddingRecord {
	vector := make([]float64, appconfig.RequiredEmbeddingDimensions)
	for index := range vector {
		vector[index] = float64(index+1) / float64(appconfig.RequiredEmbeddingDimensions)
	}
	return indexing.EmbeddingRecord{
		EmbeddingInput: input,
		TokenCount:     8,
		Vector:         vector,
	}
}

func integrationUUID(t *testing.T) indexstore.SetID {
	t.Helper()
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("rand.Read() error = %v", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return indexstore.SetID(fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		raw[0:4],
		raw[4:6],
		raw[6:8],
		raw[8:10],
		raw[10:16],
	))
}
