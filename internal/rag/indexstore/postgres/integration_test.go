package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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

	publishPaused := make(chan struct{})
	publishResume := make(chan struct{})
	var pauseOnce sync.Once
	var resumeOnce sync.Once
	releasePublish := func() { resumeOnce.Do(func() { close(publishResume) }) }
	defer releasePublish()
	callbackName := "test:publish-no-gap:" + string(build.Set.ID)
	if err := db.Callback().Update().After("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if strings.Trim(tx.Statement.Table, `"`) != chunkSetsTable {
			return
		}
		pauseOnce.Do(func() {
			close(publishPaused)
			<-publishResume
		})
	}); err != nil {
		t.Fatalf("register publish callback error = %v", err)
	}
	defer func() {
		if err := db.Callback().Update().Remove(callbackName); err != nil {
			t.Errorf("remove publish callback error = %v", err)
		}
	}()
	publishResult := make(chan error, 1)
	go func() {
		publishResult <- store.Publish(ctx, build.Set.ID)
	}()
	select {
	case <-publishPaused:
	case <-ctx.Done():
		t.Fatalf("Publish() did not reach atomic switch: %v", ctx.Err())
	}
	var visibleBeforeCommit int64
	visibilitySQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s AS sets
		JOIN %s AS embeddings ON embeddings.chunk_set_id = sets.id
		WHERE sets.tenant_id = ? AND sets.knowledge_base_id = ? AND sets.document_id = ?
		AND sets.strategy_name = ? AND sets.status = ? AND embeddings.searchable = true`,
		tables.ChunkSets,
		tables.ChunkEmbeddings,
	)
	if err := db.WithContext(ctx).Raw(
		visibilitySQL,
		build.Set.Scope.TenantID,
		build.Set.Scope.KnowledgeBaseID,
		build.Set.Document.ID,
		build.Set.StrategyName,
		setStatusActive,
	).Scan(&visibleBeforeCommit).Error; err != nil {
		t.Fatalf("query visibility during Publish error = %v", err)
	}
	if visibleBeforeCommit == 0 {
		t.Fatal("Publish transaction exposed a retrieval gap before commit")
	}
	releasePublish()
	if err := <-publishResult; err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Where("id = ?", string(build.Set.ID)).Take(&building).Error; err != nil {
		t.Fatalf("query published Set error = %v", err)
	}
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Where("id = ?", string(activeID)).Take(&active).Error; err != nil {
		t.Fatalf("query retired Set error = %v", err)
	}
	var publishedSearchable int64
	if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).
		Where("chunk_set_id = ? AND searchable = true", string(build.Set.ID)).
		Count(&publishedSearchable).Error; err != nil {
		t.Fatalf("count published Embeddings error = %v", err)
	}
	var retiredSearchable int64
	if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).
		Where("chunk_set_id = ? AND searchable = true", string(activeID)).
		Count(&retiredSearchable).Error; err != nil {
		t.Fatalf("count retired Embeddings error = %v", err)
	}
	if building.Status != setStatusActive || building.ActivatedAt == nil ||
		active.Status != setStatusRetired || publishedSearchable != 2 || retiredSearchable != 0 {
		t.Fatalf(
			"published snapshot = new:%s/%v/%d old:%s/%d",
			building.Status,
			building.ActivatedAt,
			publishedSearchable,
			active.Status,
			retiredSearchable,
		)
	}
	if err := store.Publish(ctx, build.Set.ID); !errors.Is(err, indexing.ErrBuildConflict) {
		t.Fatalf("Publish(retry) error = %v, want ErrBuildConflict", err)
	}
}

func TestStoreConfiguredDatabaseConcurrentPublish(t *testing.T) {
	if os.Getenv("EINO_FLOW_POSTGRES_INTEGRATION") != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_POSTGRES_INTEGRATION=1")
	}
	configuration, err := appconfig.Load()
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	testSuffix := string(integrationUUID(t))
	base := validStoreBuild()
	base.Set.Scope.TenantID = "concurrent-" + testSuffix
	base.Set.Scope.KnowledgeBaseID = "kb-same"
	base.Set.Document.ID = "document-same"
	base.Set.Document.SourceURI = "knowledge://documents/" + testSuffix
	base.Set.Profile.Name = "profile-" + testSuffix
	baseline := integrationBuildForSet(base, integrationUUID(t))
	first := integrationBuildForSet(base, integrationUUID(t))
	second := integrationBuildForSet(base, integrationUUID(t))
	differentFirst := integrationBuildForSet(base, integrationUUID(t))
	differentFirst.Set.Scope.KnowledgeBaseID = "kb-different-1"
	differentFirst.Set.Document.ID = "document-different-1"
	differentSecond := integrationBuildForSet(base, integrationUUID(t))
	differentSecond.Set.Scope.KnowledgeBaseID = "kb-different-2"
	differentSecond.Set.Document.ID = "document-different-2"
	allBuilds := []indexing.BuildData{baseline, first, second, differentFirst, differentSecond}
	ids := make([]string, len(allBuilds))
	for index, build := range allBuilds {
		ids[index] = string(build.Set.ID)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := db.WithContext(cleanupCtx).Table(tables.ChunkSets).
			Where("id IN ?", ids).Delete(&chunkSetModel{}).Error; err != nil {
			t.Errorf("清理并发集成测试 Set 失败: %v", err)
		}
	})

	for _, build := range allBuilds {
		persistIntegrationBuild(t, ctx, store, build)
	}
	if err := store.Publish(ctx, baseline.Set.ID); err != nil {
		t.Fatalf("Publish(baseline) error = %v", err)
	}

	assertConcurrentPublish(t, ctx, store, []indexstore.SetID{first.Set.ID, second.Set.ID})
	var activeCount int64
	if err := db.WithContext(ctx).Table(tables.ChunkSets).
		Where(
			"tenant_id = ? AND knowledge_base_id = ? AND document_id = ? AND strategy_name = ? AND status = ?",
			base.Set.Scope.TenantID,
			base.Set.Scope.KnowledgeBaseID,
			base.Set.Document.ID,
			base.Set.StrategyName,
			setStatusActive,
		).
		Count(&activeCount).Error; err != nil {
		t.Fatalf("count same-scope active Sets error = %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("same-scope active Set count = %d, want 1", activeCount)
	}

	assertConcurrentPublish(t, ctx, store, []indexstore.SetID{differentFirst.Set.ID, differentSecond.Set.ID})
	for _, build := range []indexing.BuildData{differentFirst, differentSecond} {
		var published chunkSetModel
		if err := db.WithContext(ctx).Table(tables.ChunkSets).
			Where("id = ?", string(build.Set.ID)).Take(&published).Error; err != nil {
			t.Fatalf("query different-scope Set error = %v", err)
		}
		if published.Status != setStatusActive || published.ActivatedAt == nil {
			t.Fatalf("different-scope Set = %s/%v, want active", published.Status, published.ActivatedAt)
		}
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
			SourceUnitIDs:  textArray{"unit-old"},
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
			SourceUnitIDs:  textArray{"unit-old"},
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

func integrationBuildForSet(build indexing.BuildData, setID indexstore.SetID) indexing.BuildData {
	build.Set.ID = setID
	build.Chunks = append([]indexing.ChunkRecord(nil), build.Chunks...)
	for index := range build.Chunks {
		build.Chunks[index].Candidate.SetID = setID
	}
	build.EmbeddingInputs = append([]indexing.EmbeddingInput(nil), build.EmbeddingInputs...)
	for index := range build.EmbeddingInputs {
		build.EmbeddingInputs[index].Candidate.SetID = setID
	}
	return build
}

func persistIntegrationBuild(t *testing.T, ctx context.Context, store *Store, build indexing.BuildData) {
	t.Helper()
	missing, err := store.PrepareBuild(ctx, build)
	if err != nil {
		t.Fatalf("PrepareBuild() error = %v", err)
	}
	records := make([]indexing.EmbeddingRecord, len(missing))
	for index, input := range missing {
		records[index] = integrationEmbeddingRecord(input)
	}
	if err := store.SaveEmbeddings(ctx, build.Set.ID, records); err != nil {
		t.Fatalf("SaveEmbeddings() error = %v", err)
	}
}

func assertConcurrentPublish(t *testing.T, ctx context.Context, store *Store, setIDs []indexstore.SetID) {
	t.Helper()
	start := make(chan struct{})
	errorsBySet := make(chan error, len(setIDs))
	var wait sync.WaitGroup
	for _, setID := range setIDs {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errorsBySet <- store.Publish(ctx, setID)
		}()
	}
	close(start)
	wait.Wait()
	close(errorsBySet)
	for err := range errorsBySet {
		if err != nil {
			t.Fatalf("concurrent Publish() error = %v", err)
		}
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
