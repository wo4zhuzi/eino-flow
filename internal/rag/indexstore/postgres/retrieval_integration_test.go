package postgres

import (
	"context"
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
	"gorm.io/gorm"
)

func TestStoreConfiguredDatabaseRetrievalIsolation(t *testing.T) {
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

	tenantID := "retrieval-integration-" + string(integrationUUID(t))
	otherTenantID := tenantID + "-other"
	knowledgeID := "knowledge"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := db.WithContext(cleanupCtx).Table(store.tables.ChunkSets).
			Where("tenant_id IN ?", []string{tenantID, otherTenantID}).Delete(&chunkSetModel{}).Error; err != nil {
			t.Errorf("清理检索集成测试数据失败: %v", err)
		}
	})

	visibleID := insertRetrievalIntegrationSet(t, ctx, db, store.tables, retrievalIntegrationSet{
		TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusActive,
		Chunks: []retrievalIntegrationChunk{
			{ID: "visible-b", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(0.8, 0.6)},
			{ID: "visible-a", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(0.8, 0.6)},
		},
	})
	secondVisibleID := insertRetrievalIntegrationSet(t, ctx, db, store.tables, retrievalIntegrationSet{
		TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusActive,
		Chunks: []retrievalIntegrationChunk{
			{ID: "visible-c", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(0.8, 0.6)},
		},
	})
	fixtures := []retrievalIntegrationSet{
		{TenantID: otherTenantID, KnowledgeID: knowledgeID, Status: setStatusActive, Chunks: []retrievalIntegrationChunk{{ID: "wrong-tenant", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(1, 0)}}},
		{TenantID: tenantID, KnowledgeID: "other-knowledge", Status: setStatusActive, Chunks: []retrievalIntegrationChunk{{ID: "wrong-knowledge", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(1, 0)}}},
		{TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusBuilding, Chunks: []retrievalIntegrationChunk{{ID: "building", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(1, 0)}}},
		{TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusRetired, Chunks: []retrievalIntegrationChunk{{ID: "retired", ModelKey: testModelKey, Searchable: true, Vector: retrievalIntegrationVector(1, 0)}}},
		{TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusActive, Chunks: []retrievalIntegrationChunk{{ID: "unsearchable", ModelKey: testModelKey, Searchable: false, Vector: retrievalIntegrationVector(1, 0)}}},
		{TenantID: tenantID, KnowledgeID: knowledgeID, Status: setStatusActive, Chunks: []retrievalIntegrationChunk{{ID: "wrong-model", ModelKey: indexstore.ModelKey(string(testModelKey) + ":other"), Searchable: true, Vector: retrievalIntegrationVector(1, 0)}}},
	}
	for _, fixture := range fixtures {
		insertRetrievalIntegrationSet(t, ctx, db, store.tables, fixture)
	}

	request := retrieval.SearchRequest{
		Scope:    indexstore.Scope{TenantID: tenantID, KnowledgeBaseID: knowledgeID},
		ModelKey: testModelKey,
		Vector:   retrievalIntegrationQueryVector(),
		Limit:    retrieval.MaxTopK,
	}
	evidence, err := store.Search(ctx, request)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	wantCandidates := []indexstore.CandidateID{
		{SetID: visibleID, ChunkID: "visible-a"},
		{SetID: visibleID, ChunkID: "visible-b"},
		{SetID: secondVisibleID, ChunkID: "visible-c"},
	}
	sort.Slice(wantCandidates, func(left, right int) bool {
		if wantCandidates[left].SetID != wantCandidates[right].SetID {
			return wantCandidates[left].SetID < wantCandidates[right].SetID
		}
		return wantCandidates[left].ChunkID < wantCandidates[right].ChunkID
	})
	if len(evidence) != len(wantCandidates) {
		t.Fatalf("Search() evidence = %#v", evidence)
	}
	for index, want := range wantCandidates {
		if evidence[index].Candidate != want || evidence[index].CosineDistance != evidence[0].CosineDistance {
			t.Fatalf("Search() evidence[%d] = %#v, want candidate=%#v and equal distance", index, evidence[index], want)
		}
	}

	emptyRequest := request
	emptyRequest.Scope.KnowledgeBaseID = "missing-knowledge"
	empty, err := store.Search(ctx, emptyRequest)
	if err != nil {
		t.Fatalf("Search(missing knowledge) error = %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("Search(missing knowledge) = %#v, want non-nil empty", empty)
	}

	plan, err := retrievalIntegrationExplain(ctx, store, request)
	if err != nil {
		t.Fatalf("EXPLAIN (ANALYZE, BUFFERS) error = %v", err)
	}
	usesHNSW := strings.Contains(plan, "idx_chunk_embeddings_hnsw_cosine")
	t.Logf("检索执行计划完成: hnsw=%t（隔离测试数据量较小时 PostgreSQL 可能选择顺序扫描）", usesHNSW)
}

type retrievalIntegrationSet struct {
	TenantID    string
	KnowledgeID string
	Status      string
	Chunks      []retrievalIntegrationChunk
}

type retrievalIntegrationChunk struct {
	ID         string
	ModelKey   indexstore.ModelKey
	Searchable bool
	Vector     []float32
}

func insertRetrievalIntegrationSet(
	t *testing.T,
	ctx context.Context,
	db *gorm.DB,
	tables tables,
	fixture retrievalIntegrationSet,
) indexstore.SetID {
	t.Helper()
	setID := integrationUUID(t)
	var activatedAt *time.Time
	if fixture.Status == setStatusActive || fixture.Status == setStatusRetired {
		value := time.Now().UTC()
		activatedAt = &value
	}
	set := chunkSetModel{
		ID:              string(setID),
		TenantID:        fixture.TenantID,
		KnowledgeBaseID: fixture.KnowledgeID,
		DocumentID:      "document-" + string(setID),
		SourceURI:       "knowledge://retrieval-integration/" + string(setID),
		SourceName:      "knowledge.md",
		ContentSHA256:   storeHash(string(setID)),
		StrategyName:    "retrieval-integration",
		ProfileName:     "retrieval-integration",
		ProfileVersion:  "v1",
		Config:          []byte(`{}`),
		Status:          fixture.Status,
		ActivatedAt:     activatedAt,
	}
	if err := db.WithContext(ctx).Table(tables.ChunkSets).Create(&set).Error; err != nil {
		t.Fatalf("创建检索集成 Set 失败: %v", err)
	}
	for index, item := range fixture.Chunks {
		content := "检索集成证据 " + item.ID
		chunk := chunkModel{
			ChunkSetID:     string(setID),
			ChunkID:        item.ID,
			Kind:           "structure",
			Level:          0,
			Sequence:       index + 1,
			Content:        content,
			CharacterCount: len([]rune(content)),
			TokenCount:     1,
			SourceUnitIDs:  textArray{"unit-" + item.ID},
			Metadata:       []byte(`{"fixture":"retrieval"}`),
		}
		if err := db.WithContext(ctx).Table(tables.Chunks).Create(&chunk).Error; err != nil {
			t.Fatalf("创建检索集成 Chunk 失败: %v", err)
		}
		embedding := chunkEmbeddingModel{
			ChunkSetID:          string(setID),
			ChunkID:             item.ID,
			ModelKey:            string(item.ModelKey),
			EmbeddingText:       content,
			EmbeddingTokenCount: 1,
			InputSHA256:         storeHash(content),
			Embedding:           pgvector.NewVector(item.Vector),
			Searchable:          item.Searchable,
		}
		if err := db.WithContext(ctx).Table(tables.ChunkEmbeddings).Create(&embedding).Error; err != nil {
			t.Fatalf("创建检索集成 Embedding 失败: %v", err)
		}
	}
	return setID
}

func retrievalIntegrationVector(first, second float32) []float32 {
	vector := make([]float32, appconfig.RequiredEmbeddingDimensions)
	vector[0] = first
	vector[1] = second
	return vector
}

func retrievalIntegrationQueryVector() []float64 {
	vector := make([]float64, appconfig.RequiredEmbeddingDimensions)
	vector[0] = 1
	return vector
}

func retrievalIntegrationExplain(
	ctx context.Context,
	store *Store,
	request retrieval.SearchRequest,
) (string, error) {
	vector := make([]float32, len(request.Vector))
	for index, value := range request.Vector {
		vector[index] = float32(value)
	}
	rows, err := store.db.WithContext(ctx).Raw(
		"EXPLAIN (ANALYZE, BUFFERS) "+store.searchStatement(),
		pgvector.NewVector(vector),
		request.Scope.TenantID,
		request.Scope.KnowledgeBaseID,
		string(request.ModelKey),
		request.Limit,
	).Rows()
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(lines) == 0 {
		return "", sql.ErrNoRows
	}
	return strings.Join(lines, "\n"), nil
}
