package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-document-parser-structured/markdown"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
	"gorm.io/gorm"
)

const (
	workflowE2EEnv             = "EINO_FLOW_WORKFLOW_E2E"
	workflowE2ETimeout         = 3 * time.Minute
	workflowE2ECanonicalPrefix = "knowledge://workflow-e2e/"
)

var errWorkflowE2EPublishInterrupted = errors.New("端到端测试模拟发布中断")

type countingEmbedder struct {
	delegate indexing.Embedder

	mu     sync.Mutex
	calls  int
	inputs int
}

func (e *countingEmbedder) Embed(ctx context.Context, texts []string) ([]embedding.Result, error) {
	results, err := e.delegate.Embed(ctx, texts)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	e.inputs += len(texts)
	return results, err
}

func (e *countingEmbedder) counts() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.inputs
}

type failOncePublishStore struct {
	indexing.Store

	mu      sync.Mutex
	failed  bool
	failure error
}

func (s *failOncePublishStore) Publish(ctx context.Context, setID indexstore.SetID) error {
	s.mu.Lock()
	if !s.failed {
		s.failed = true
		failure := s.failure
		s.mu.Unlock()
		return failure
	}
	s.mu.Unlock()
	return s.Store.Publish(ctx, setID)
}

type errorEmbedder struct {
	err error
}

func (e errorEmbedder) Embed(context.Context, []string) ([]embedding.Result, error) {
	return nil, e.err
}

type workflowE2EFixture struct {
	db               *gorm.DB
	tables           tables
	store            *Store
	ingestor         indexing.Ingestor
	chunker          indexing.Chunker
	embedder         *countingEmbedder
	realEmbedder     *embedding.Client
	buildConfig      indexing.BuildConfig
	tenantID         string
	knowledgeID      string
	markdownV1       string
	markdownV2       string
	plainText        string
	expectedModelKey string
}

func TestRetrievalConfiguredEndToEnd(t *testing.T) {
	if os.Getenv(workflowE2EEnv) != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_WORKFLOW_E2E=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), workflowE2ETimeout)
	defer cancel()
	fixture := newWorkflowE2EFixture(t, ctx)
	indexWorkflow := fixture.newWorkflow(t, ctx, fixture.realEmbedder, fixture.store)
	setID := indexstore.SetID(uuid.NewString())
	knowledgePath := workflowE2EKnowledgePath(t)
	_, err := indexWorkflow.Run(ctx, fixture.request(
		"retrieval-index",
		knowledgePath,
		setID,
		"retrieval-document",
		workflowE2ECanonicalPrefix+fixture.tenantID+"/retrieval",
	))
	if err != nil {
		t.Fatalf("索引 testdata/knowledge.md error = %v", err)
	}
	retrievalWorkflow, err := retrieval.New(ctx, retrieval.Dependencies{
		Embedder: fixture.realEmbedder,
		Store:    fixture.store,
		Config: retrieval.Config{Model: indexstore.ModelProfile{
			Model:         fixture.buildConfig.Model.Model,
			Dimensions:    fixture.buildConfig.Model.Dimensions,
			Distance:      indexstore.DistanceCosine,
			ConfigVersion: appconfig.EmbeddingModelConfigVersion,
		}},
	})
	if err != nil {
		t.Fatalf("retrieval.New() error = %v", err)
	}
	request := retrieval.Request{
		RunID: fixture.runID("retrieval-query"),
		Query: "Markdown 文档使用什么切分策略？",
		Scope: indexstore.Scope{TenantID: fixture.tenantID, KnowledgeBaseID: fixture.knowledgeID},
		TopK:  3,
	}
	result, err := retrievalWorkflow.Run(ctx, request)
	if err != nil {
		t.Fatalf("检索已发布 knowledge.md error = %v", err)
	}
	containsExpectedEvidence := false
	for _, item := range result.Evidence {
		if strings.Contains(item.Content, "结构化 Markdown 使用 Structure-aware Chunk") {
			containsExpectedEvidence = true
			break
		}
	}
	if result.Status != retrieval.StatusCompleted || len(result.Evidence) == 0 ||
		len(result.Evidence) > request.TopK || !containsExpectedEvidence {
		t.Fatalf("检索结果不满足验收契约: status=%s count=%d expected_evidence=%t", result.Status, len(result.Evidence), containsExpectedEvidence)
	}
	emptyRequest := request
	emptyRequest.RunID = fixture.runID("retrieval-empty")
	emptyRequest.Scope.KnowledgeBaseID = "missing-knowledge"
	empty, err := retrievalWorkflow.Run(ctx, emptyRequest)
	if err != nil {
		t.Fatalf("检索不存在知识库 error = %v", err)
	}
	if empty.Evidence == nil || len(empty.Evidence) != 0 {
		t.Fatalf("检索不存在知识库 evidence=%d, want non-nil empty", len(empty.Evidence))
	}
	t.Logf("基础向量召回验收完成: top_k=%d evidence=%d expected_evidence=%t empty=%d", request.TopK, len(result.Evidence), containsExpectedEvidence, len(empty.Evidence))
}

func TestWorkflowConfiguredEndToEnd(t *testing.T) {
	if os.Getenv(workflowE2EEnv) != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_WORKFLOW_E2E=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), workflowE2ETimeout)
	defer cancel()
	fixture := newWorkflowE2EFixture(t, ctx)

	structureDocumentID := "structure-document"
	structureCanonicalURI := workflowE2ECanonicalPrefix + fixture.tenantID + "/structure"
	firstSetID := indexstore.SetID(uuid.NewString())
	interruptedStore := &failOncePublishStore{
		Store:   fixture.store,
		failure: errWorkflowE2EPublishInterrupted,
	}
	workflow := fixture.newWorkflow(t, ctx, fixture.embedder, interruptedStore)
	firstRequest := fixture.request(
		"structure-first",
		fixture.markdownV1,
		firstSetID,
		structureDocumentID,
		structureCanonicalURI,
	)

	if _, err := workflow.Run(ctx, firstRequest); !errors.Is(err, errWorkflowE2EPublishInterrupted) {
		t.Fatalf("首次构建模拟发布中断 error = %v", err)
	}
	firstBuilding := fixture.loadSnapshot(t, ctx, firstSetID)
	assertSnapshot(t, firstBuilding, snapshotExpectation{
		status:           setStatusBuilding,
		strategy:         structureaware.StructureAwareStrategyName,
		modelKey:         fixture.expectedModelKey,
		searchable:       false,
		requireEmbedding: true,
	})
	firstCalls, firstInputs := fixture.embedder.counts()
	if firstCalls != 1 || firstInputs != len(firstBuilding.embeddings) {
		t.Fatalf("首次 Embedding 调用 calls=%d inputs=%d embeddings=%d", firstCalls, firstInputs, len(firstBuilding.embeddings))
	}

	retryRequest := firstRequest
	retryRequest.RunID = fixture.runID("structure-retry")
	retryResult, err := workflow.Run(ctx, retryRequest)
	if err != nil {
		t.Fatalf("相同 Set 重试 error = %v", err)
	}
	if retryResult.Index.GeneratedEmbeddingCount != 0 ||
		retryResult.Index.ReusedEmbeddingCount != len(firstBuilding.embeddings) {
		t.Fatalf(
			"重试复用统计 generated=%d reused=%d want generated=0 reused=%d",
			retryResult.Index.GeneratedEmbeddingCount,
			retryResult.Index.ReusedEmbeddingCount,
			len(firstBuilding.embeddings),
		)
	}
	retryCalls, retryInputs := fixture.embedder.counts()
	if retryCalls != firstCalls || retryInputs != firstInputs {
		t.Fatalf("重试发生了不必要的模型调用 before=(%d,%d) after=(%d,%d)", firstCalls, firstInputs, retryCalls, retryInputs)
	}
	firstActive := fixture.loadSnapshot(t, ctx, firstSetID)
	assertPublishedResult(t, retryResult, firstActive, structureaware.StructureAwareStrategyName, fixture.expectedModelKey)
	if len(firstActive.chunks) != len(firstBuilding.chunks) || len(firstActive.embeddings) != len(firstBuilding.embeddings) {
		t.Fatalf(
			"重试产生重复或缺失数据 before=(%d,%d) after=(%d,%d)",
			len(firstBuilding.chunks),
			len(firstBuilding.embeddings),
			len(firstActive.chunks),
			len(firstActive.embeddings),
		)
	}

	parentSetID := indexstore.SetID(uuid.NewString())
	parentResult, err := workflow.Run(ctx, fixture.request(
		"parent-child",
		fixture.plainText,
		parentSetID,
		"parent-child-document",
		workflowE2ECanonicalPrefix+fixture.tenantID+"/parent-child",
	))
	if err != nil {
		t.Fatalf("Parent-child 构建 error = %v", err)
	}
	parentSnapshot := fixture.loadSnapshot(t, ctx, parentSetID)
	assertPublishedResult(t, parentResult, parentSnapshot, parentchild.ParentChildStrategyName, fixture.expectedModelKey)
	assertParentChildRelations(t, parentSnapshot)

	secondSetID := indexstore.SetID(uuid.NewString())
	secondResult, err := workflow.Run(ctx, fixture.request(
		"structure-content-change",
		fixture.markdownV2,
		secondSetID,
		structureDocumentID,
		structureCanonicalURI,
	))
	if err != nil {
		t.Fatalf("内容变更构建 error = %v", err)
	}
	secondActive := fixture.loadSnapshot(t, ctx, secondSetID)
	assertPublishedResult(t, secondResult, secondActive, structureaware.StructureAwareStrategyName, fixture.expectedModelKey)
	retiredFirst := fixture.loadSnapshot(t, ctx, firstSetID)
	assertSnapshot(t, retiredFirst, snapshotExpectation{
		status:           setStatusRetired,
		strategy:         structureaware.StructureAwareStrategyName,
		modelKey:         fixture.expectedModelKey,
		searchable:       false,
		requireEmbedding: true,
	})
	fixture.assertOnlyActiveSet(t, ctx, structureDocumentID, structureaware.StructureAwareStrategyName, secondSetID)

	failure := errors.New("端到端测试模拟 Embedding 失败")
	failedSetID := indexstore.SetID(uuid.NewString())
	failedWorkflow := fixture.newWorkflow(t, ctx, errorEmbedder{err: failure}, fixture.store)
	if _, err := failedWorkflow.Run(ctx, fixture.request(
		"structure-failed-build",
		fixture.markdownV1,
		failedSetID,
		structureDocumentID,
		structureCanonicalURI,
	)); !errors.Is(err, failure) {
		t.Fatalf("失败构建 error = %v", err)
	}
	failedSnapshot := fixture.loadSnapshot(t, ctx, failedSetID)
	assertSnapshot(t, failedSnapshot, snapshotExpectation{
		status:           setStatusBuilding,
		strategy:         structureaware.StructureAwareStrategyName,
		modelKey:         fixture.expectedModelKey,
		searchable:       false,
		requireEmbedding: false,
	})
	if len(failedSnapshot.chunks) == 0 || len(failedSnapshot.embeddings) != 0 {
		t.Fatalf("失败构建数据 chunks=%d embeddings=%d", len(failedSnapshot.chunks), len(failedSnapshot.embeddings))
	}
	secondAfterFailure := fixture.loadSnapshot(t, ctx, secondSetID)
	assertPublishedResult(t, secondResult, secondAfterFailure, structureaware.StructureAwareStrategyName, fixture.expectedModelKey)
	fixture.assertOnlyActiveSet(t, ctx, structureDocumentID, structureaware.StructureAwareStrategyName, secondSetID)
	modelCalls, modelInputs := fixture.embedder.counts()
	t.Logf(
		"验收统计: structure_v1=(chunks:%d embeddings:%d) parent_child=(chunks:%d embeddings:%d) structure_v2=(chunks:%d embeddings:%d) failed=(chunks:%d embeddings:%d) model=(calls:%d inputs:%d)",
		len(firstActive.chunks),
		len(firstActive.embeddings),
		len(parentSnapshot.chunks),
		len(parentSnapshot.embeddings),
		len(secondActive.chunks),
		len(secondActive.embeddings),
		len(failedSnapshot.chunks),
		len(failedSnapshot.embeddings),
		modelCalls,
		modelInputs,
	)
}

func newWorkflowE2EFixture(t *testing.T, ctx context.Context) workflowE2EFixture {
	t.Helper()
	configuration, err := appconfig.Load()
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
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
		t.Fatalf("Validator.Validate() error = %v", err)
	}
	store, err := NewStore(db, configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	resolvedTables, err := newTables(configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("newTables() error = %v", err)
	}
	realEmbedder, err := embedding.New(ctx, configuration.Embedding())
	if err != nil {
		t.Fatalf("embedding.New() error = %v", err)
	}
	registry, err := ingestion.NewDefaultRegistry(ctx)
	if err != nil {
		t.Fatalf("ingestion.NewDefaultRegistry() error = %v", err)
	}
	if err := registry.ReplaceParser(ingestion.ExtensionMarkdown, markdown.ParserInfo(), markdown.New()); err != nil {
		t.Fatalf("ReplaceParser(markdown) error = %v", err)
	}
	ingestor, err := ingestion.New(ctx, ingestion.Config{
		MaxFileBytes: ingestion.DefaultMaxFileBytes,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("ingestion.New() error = %v", err)
	}
	chunkConfig := indexing.ChunkConfig{
		ProfileName:       "workflow-e2e",
		ProfileVersion:    "v1",
		ParentMaxRunes:    280,
		ChildMaxRunes:     140,
		StructureMaxRunes: 320,
		StructureMinRunes: 80,
	}
	chunker, err := indexing.NewAutomaticChunker(chunkConfig)
	if err != nil {
		t.Fatalf("indexing.NewAutomaticChunker() error = %v", err)
	}

	tenantID := "workflow-e2e-" + uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := db.WithContext(cleanupCtx).
			Table(resolvedTables.ChunkSets).
			Where("tenant_id = ?", tenantID).
			Delete(&chunkSetModel{}).Error; err != nil {
			t.Errorf("清理端到端测试租户数据失败: %v", err)
		}
	})

	markdownV1, markdownV2, plainText := writeWorkflowE2ECorpus(t)
	model := configuration.Embedding().Model()
	dimensions := configuration.Embedding().Dimensions()
	return workflowE2EFixture{
		db:           db,
		tables:       resolvedTables,
		store:        store,
		ingestor:     ingestor,
		chunker:      chunker,
		embedder:     &countingEmbedder{delegate: realEmbedder},
		realEmbedder: realEmbedder,
		buildConfig: indexing.BuildConfig{
			Chunk: chunkConfig,
			Model: indexing.ModelProfile{
				Model:         model,
				Dimensions:    dimensions,
				Distance:      indexing.DistanceCosine,
				ConfigVersion: appconfig.EmbeddingModelConfigVersion,
			},
		},
		tenantID:         tenantID,
		knowledgeID:      "workflow-e2e-knowledge",
		markdownV1:       markdownV1,
		markdownV2:       markdownV2,
		plainText:        plainText,
		expectedModelKey: workflowE2EModelKey(model, dimensions),
	}
}

func (f workflowE2EFixture) newWorkflow(
	t *testing.T,
	ctx context.Context,
	embedder indexing.Embedder,
	store indexing.Store,
) *indexing.Workflow {
	t.Helper()
	workflow, err := indexing.New(ctx, indexing.Dependencies{
		Ingestor:    f.ingestor,
		Chunker:     f.chunker,
		Embedder:    embedder,
		Store:       store,
		BuildConfig: f.buildConfig,
	})
	if err != nil {
		t.Fatalf("indexing.New() error = %v", err)
	}
	return workflow
}

func (f workflowE2EFixture) request(
	name string,
	sourceURI string,
	setID indexstore.SetID,
	documentID string,
	canonicalURI string,
) indexing.Request {
	return indexing.Request{
		RunID:     f.runID(name),
		SourceURI: sourceURI,
		Index: indexing.IndexTarget{
			SetID:           setID,
			TenantID:        f.tenantID,
			KnowledgeBaseID: f.knowledgeID,
			DocumentID:      documentID,
			CanonicalURI:    canonicalURI,
			SourceName:      filepath.Base(sourceURI),
			Title:           "端到端验收语料",
		},
	}
}

func (f workflowE2EFixture) runID(name string) string {
	return "workflow-e2e-" + name + "-" + uuid.NewString()
}

type workflowE2ESnapshot struct {
	set        chunkSetModel
	chunks     []chunkModel
	embeddings []chunkEmbeddingModel
}

func (f workflowE2EFixture) loadSnapshot(
	t *testing.T,
	ctx context.Context,
	setID indexstore.SetID,
) workflowE2ESnapshot {
	t.Helper()
	var snapshot workflowE2ESnapshot
	if err := f.db.WithContext(ctx).Table(f.tables.ChunkSets).
		Where("id = ?", string(setID)).Take(&snapshot.set).Error; err != nil {
		t.Fatalf("查询 Set %s error = %v", setID, err)
	}
	if err := f.db.WithContext(ctx).Table(f.tables.Chunks).
		Where("chunk_set_id = ?", string(setID)).
		Order("sequence ASC").Find(&snapshot.chunks).Error; err != nil {
		t.Fatalf("查询 Chunk %s error = %v", setID, err)
	}
	if err := f.db.WithContext(ctx).Table(f.tables.ChunkEmbeddings).
		Where("chunk_set_id = ?", string(setID)).
		Order("chunk_id ASC, model_key ASC").Find(&snapshot.embeddings).Error; err != nil {
		t.Fatalf("查询 Embedding %s error = %v", setID, err)
	}
	return snapshot
}

func (f workflowE2EFixture) assertOnlyActiveSet(
	t *testing.T,
	ctx context.Context,
	documentID string,
	strategy string,
	want indexstore.SetID,
) {
	t.Helper()
	var activeIDs []string
	if err := f.db.WithContext(ctx).Table(f.tables.ChunkSets).
		Where(
			"tenant_id = ? AND knowledge_base_id = ? AND document_id = ? AND strategy_name = ? AND status = ?",
			f.tenantID,
			f.knowledgeID,
			documentID,
			strategy,
			setStatusActive,
		).
		Pluck("id", &activeIDs).Error; err != nil {
		t.Fatalf("查询 active Set error = %v", err)
	}
	if len(activeIDs) != 1 || activeIDs[0] != string(want) {
		t.Fatalf("active Set IDs = %#v, want [%s]", activeIDs, want)
	}
}

type snapshotExpectation struct {
	status           string
	strategy         string
	modelKey         string
	searchable       bool
	requireEmbedding bool
}

func assertSnapshot(t *testing.T, snapshot workflowE2ESnapshot, want snapshotExpectation) {
	t.Helper()
	if snapshot.set.Status != want.status || snapshot.set.StrategyName != want.strategy {
		t.Fatalf(
			"Set status=%q strategy=%q, want status=%q strategy=%q",
			snapshot.set.Status,
			snapshot.set.StrategyName,
			want.status,
			want.strategy,
		)
	}
	if len(snapshot.chunks) == 0 {
		t.Fatal("Set 没有持久化 Chunk")
	}
	if want.requireEmbedding && len(snapshot.embeddings) == 0 {
		t.Fatal("Set 没有持久化 Embedding")
	}
	for _, record := range snapshot.embeddings {
		if record.ModelKey != want.modelKey || record.EmbeddingTokenCount < 1 ||
			len(record.Embedding.Slice()) != appconfig.RequiredEmbeddingDimensions ||
			record.Searchable != want.searchable {
			t.Fatalf(
				"Embedding 契约不匹配 model_key=%q tokens=%d dimensions=%d searchable=%v",
				record.ModelKey,
				record.EmbeddingTokenCount,
				len(record.Embedding.Slice()),
				record.Searchable,
			)
		}
	}
}

func assertPublishedResult(
	t *testing.T,
	result indexing.Result,
	snapshot workflowE2ESnapshot,
	strategy string,
	modelKey string,
) {
	t.Helper()
	assertSnapshot(t, snapshot, snapshotExpectation{
		status:           setStatusActive,
		strategy:         strategy,
		modelKey:         modelKey,
		searchable:       true,
		requireEmbedding: true,
	})
	strategyName := ""
	if result.Chunking != nil {
		strategyName = result.Chunking.StrategyName
	}
	if result.Status != "published" || !result.Index.ValidationPassed || !result.Index.Published ||
		result.Index.ChunkCount != len(snapshot.chunks) ||
		result.Index.EmbeddingCount != len(snapshot.embeddings) ||
		result.Index.VectorDimension != appconfig.RequiredEmbeddingDimensions ||
		!strings.HasPrefix(modelKey, result.Index.EmbeddingModel+":") ||
		strategyName != strategy {
		t.Fatalf(
			"发布结果不匹配 status=%q validated=%v published=%v chunks=%d/%d embeddings=%d/%d dimensions=%d strategy=%q",
			result.Status,
			result.Index.ValidationPassed,
			result.Index.Published,
			result.Index.ChunkCount,
			len(snapshot.chunks),
			result.Index.EmbeddingCount,
			len(snapshot.embeddings),
			result.Index.VectorDimension,
			strategyName,
		)
	}
}

func assertParentChildRelations(t *testing.T, snapshot workflowE2ESnapshot) {
	t.Helper()
	parents := make(map[string]struct{})
	children := 0
	for _, chunk := range snapshot.chunks {
		switch chunk.Kind {
		case "parent":
			parents[chunk.ChunkID] = struct{}{}
		case "child":
			children++
			if chunk.ParentChunkID == nil {
				t.Fatalf("Child %s 没有 Parent 关系", chunk.ChunkID)
			}
		default:
			t.Fatalf("Parent-child Set 包含未知 Chunk kind %q", chunk.Kind)
		}
	}
	if len(parents) == 0 || children == 0 || children != len(snapshot.embeddings) {
		t.Fatalf("Parent-child 数量 parents=%d children=%d embeddings=%d", len(parents), children, len(snapshot.embeddings))
	}
	for _, chunk := range snapshot.chunks {
		if chunk.ParentChunkID == nil {
			continue
		}
		if _, ok := parents[*chunk.ParentChunkID]; !ok {
			t.Fatalf("Child %s 引用了不存在的 Parent %s", chunk.ChunkID, *chunk.ParentChunkID)
		}
	}
}

func writeWorkflowE2ECorpus(t *testing.T) (string, string, string) {
	t.Helper()
	directory := t.TempDir()
	markdownV1 := filepath.Join(directory, "structure-v1.md")
	markdownV2 := filepath.Join(directory, "structure-v2.md")
	plainText := filepath.Join(directory, "parent-child.txt")
	files := map[string]string{
		markdownV1: "# 索引验收\n\n## 构建\n\n" + strings.Repeat("构建阶段会解析文档、切分内容并生成向量。", 12) +
			"\n\n## 发布\n\n" + strings.Repeat("发布阶段会校验完整性并原子切换活动版本。", 12),
		markdownV2: "# 索引验收\n\n## 构建\n\n" + strings.Repeat("变更后的构建会生成新的内容哈希和索引版本。", 12) +
			"\n\n## 发布\n\n" + strings.Repeat("新版本发布后旧版本必须退出检索候选集合。", 12),
		plainText: strings.Repeat("这是用于 Parent-child 策略验收的普通文本，包含稳定的父子关系和相邻关系。", 14),
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("写入端到端语料 %s error = %v", filepath.Base(path), err)
		}
	}
	return markdownV1, markdownV2, plainText
}

func workflowE2EKnowledgePath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "testdata", "knowledge.md"))
	if err != nil {
		t.Fatalf("解析 testdata/knowledge.md 路径 error = %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("读取 testdata/knowledge.md error = %v", err)
	}
	return path
}

func workflowE2EModelKey(model string, dimensions int) string {
	canonical := fmt.Sprintf("%s\x00%d\x00%s\x00%s", model, dimensions, indexing.DistanceCosine, appconfig.EmbeddingModelConfigVersion)
	digest := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("%s:%x", model, digest)
}
