package indexing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cloudwego/eino/schema"
	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-document-parser-structured/markdown"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	workflowruntime "github.com/wo4zhuzi/eino-flow/internal/workflow"
)

type stubIngestor struct {
	result   *ingestion.Result
	err      error
	received string
}

func (s *stubIngestor) Ingest(_ context.Context, uri string) (*ingestion.Result, error) {
	s.received = uri
	return s.result, s.err
}

type stubChunker struct {
	result   *chunking.Result
	err      error
	received *ingestion.Result
}

type stubEmbedder struct {
	err      error
	calls    int
	received [][]string
	order    *[]string
}

func (s *stubEmbedder) Embed(_ context.Context, texts []string) ([]embedding.Result, error) {
	if s.order != nil {
		*s.order = append(*s.order, "embed")
	}
	s.calls++
	s.received = append(s.received, append([]string(nil), texts...))
	if s.err != nil {
		return nil, s.err
	}
	results := make([]embedding.Result, len(texts))
	for index := range results {
		results[index] = embedding.Result{Vector: make([]float64, 1536), TokenCount: index + 1}
	}
	return results, nil
}

type stubStore struct {
	prepareErr    error
	saveErr       error
	validateErr   error
	publishErr    error
	prepared      []BuildData
	saved         [][]EmbeddingRecord
	validated     []indexstore.SetID
	published     []indexstore.SetID
	order         *[]string
	prepareResult []EmbeddingInput
}

func (s *stubStore) PrepareBuild(_ context.Context, build BuildData) ([]EmbeddingInput, error) {
	if s.order != nil {
		*s.order = append(*s.order, "prepare")
	}
	s.prepared = append(s.prepared, build)
	if s.prepareErr != nil {
		return nil, s.prepareErr
	}
	if s.prepareResult != nil {
		return append([]EmbeddingInput(nil), s.prepareResult...), nil
	}
	return append([]EmbeddingInput(nil), build.EmbeddingInputs...), nil
}

func (s *stubStore) SaveEmbeddings(_ context.Context, _ indexstore.SetID, records []EmbeddingRecord) error {
	if s.order != nil {
		*s.order = append(*s.order, "save")
	}
	s.saved = append(s.saved, append([]EmbeddingRecord(nil), records...))
	return s.saveErr
}

func (s *stubStore) Validate(_ context.Context, setID indexstore.SetID) error {
	if s.order != nil {
		*s.order = append(*s.order, "validate")
	}
	s.validated = append(s.validated, setID)
	return s.validateErr
}

func (s *stubStore) Publish(_ context.Context, setID indexstore.SetID) error {
	if s.order != nil {
		*s.order = append(*s.order, "publish")
	}
	s.published = append(s.published, setID)
	return s.publishErr
}

type retryStore struct {
	embedded     bool
	saveCalls    int
	publishCalls int
	publishCause error
}

type blockingEmbedder struct {
	started chan struct{}
}

func (s *blockingEmbedder) Embed(ctx context.Context, _ []string) ([]embedding.Result, error) {
	close(s.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *retryStore) PrepareBuild(_ context.Context, build BuildData) ([]EmbeddingInput, error) {
	if s.embedded {
		return nil, nil
	}
	return append([]EmbeddingInput(nil), build.EmbeddingInputs...), nil
}

func (s *retryStore) SaveEmbeddings(_ context.Context, _ indexstore.SetID, _ []EmbeddingRecord) error {
	s.embedded = true
	s.saveCalls++
	return nil
}

func (*retryStore) Validate(context.Context, indexstore.SetID) error {
	return nil
}

func (s *retryStore) Publish(context.Context, indexstore.SetID) error {
	s.publishCalls++
	if s.publishCalls == 1 {
		return s.publishCause
	}
	return nil
}

func TestWorkflowExecutesRealIndexLifecycle(t *testing.T) {
	chunkResult := parentChildResult()
	chunkResult.Profile = chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion}
	ingestor := &stubIngestor{result: plainIngestionResult()}
	chunker := &stubChunker{result: chunkResult}
	order := make([]string, 0, 5)
	embedder := &stubEmbedder{order: &order}
	store := &stubStore{order: &order}
	dependencies := validWorkflowDependencies(ingestor, chunker)
	dependencies.Embedder = embedder
	dependencies.Store = store
	workflow, err := New(context.Background(), dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := validWorkflowRequest("real-lifecycle", "document.txt")
	original := request

	result, err := workflow.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(request, original) {
		t.Fatalf("Run() mutated request: got=%#v want=%#v", request, original)
	}
	if !reflect.DeepEqual(order, []string{"prepare", "embed", "save", "validate", "publish"}) {
		t.Fatalf("dependency order = %#v", order)
	}
	if len(store.prepared) != 1 || len(store.saved) != 1 || len(store.validated) != 1 || len(store.published) != 1 {
		t.Fatalf("store calls: prepared=%d saved=%d validated=%d published=%d", len(store.prepared), len(store.saved), len(store.validated), len(store.published))
	}
	build := store.prepared[0]
	if build.Set.ID != request.Index.SetID || build.Set.Scope.TenantID != request.Index.TenantID ||
		build.Set.Document.SourceURI != request.Index.CanonicalURI || build.Set.Document.ContentSHA256 != strings.Repeat("b", 64) {
		t.Fatalf("prepared build = %#v", build.Set)
	}
	if len(store.saved[0]) != len(build.EmbeddingInputs) || embedder.calls != 1 {
		t.Fatalf("embedding calls=%d saved records=%d inputs=%d", embedder.calls, len(store.saved[0]), len(build.EmbeddingInputs))
	}
	if !result.Index.Published || !result.Index.ValidationPassed ||
		result.Index.GeneratedEmbeddingCount != len(build.EmbeddingInputs) || result.Index.ReusedEmbeddingCount != 0 {
		t.Fatalf("index result = %#v", result.Index)
	}
	assertStageStatuses(t, result.Stages)
}

func TestWorkflowRetryReusesSavedEmbeddings(t *testing.T) {
	chunkResult := parentChildResult()
	chunkResult.Profile = chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion}
	cause := errors.New("publish unavailable")
	store := &retryStore{publishCause: cause}
	embedder := &stubEmbedder{}
	dependencies := validWorkflowDependencies(&stubIngestor{result: plainIngestionResult()}, &stubChunker{result: chunkResult})
	dependencies.Embedder = embedder
	dependencies.Store = store
	workflow, err := New(context.Background(), dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := validWorkflowRequest("retry-run", "document.txt")

	_, err = workflow.Run(context.Background(), request)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), nodePublishIndex) || !strings.Contains(err.Error(), `run_id="retry-run"`) {
		t.Fatalf("first Run() error = %v", err)
	}
	result, err := workflow.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if embedder.calls != 1 || store.saveCalls != 1 || store.publishCalls != 2 {
		t.Fatalf("retry calls: embed=%d save=%d publish=%d", embedder.calls, store.saveCalls, store.publishCalls)
	}
	if result.Index.GeneratedEmbeddingCount != 0 || result.Index.ReusedEmbeddingCount != result.Index.EmbeddingCount {
		t.Fatalf("retry index result = %#v", result.Index)
	}
}

func TestWorkflowPreservesDownstreamNodeErrors(t *testing.T) {
	tests := []struct {
		name      string
		node      string
		configure func(error, *stubEmbedder, *stubStore)
	}{
		{name: "prepare", node: nodePrepareIndex, configure: func(err error, _ *stubEmbedder, store *stubStore) { store.prepareErr = err }},
		{name: "embed", node: nodeEmbedChunks, configure: func(err error, embedder *stubEmbedder, _ *stubStore) { embedder.err = err }},
		{name: "persist", node: nodePersistIndex, configure: func(err error, _ *stubEmbedder, store *stubStore) { store.saveErr = err }},
		{name: "validate", node: nodeValidateIndex, configure: func(err error, _ *stubEmbedder, store *stubStore) { store.validateErr = err }},
		{name: "publish", node: nodePublishIndex, configure: func(err error, _ *stubEmbedder, store *stubStore) { store.publishErr = err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunkResult := parentChildResult()
			chunkResult.Profile = chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion}
			embedder := &stubEmbedder{}
			store := &stubStore{}
			cause := errors.New(test.name + " failed")
			test.configure(cause, embedder, store)
			dependencies := validWorkflowDependencies(&stubIngestor{result: plainIngestionResult()}, &stubChunker{result: chunkResult})
			dependencies.Embedder = embedder
			dependencies.Store = store
			workflow, err := New(context.Background(), dependencies)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			_, err = workflow.Run(context.Background(), validWorkflowRequest("error-run", "document.txt"))
			if !errors.Is(err, cause) || !strings.Contains(err.Error(), test.node) || !strings.Contains(err.Error(), `run_id="error-run"`) {
				t.Fatalf("Run() error = %v", err)
			}
		})
	}
}

func TestWorkflowRejectsInvalidTargetAndStoreEmbeddingSelection(t *testing.T) {
	chunkResult := parentChildResult()
	chunkResult.Profile = chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion}
	dependencies := validWorkflowDependencies(&stubIngestor{result: plainIngestionResult()}, &stubChunker{result: chunkResult})
	workflow, err := New(context.Background(), dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	request := validWorkflowRequest("invalid-target", "document.txt")
	request.Index.SourceName = ""
	_, err = workflow.Run(context.Background(), request)
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), nodeIngestDocument) {
		t.Fatalf("Run(invalid target) error = %v", err)
	}

	store := &stubStore{prepareResult: []EmbeddingInput{{
		Candidate: indexstore.CandidateID{SetID: request.Index.SetID, ChunkID: "unknown"},
		ModelKey:  "unknown",
		Text:      "unknown",
	}}}
	dependencies.Store = store
	workflow, err = New(context.Background(), dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = workflow.Run(context.Background(), validWorkflowRequest("invalid-selection", "document.txt"))
	if !errors.Is(err, ErrInvalidEmbedding) || !strings.Contains(err.Error(), nodePrepareIndex) {
		t.Fatalf("Run(invalid selection) error = %v", err)
	}
}

func TestWorkflowCancelsInFlightEmbedding(t *testing.T) {
	chunkResult := parentChildResult()
	chunkResult.Profile = chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion}
	embedder := &blockingEmbedder{started: make(chan struct{})}
	dependencies := validWorkflowDependencies(&stubIngestor{result: plainIngestionResult()}, &stubChunker{result: chunkResult})
	dependencies.Embedder = embedder
	workflow, err := New(context.Background(), dependencies)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := workflow.Run(ctx, validWorkflowRequest("cancel-run", "document.txt"))
		done <- runErr
	}()
	<-embedder.started
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), nodeEmbedChunks) ||
		!strings.Contains(err.Error(), `run_id="cancel-run"`) {
		t.Fatalf("Run(canceled embedding) error = %v", err)
	}
}

func (s *stubChunker) Chunk(_ context.Context, result *ingestion.Result) (*chunking.Result, error) {
	s.received = result
	return s.result, s.err
}

func TestWorkflowUsesStructuredMarkdownAndStructureAwareChunking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knowledge.md")
	writeFile(t, path, "# 安装\n\n下载发布包并初始化配置。\n\n## 验证\n\n检查健康状态与日志。\n")
	workflow := newRealWorkflow(t)

	result, err := workflow.Run(context.Background(), validWorkflowRequest("markdown-run", path))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Workflow != "rag_document_indexing@v5" || result.Status != "published" {
		t.Fatalf("result identity = %#v", result)
	}
	if result.Parser != markdown.ParserInfo() {
		t.Fatalf("Parser = %#v, want %#v", result.Parser, markdown.ParserInfo())
	}
	if result.Chunking == nil || result.Chunking.StrategyName != structureaware.StructureAwareStrategyName {
		t.Fatalf("Chunking = %#v", result.Chunking)
	}
	if result.Chunking.Profile.Name != defaultProfileName || result.Chunking.Profile.Version != defaultProfileVersion {
		t.Fatalf("Chunking profile = %#v", result.Chunking.Profile)
	}
	if result.Chunking.AdapterName != "ingestion" || len(result.Chunking.Chunks) != 2 {
		t.Fatalf("Chunking = %#v", result.Chunking)
	}
	wantSemanticPaths := [][]string{{"安装"}, {"安装", "验证"}}
	for index, item := range result.Chunking.Chunks {
		if item.Kind != structureaware.ChunkKindStructure || len(item.SourceUnitIDs) == 0 {
			t.Fatalf("chunk = %#v", item)
		}
		structurePath, ok := item.Metadata[structureaware.MetadataStructurePath].([]string)
		if !ok || len(structurePath) == 0 || structurePath[len(structurePath)-1] != item.SourceUnitIDs[0] {
			t.Fatalf("chunk structure path = %#v", item.Metadata[structureaware.MetadataStructurePath])
		}
		semanticPath, ok := item.Metadata[structureaware.MetadataStructureSemanticPath].([]string)
		if !ok || !reflect.DeepEqual(semanticPath, wantSemanticPaths[index]) {
			t.Fatalf("chunk semantic path = %#v, want %#v", semanticPath, wantSemanticPaths[index])
		}
	}
	assertStageStatuses(t, result.Stages)
	if !result.Index.Published || !result.Index.ValidationPassed || result.Index.ChunkCount != 2 || result.Index.EmbeddingCount != 2 {
		t.Fatalf("Index = %#v", result.Index)
	}
}

func TestWorkflowPrependsReadableContextWithoutLeakingStructureIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.md")
	writeFile(t, path, "# 安装\n\n```sh\nrun-install\n```\n")
	workflow := newRealWorkflow(t)

	result, err := workflow.Run(context.Background(), validWorkflowRequest("semantic-context-run", path))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Chunking.Chunks) != 2 {
		t.Fatalf("chunks = %#v", result.Chunking.Chunks)
	}
	contextChunk := result.Chunking.Chunks[1]
	if contextChunk.Content != "安装\n\n```sh\nrun-install\n```" || strings.Contains(contextChunk.Content, "md_") {
		t.Fatalf("context chunk content = %q", contextChunk.Content)
	}
	wantSemanticPath := []string{"安装"}
	if got := contextChunk.Metadata[structureaware.MetadataStructureSemanticPath]; !reflect.DeepEqual(got, wantSemanticPath) {
		t.Fatalf("semantic path = %#v, want %#v", got, wantSemanticPath)
	}
}

func TestWorkflowUsesParentChildForPlainText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knowledge.txt")
	writeFile(t, path, "普通文本使用 Parent-child Chunk。")
	workflow := newRealWorkflow(t)

	result, err := workflow.Run(context.Background(), validWorkflowRequest("text-run", path))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Parser.Output.Structured || result.Chunking.StrategyName != parentchild.ParentChildStrategyName {
		t.Fatalf("Parser=%#v Chunking=%#v", result.Parser, result.Chunking)
	}
	if result.Chunking.Statistics.ParentCount == 0 || result.Chunking.Statistics.ChildCount == 0 {
		t.Fatalf("Statistics = %#v", result.Chunking.Statistics)
	}
	for _, item := range result.Chunking.Chunks {
		if item.Kind != chunking.ChunkKindParent && item.Kind != chunking.ChunkKindChild {
			t.Fatalf("chunk = %#v", item)
		}
	}
	assertStageStatuses(t, result.Stages)
}

func TestWorkflowPreservesStructuredIDsAndDoesNotMutateIngestorResult(t *testing.T) {
	path := []string{"root", "child"}
	rootMetadata := map[string]any{
		ingestion.MetadataStructureKind:     "heading",
		ingestion.MetadataStructureDepth:    0,
		ingestion.MetadataStructurePath:     []string{"root"},
		ingestion.MetadataStructureBoundary: "hard",
		ingestion.MetadataStructureLabel:    "根标题",
	}
	childMetadata := map[string]any{
		ingestion.MetadataStructureKind:     "paragraph",
		ingestion.MetadataStructureDepth:    1,
		ingestion.MetadataStructurePath:     path,
		ingestion.MetadataStructureParentID: "root",
	}
	documents := []*schema.Document{
		{ID: "root", Content: "# 根标题", MetaData: rootMetadata},
		{ID: "child", Content: "正文", MetaData: childMetadata},
	}
	ingestor := &stubIngestor{result: &ingestion.Result{
		Source: ingestion.SourceInfo{
			URI:       "/documents/guide.md",
			FileName:  "guide.md",
			Extension: ingestion.ExtensionMarkdown,
			MIMEType:  "text/markdown",
			SHA256:    strings.Repeat("a", 64),
		},
		Parser:    markdown.ParserInfo(),
		Documents: documents,
	}}
	chunker := &stubChunker{result: &chunking.Result{
		Profile:      chunking.Profile{Name: defaultProfileName, Version: defaultProfileVersion},
		StrategyName: structureaware.StructureAwareStrategyName,
		Chunks: []chunking.Chunk{{
			ID:             "chunk",
			Kind:           structureaware.ChunkKindStructure,
			Content:        "正文",
			DocumentID:     "document-1",
			SourceUnitIDs:  []string{"root"},
			Sequence:       1,
			CharacterCount: 2,
			Metadata:       map[string]any{},
		}},
	}}
	workflow := newWorkflow(t, ingestor, chunker)

	request := validWorkflowRequest("  stable-ids  ", "  /documents/guide.md  ")
	request.Index.TenantID = "  tenant-1  "
	result, err := workflow.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if ingestor.received != "/documents/guide.md" || result.RunID != "stable-ids" {
		t.Fatalf("received=%q runID=%q", ingestor.received, result.RunID)
	}
	if chunker.received.Documents[0].ID != "root" || chunker.received.Documents[1].ID != "child" {
		t.Fatalf("prepared IDs = %q, %q", chunker.received.Documents[0].ID, chunker.received.Documents[1].ID)
	}
	if chunker.received.Documents[1].MetaData[ingestion.MetadataStructureParentID] != "root" {
		t.Fatalf("prepared child metadata = %#v", chunker.received.Documents[1].MetaData)
	}
	if chunker.received.Documents[0].MetaData[ingestion.MetadataStructureLabel] != "根标题" {
		t.Fatalf("prepared root metadata = %#v", chunker.received.Documents[0].MetaData)
	}
	if _, ok := chunker.received.Documents[0].MetaData["document_id"]; !ok {
		t.Fatalf("prepared metadata = %#v", chunker.received.Documents[0].MetaData)
	}
	if len(rootMetadata) != 5 || len(childMetadata) != 4 || documents[0].ID != "root" || documents[1].ID != "child" {
		t.Fatalf("摄取结果被原地修改: %#v", documents)
	}
}

func TestWorkflowPreservesDependencyAndContextErrors(t *testing.T) {
	ingestor := &stubIngestor{err: ingestion.ErrUnsupportedFormat}
	chunker := &stubChunker{}
	workflow := newWorkflow(t, ingestor, chunker)
	_, err := workflow.Run(context.Background(), validWorkflowRequest("run", "document.csv"))
	if !errors.Is(err, ingestion.ErrUnsupportedFormat) {
		t.Fatalf("Run(ingestion error) = %v", err)
	}
	var operationError *workflowruntime.OperationError
	if !errors.As(err, &operationError) ||
		operationError.Execution.Descriptor != Descriptor() ||
		operationError.Execution.RunID != "run" ||
		operationError.Operation != workflowruntime.OperationRun {
		t.Fatalf("OperationError = %#v", operationError)
	}

	ingestor.err = nil
	ingestor.result = plainIngestionResult()
	chunker.err = chunking.ErrOversizeBlock
	_, err = workflow.Run(context.Background(), validWorkflowRequest("run", "document.md"))
	if !errors.Is(err, chunking.ErrOversizeBlock) {
		t.Fatalf("Run(chunk error) = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = workflow.Run(canceled, validWorkflowRequest("run", "document.txt"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) = %v", err)
	}
}

func TestStructuredMarkdownRejectsOversizeAtomicBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversize.md")
	writeFile(t, path, "```text\n"+strings.Repeat("x", defaultStructureMaxRunes+1)+"\n```\n")
	workflow := newRealWorkflow(t)

	_, err := workflow.Run(context.Background(), validWorkflowRequest("oversize", path))
	if !errors.Is(err, chunking.ErrOversizeBlock) {
		t.Fatalf("Run() error = %v, want %v", err, chunking.ErrOversizeBlock)
	}
}

func TestWorkflowAndChunkerValidateBoundaries(t *testing.T) {
	ingestor := &stubIngestor{result: plainIngestionResult()}
	chunker := &stubChunker{result: &chunking.Result{}}
	workflow := newWorkflow(t, ingestor, chunker)
	request := validWorkflowRequest("", "document.txt")
	if _, err := workflow.Run(context.Background(), request); !errors.Is(err, ErrInvalidRunID) {
		t.Fatalf("Run(empty run ID) = %v", err)
	}
	ingestor.result = nil
	if _, err := workflow.Run(context.Background(), validWorkflowRequest("run", "document.txt")); !errors.Is(err, ingestion.ErrNoParsedContent) {
		t.Fatalf("Run(nil ingestion result) = %v", err)
	}
	ingestor.result = plainIngestionResult()
	chunker.result = nil
	if _, err := workflow.Run(context.Background(), validWorkflowRequest("run", "document.txt")); !errors.Is(err, chunking.ErrNoValidChunks) {
		t.Fatalf("Run(nil chunk result) = %v", err)
	}
	dependencies := validWorkflowDependencies(ingestor, chunker)
	if _, err := New(nil, dependencies); !errors.Is(err, ErrNilContext) {
		t.Fatalf("New(nil) = %v", err)
	}
	missingIngestor := dependencies
	missingIngestor.Ingestor = nil
	if _, err := New(context.Background(), missingIngestor); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil ingestor) = %v", err)
	}
	missingChunker := dependencies
	missingChunker.Chunker = nil
	if _, err := New(context.Background(), missingChunker); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil chunker) = %v", err)
	}
	missingEmbedder := dependencies
	missingEmbedder.Embedder = nil
	if _, err := New(context.Background(), missingEmbedder); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil embedder) = %v", err)
	}
	missingStore := dependencies
	missingStore.Store = nil
	if _, err := New(context.Background(), missingStore); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil store) = %v", err)
	}
	invalidBuildConfig := dependencies
	invalidBuildConfig.BuildConfig = BuildConfig{}
	if _, err := New(context.Background(), invalidBuildConfig); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(invalid build config) = %v", err)
	}
	var unavailable *Workflow
	if _, err := unavailable.Run(context.Background(), Request{}); !errors.Is(err, ErrWorkflowUnavailable) {
		t.Fatalf("nil Workflow.Run() = %v", err)
	}

	invalidConfigs := []ChunkConfig{
		{},
		{ProfileName: "profile", ProfileVersion: "v1", ParentMaxRunes: -1, ChildMaxRunes: 1, StructureMaxRunes: 1, StructureMinRunes: 1},
		{ProfileName: "profile", ProfileVersion: "v1", ParentMaxRunes: 1, ChildMaxRunes: -1, StructureMaxRunes: 1, StructureMinRunes: 1},
		{ProfileName: "profile", ProfileVersion: "v1", ParentMaxRunes: 1, ChildMaxRunes: 1, StructureMaxRunes: 1, StructureMinRunes: 2},
	}
	for _, config := range invalidConfigs {
		if _, err := NewAutomaticChunker(config); !errors.Is(err, ErrInvalidChunkConfig) {
			t.Fatalf("NewAutomaticChunker(%#v) = %v", config, err)
		}
	}
	var automatic *AutomaticChunker
	if _, err := automatic.Chunk(context.Background(), plainIngestionResult()); !errors.Is(err, ErrInvalidChunkConfig) {
		t.Fatalf("nil AutomaticChunker.Chunk() = %v", err)
	}
}

func newRealWorkflow(t *testing.T) *Workflow {
	t.Helper()
	ctx := context.Background()
	registry, err := ingestion.NewDefaultRegistry(ctx)
	if err != nil {
		t.Fatalf("NewDefaultRegistry() error = %v", err)
	}
	if err := registry.ReplaceParser(ingestion.ExtensionMarkdown, markdown.ParserInfo(), markdown.New()); err != nil {
		t.Fatalf("ReplaceParser() error = %v", err)
	}
	ingestor, err := ingestion.New(ctx, ingestion.Config{
		MaxFileBytes: ingestion.DefaultMaxFileBytes,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("ingestion.New() error = %v", err)
	}
	chunker, err := NewAutomaticChunker(DefaultChunkConfig())
	if err != nil {
		t.Fatalf("NewAutomaticChunker() error = %v", err)
	}
	return newWorkflow(t, ingestor, chunker)
}

func newWorkflow(t *testing.T, ingestor Ingestor, chunker Chunker) *Workflow {
	t.Helper()
	workflow, err := New(context.Background(), validWorkflowDependencies(ingestor, chunker))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return workflow
}

func validWorkflowDependencies(ingestor Ingestor, chunker Chunker) Dependencies {
	return Dependencies{
		Ingestor: ingestor,
		Chunker:  chunker,
		Embedder: &stubEmbedder{},
		Store:    &stubStore{},
		BuildConfig: BuildConfig{
			Chunk: DefaultChunkConfig(),
			Model: ModelProfile{
				Model:         "text-embedding-v4",
				Dimensions:    1536,
				Distance:      DistanceCosine,
				ConfigVersion: "v1",
			},
		},
	}
}

func validWorkflowRequest(runID, sourceURI string) Request {
	return Request{
		RunID:     runID,
		SourceURI: sourceURI,
		Index: IndexTarget{
			SetID:           "11111111-1111-4111-8111-111111111111",
			TenantID:        "tenant-1",
			KnowledgeBaseID: "kb-1",
			DocumentID:      "document-1",
			CanonicalURI:    "knowledge://documents/document-1",
			SourceName:      "document.md",
			Title:           "文档",
		},
	}
}

func plainIngestionResult() *ingestion.Result {
	return &ingestion.Result{
		Source: ingestion.SourceInfo{
			URI:       "/documents/knowledge.txt",
			FileName:  "knowledge.txt",
			Extension: ingestion.ExtensionText,
			MIMEType:  "text/plain",
			SHA256:    strings.Repeat("b", 64),
		},
		Parser: ingestion.ParserInfo{
			Name:    "text",
			Version: "v1",
			Output: ingestion.ParserOutput{
				Granularity: ingestion.GranularityDocument,
			},
		},
		Documents: []*schema.Document{{Content: "普通文本"}},
	}
}

func assertStageStatuses(t *testing.T, stages []StageResult) {
	t.Helper()
	wantNames := []string{
		nodeIngestDocument,
		nodeChunkDocument,
		nodePrepareIndex,
		nodeEmbedChunks,
		nodePersistIndex,
		nodeValidateIndex,
		nodePublishIndex,
		nodeBuildResult,
	}
	if len(stages) != len(wantNames) {
		t.Fatalf("stages = %#v", stages)
	}
	for index, name := range wantNames {
		if stages[index].Name != name || stages[index].Status != StageStatusCompleted {
			t.Fatalf("stage[%d] = %#v, want name=%s status=%s", index, stages[index], name, StageStatusCompleted)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
