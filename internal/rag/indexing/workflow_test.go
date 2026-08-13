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

func (s *stubChunker) Chunk(_ context.Context, result *ingestion.Result) (*chunking.Result, error) {
	s.received = result
	return s.result, s.err
}

func TestWorkflowUsesStructuredMarkdownAndStructureAwareChunking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knowledge.md")
	writeFile(t, path, "# 安装\n\n下载发布包并初始化配置。\n\n## 验证\n\n检查健康状态与日志。\n")
	workflow := newRealWorkflow(t)

	result, err := workflow.Run(context.Background(), Request{
		RunID:     "markdown-run",
		SourceURI: path,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Workflow != "rag_document_indexing@v4" || result.Status != "chunked_with_simulated_downstream" {
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
	if !result.Placeholder.Simulated || result.Placeholder.PersistedRecordCount != 0 {
		t.Fatalf("Placeholder = %#v", result.Placeholder)
	}
}

func TestWorkflowPrependsReadableContextWithoutLeakingStructureIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.md")
	writeFile(t, path, "# 安装\n\n```sh\nrun-install\n```\n")
	workflow := newRealWorkflow(t)

	result, err := workflow.Run(context.Background(), Request{
		RunID:     "semantic-context-run",
		SourceURI: path,
	})
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

	result, err := workflow.Run(context.Background(), Request{
		RunID:     "text-run",
		SourceURI: path,
	})
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
		StrategyName: structureaware.StructureAwareStrategyName,
		Chunks:       []chunking.Chunk{{ID: "chunk", Content: "正文"}},
	}}
	workflow := newWorkflow(t, ingestor, chunker)

	result, err := workflow.Run(context.Background(), Request{
		RunID:     "  stable-ids  ",
		SourceURI: "  /documents/guide.md  ",
	})
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
	_, err := workflow.Run(context.Background(), Request{RunID: "run", SourceURI: "document.csv"})
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
	_, err = workflow.Run(context.Background(), Request{RunID: "run", SourceURI: "document.md"})
	if !errors.Is(err, chunking.ErrOversizeBlock) {
		t.Fatalf("Run(chunk error) = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = workflow.Run(canceled, Request{RunID: "run", SourceURI: "document.txt"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) = %v", err)
	}
}

func TestStructuredMarkdownRejectsOversizeAtomicBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversize.md")
	writeFile(t, path, "```text\n"+strings.Repeat("x", defaultStructureMaxRunes+1)+"\n```\n")
	workflow := newRealWorkflow(t)

	_, err := workflow.Run(context.Background(), Request{RunID: "oversize", SourceURI: path})
	if !errors.Is(err, chunking.ErrOversizeBlock) {
		t.Fatalf("Run() error = %v, want %v", err, chunking.ErrOversizeBlock)
	}
}

func TestWorkflowAndChunkerValidateBoundaries(t *testing.T) {
	ingestor := &stubIngestor{result: plainIngestionResult()}
	chunker := &stubChunker{result: &chunking.Result{}}
	workflow := newWorkflow(t, ingestor, chunker)
	if _, err := workflow.Run(context.Background(), Request{SourceURI: "document.txt"}); !errors.Is(err, ErrInvalidRunID) {
		t.Fatalf("Run(empty run ID) = %v", err)
	}
	ingestor.result = nil
	if _, err := workflow.Run(context.Background(), Request{RunID: "run", SourceURI: "document.txt"}); !errors.Is(err, ingestion.ErrNoParsedContent) {
		t.Fatalf("Run(nil ingestion result) = %v", err)
	}
	ingestor.result = plainIngestionResult()
	chunker.result = nil
	if _, err := workflow.Run(context.Background(), Request{RunID: "run", SourceURI: "document.txt"}); !errors.Is(err, chunking.ErrNoValidChunks) {
		t.Fatalf("Run(nil chunk result) = %v", err)
	}
	if _, err := New(nil, Dependencies{Ingestor: ingestor, Chunker: chunker}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("New(nil) = %v", err)
	}
	if _, err := New(context.Background(), Dependencies{Chunker: chunker}); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil ingestor) = %v", err)
	}
	if _, err := New(context.Background(), Dependencies{Ingestor: ingestor}); !errors.Is(err, ErrInvalidDependencies) {
		t.Fatalf("New(nil chunker) = %v", err)
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
	workflow, err := New(context.Background(), Dependencies{Ingestor: ingestor, Chunker: chunker})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return workflow
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
		status := StageStatusSimulated
		if name == nodeIngestDocument || name == nodeChunkDocument || name == nodeBuildResult {
			status = StageStatusCompleted
		}
		if stages[index].Name != name || stages[index].Status != status {
			t.Fatalf("stage[%d] = %#v, want name=%s status=%s", index, stages[index], name, status)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
