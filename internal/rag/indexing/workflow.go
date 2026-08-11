package indexing

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/compose"
	workflowruntime "github.com/wo4zhuzi/eino-flow/internal/platform/workflow"
)

const (
	nodeIngestDocument = "ingest_document"
	nodeChunkDocument  = "chunk_document"
	nodeEmbedChunks    = "embed_chunks"
	nodePersistIndex   = "persist_index"
	nodeValidateIndex  = "validate_index"
	nodePublishIndex   = "publish_index"
	nodeBuildResult    = "build_result"
)

var descriptor = workflowruntime.Descriptor{
	Name:    "rag_document_indexing",
	Version: "v4",
}

// Workflow 保存编译一次、重复执行的 Eino 索引工作流。
type Workflow struct {
	runner *workflowruntime.Runner[Request, Result]
}

// Descriptor 返回稳定工作流名称和版本。
func Descriptor() workflowruntime.Descriptor {
	return descriptor
}

// New 使用应用启动层提供的依赖编译完整索引拓扑。
func New(ctx context.Context, dependencies Dependencies) (*Workflow, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := workflowruntime.RequireDependency("Ingestor", dependencies.Ingestor); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}
	if err := workflowruntime.RequireDependency("Chunker", dependencies.Chunker); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}

	handlers := &workflowHandlers{
		ingestor: dependencies.Ingestor,
		chunker:  dependencies.Chunker,
	}
	chain := compose.NewChain[Request, Result]()
	chain.
		AppendLambda(
			compose.InvokableLambda(handlers.ingest),
			compose.WithNodeKey(nodeIngestDocument),
			compose.WithNodeName(nodeIngestDocument),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.chunk),
			compose.WithNodeKey(nodeChunkDocument),
			compose.WithNodeName(nodeChunkDocument),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.simulateEmbedding),
			compose.WithNodeKey(nodeEmbedChunks),
			compose.WithNodeName(nodeEmbedChunks),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.simulatePersist),
			compose.WithNodeKey(nodePersistIndex),
			compose.WithNodeName(nodePersistIndex),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.simulateValidate),
			compose.WithNodeKey(nodeValidateIndex),
			compose.WithNodeName(nodeValidateIndex),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.simulatePublish),
			compose.WithNodeKey(nodePublishIndex),
			compose.WithNodeName(nodePublishIndex),
		).
		AppendLambda(
			compose.InvokableLambda(handlers.buildResult),
			compose.WithNodeKey(nodeBuildResult),
			compose.WithNodeName(nodeBuildResult),
		)
	runner, err := workflowruntime.Compile[Request, Result](ctx, Descriptor(), chain)
	if err != nil {
		return nil, fmt.Errorf("编译索引工作流: %w", err)
	}
	return &Workflow{runner: runner}, nil
}

// Run 执行一次完整索引拓扑。
func (w *Workflow) Run(
	ctx context.Context,
	request Request,
	opts ...workflowruntime.RunOption,
) (Result, error) {
	if ctx == nil {
		return Result{}, ErrNilContext
	}
	if w == nil || w.runner == nil {
		return Result{}, ErrWorkflowUnavailable
	}
	request.RunID = strings.TrimSpace(request.RunID)
	request.SourceURI = strings.TrimSpace(request.SourceURI)
	if request.RunID == "" {
		return Result{}, ErrInvalidRunID
	}
	runOptions := append([]workflowruntime.RunOption(nil), opts...)
	runOptions = append(runOptions, workflowruntime.WithRunID(request.RunID))
	result, err := w.runner.Run(ctx, request, runOptions...)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}
