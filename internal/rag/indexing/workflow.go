package indexing

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/compose"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	workflowruntime "github.com/wo4zhuzi/eino-flow/internal/workflow"
)

const (
	nodeIngestDocument = "ingest_document"
	nodeChunkDocument  = "chunk_document"
	nodePrepareIndex   = "prepare_index"
	nodeEmbedChunks    = "embed_chunks"
	nodePersistIndex   = "persist_index"
	nodeValidateIndex  = "validate_index"
	nodePublishIndex   = "publish_index"
	nodeBuildResult    = "build_result"
)

var descriptor = workflowruntime.Descriptor{
	Name:    "rag_document_indexing",
	Version: "v5",
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
	if err := workflowruntime.RequireDependency("Embedder", dependencies.Embedder); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}
	if err := workflowruntime.RequireDependency("Store", dependencies.Store); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}
	buildConfig, err := normalizeWorkflowBuildConfig(dependencies.BuildConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: BuildConfig: %w", ErrInvalidDependencies, err)
	}

	nodes := &workflowNodes{
		ingestor:    dependencies.Ingestor,
		chunker:     dependencies.Chunker,
		embedder:    dependencies.Embedder,
		store:       dependencies.Store,
		buildConfig: buildConfig,
	}
	chain := compose.NewChain[Request, Result]()
	chain.
		AppendLambda(
			compose.InvokableLambda(nodes.ingest),
			compose.WithNodeKey(nodeIngestDocument),
			compose.WithNodeName(nodeIngestDocument),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.chunk),
			compose.WithNodeKey(nodeChunkDocument),
			compose.WithNodeName(nodeChunkDocument),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.prepareIndex),
			compose.WithNodeKey(nodePrepareIndex),
			compose.WithNodeName(nodePrepareIndex),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.embedChunks),
			compose.WithNodeKey(nodeEmbedChunks),
			compose.WithNodeName(nodeEmbedChunks),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.persistIndex),
			compose.WithNodeKey(nodePersistIndex),
			compose.WithNodeName(nodePersistIndex),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.validateIndex),
			compose.WithNodeKey(nodeValidateIndex),
			compose.WithNodeName(nodeValidateIndex),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.publishIndex),
			compose.WithNodeKey(nodePublishIndex),
			compose.WithNodeName(nodePublishIndex),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.buildResult),
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
	request = normalizeRequest(request)
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

func normalizeRequest(request Request) Request {
	request.RunID = strings.TrimSpace(request.RunID)
	request.SourceURI = strings.TrimSpace(request.SourceURI)
	request.Index.SetID = indexstore.SetID(strings.TrimSpace(string(request.Index.SetID)))
	request.Index.TenantID = strings.TrimSpace(request.Index.TenantID)
	request.Index.KnowledgeBaseID = strings.TrimSpace(request.Index.KnowledgeBaseID)
	request.Index.DocumentID = strings.TrimSpace(request.Index.DocumentID)
	request.Index.CanonicalURI = strings.TrimSpace(request.Index.CanonicalURI)
	request.Index.SourceName = strings.TrimSpace(request.Index.SourceName)
	request.Index.Title = strings.TrimSpace(request.Index.Title)
	return request
}

func normalizeWorkflowBuildConfig(config BuildConfig) (BuildConfig, error) {
	config.Chunk.ProfileName = strings.TrimSpace(config.Chunk.ProfileName)
	config.Chunk.ProfileVersion = strings.TrimSpace(config.Chunk.ProfileVersion)
	config.Model.Model = strings.TrimSpace(config.Model.Model)
	config.Model.Distance = strings.TrimSpace(config.Model.Distance)
	config.Model.ConfigVersion = strings.TrimSpace(config.Model.ConfigVersion)
	if config.Chunk.ProfileName == "" || config.Chunk.ProfileVersion == "" ||
		config.Chunk.ParentMaxRunes < 1 || config.Chunk.ChildMaxRunes < 1 ||
		config.Chunk.StructureMaxRunes < 1 || config.Chunk.StructureMinRunes < 1 ||
		config.Chunk.StructureMinRunes > config.Chunk.StructureMaxRunes {
		return BuildConfig{}, ErrInvalidChunkConfig
	}
	if _, err := makeModelKey(config.Model); err != nil {
		return BuildConfig{}, err
	}
	return config, nil
}
