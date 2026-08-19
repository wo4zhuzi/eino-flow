package retrieval

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/compose"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	workflowruntime "github.com/wo4zhuzi/eino-flow/internal/workflow"
)

var descriptor = workflowruntime.Descriptor{
	Name:    "rag_vector_retrieval",
	Version: "v1",
}

// Workflow 保存编译一次、重复执行的 Eino 基础向量召回工作流。
type Workflow struct {
	runner *workflowruntime.Runner[Request, Result]
}

// Descriptor 返回稳定工作流名称和版本。
func Descriptor() workflowruntime.Descriptor {
	return descriptor
}

// New 使用启动层提供的依赖编译基础向量召回拓扑。
func New(ctx context.Context, dependencies Dependencies) (*Workflow, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := workflowruntime.RequireDependency("Embedder", dependencies.Embedder); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}
	if err := workflowruntime.RequireDependency("Store", dependencies.Store); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDependencies, err)
	}
	config, modelKey, err := normalizeConfig(dependencies.Config)
	if err != nil {
		return nil, fmt.Errorf("%w: Config: %w", ErrInvalidDependencies, err)
	}

	nodes := &workflowNodes{
		embedder: dependencies.Embedder,
		store:    dependencies.Store,
		model:    config.Model,
		modelKey: modelKey,
	}
	chain := compose.NewChain[Request, Result]()
	chain.
		AppendLambda(
			compose.InvokableLambda(nodes.embedQuery),
			compose.WithNodeKey(nodeEmbedQuery),
			compose.WithNodeName(nodeEmbedQuery),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.retrieveEvidence),
			compose.WithNodeKey(nodeRetrieveEvidence),
			compose.WithNodeName(nodeRetrieveEvidence),
		).
		AppendLambda(
			compose.InvokableLambda(nodes.buildResult),
			compose.WithNodeKey(nodeBuildResult),
			compose.WithNodeName(nodeBuildResult),
		)
	runner, err := workflowruntime.Compile[Request, Result](ctx, Descriptor(), chain)
	if err != nil {
		return nil, fmt.Errorf("编译检索工作流: %w", err)
	}
	return &Workflow{runner: runner}, nil
}

// Run 执行一次基础向量召回拓扑。
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
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	runOptions := append([]workflowruntime.RunOption(nil), opts...)
	runOptions = append(runOptions, workflowruntime.WithRunID(request.RunID))
	result, err := w.runner.Run(ctx, request, runOptions...)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func normalizeConfig(config Config) (Config, indexstore.ModelKey, error) {
	config.Model.Model = strings.TrimSpace(config.Model.Model)
	config.Model.Distance = strings.TrimSpace(config.Model.Distance)
	config.Model.ConfigVersion = strings.TrimSpace(config.Model.ConfigVersion)
	if !validModelProfile(config.Model) {
		return Config{}, "", indexstore.ErrInvalidModelProfile
	}
	modelKey, err := indexstore.NewModelKey(config.Model)
	if err != nil {
		return Config{}, "", err
	}
	return config, modelKey, nil
}
