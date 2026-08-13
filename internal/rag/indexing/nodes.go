package indexing

import (
	"context"
	"fmt"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
)

type workflowNodes struct {
	ingestor Ingestor
	chunker  Chunker
}

func (n *workflowNodes) ingest(ctx context.Context, request Request) (workflowState, error) {
	if request.RunID == "" {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, ErrInvalidRunID)
	}
	result, err := n.ingestor.Ingest(ctx, request.SourceURI)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, err)
	}
	if result == nil {
		return workflowState{}, fmt.Errorf("%s: %w: 摄取器返回 nil 结果", nodeIngestDocument, ingestion.ErrNoParsedContent)
	}
	source, prepared, err := prepareForIndexing(result)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, err)
	}
	return workflowState{
		request:  request,
		source:   source,
		ingested: prepared,
		stages: []StageResult{{
			Name:   nodeIngestDocument,
			Status: StageStatusCompleted,
			Detail: fmt.Sprintf("Package Loader 与 Parser 已输出 %d 个标准化单元", len(prepared.Documents)),
		}},
	}, nil
}

func (n *workflowNodes) chunk(ctx context.Context, state workflowState) (workflowState, error) {
	result, err := n.chunker.Chunk(ctx, state.ingested)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeChunkDocument, err)
	}
	if result == nil {
		return workflowState{}, fmt.Errorf("%s: %w: Chunker 返回 nil 结果", nodeChunkDocument, chunking.ErrNoValidChunks)
	}
	state.chunking = result
	state.stages = append(state.stages, StageResult{
		Name:   nodeChunkDocument,
		Status: StageStatusCompleted,
		Detail: fmt.Sprintf("Package Chunker 使用 %s 策略生成 %d 个 Chunk", result.StrategyName, len(result.Chunks)),
	})
	return state, nil
}

func (*workflowNodes) simulateEmbedding(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodeEmbedChunks); err != nil {
		return workflowState{}, err
	}
	state.placeholder.Simulated = true
	state.placeholder.EmbeddingModel = "not-configured"
	state.placeholder.VectorDimension = 0
	state.stages = append(state.stages, StageResult{
		Name:   nodeEmbedChunks,
		Status: StageStatusSimulated,
		Detail: "已有真实 Chunk，但未调用 Embedding 服务",
	})
	return state, nil
}

func (*workflowNodes) simulatePersist(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodePersistIndex); err != nil {
		return workflowState{}, err
	}
	state.placeholder.PersistedRecordCount = 0
	state.stages = append(state.stages, StageResult{
		Name:   nodePersistIndex,
		Status: StageStatusSimulated,
		Detail: "未连接 PostgreSQL，持久化记录数固定为 0",
	})
	return state, nil
}

func (*workflowNodes) simulateValidate(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodeValidateIndex); err != nil {
		return workflowState{}, err
	}
	state.placeholder.ValidationPassed = false
	state.stages = append(state.stages, StageResult{
		Name:   nodeValidateIndex,
		Status: StageStatusSimulated,
		Detail: "没有真实向量和存储记录，因此不执行索引校验",
	})
	return state, nil
}

func (*workflowNodes) simulatePublish(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodePublishIndex); err != nil {
		return workflowState{}, err
	}
	state.placeholder.PublishedIndexVersion = ""
	state.stages = append(state.stages, StageResult{
		Name:   nodePublishIndex,
		Status: StageStatusSimulated,
		Detail: "未发布索引版本，线上活动索引不会发生变化",
	})
	return state, nil
}

func (*workflowNodes) buildResult(ctx context.Context, state workflowState) (Result, error) {
	if err := contextError(ctx, nodeBuildResult); err != nil {
		return Result{}, err
	}
	state.stages = append(state.stages, StageResult{
		Name:   nodeBuildResult,
		Status: StageStatusCompleted,
		Detail: "已生成包含完整 Chunk、关系和统计的工作流结果",
	})
	return Result{
		Workflow:    Descriptor().String(),
		RunID:       state.request.RunID,
		Status:      "chunked_with_simulated_downstream",
		Source:      state.source,
		Parser:      state.ingested.Parser,
		Chunking:    state.chunking,
		Stages:      append([]StageResult(nil), state.stages...),
		Placeholder: state.placeholder,
	}, nil
}

func contextError(ctx context.Context, operation string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}
