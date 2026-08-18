package indexing

import (
	"context"
	"fmt"
	"math"
	"strings"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

type workflowNodes struct {
	ingestor    Ingestor
	chunker     Chunker
	embedder    Embedder
	store       Store
	buildConfig BuildConfig
}

func (n *workflowNodes) ingest(ctx context.Context, request Request) (workflowState, error) {
	if request.RunID == "" {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, ErrInvalidRunID)
	}
	if err := validateIndexTarget(request.Index); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, err)
	}
	result, err := n.ingestor.Ingest(ctx, request.SourceURI)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeIngestDocument, err)
	}
	if result == nil {
		return workflowState{}, fmt.Errorf("%s: %w: 摄取器返回 nil 结果", nodeIngestDocument, ingestion.ErrNoParsedContent)
	}
	source, prepared, err := prepareForIndexing(result, request.Index.DocumentID)
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

func (n *workflowNodes) prepareIndex(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodePrepareIndex); err != nil {
		return workflowState{}, err
	}
	spec, err := n.buildSpec(state)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodePrepareIndex, err)
	}
	build, err := MapBuild(spec, state.chunking)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodePrepareIndex, err)
	}
	pending, err := n.store.PrepareBuild(ctx, build)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodePrepareIndex, err)
	}
	if err := validatePendingEmbeddingInputs(build.EmbeddingInputs, pending); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodePrepareIndex, err)
	}
	state.build = build
	state.pendingEmbedding = append([]EmbeddingInput(nil), pending...)
	state.index = IndexResult{
		SetID:                build.Set.ID,
		EmbeddingModel:       n.buildConfig.Model.Model,
		VectorDimension:      n.buildConfig.Model.Dimensions,
		ChunkCount:           len(build.Chunks),
		EmbeddingCount:       len(build.EmbeddingInputs),
		ReusedEmbeddingCount: len(build.EmbeddingInputs) - len(pending),
	}
	state.stages = append(state.stages, StageResult{
		Name:   nodePrepareIndex,
		Status: StageStatusCompleted,
		Detail: fmt.Sprintf("已持久化 building Set 和 %d 个 Chunk，%d 个向量输入待生成", len(build.Chunks), len(pending)),
	})
	return state, nil
}

func (n *workflowNodes) embedChunks(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodeEmbedChunks); err != nil {
		return workflowState{}, err
	}
	if len(state.pendingEmbedding) > 0 {
		texts := make([]string, len(state.pendingEmbedding))
		for index, input := range state.pendingEmbedding {
			texts[index] = input.Text
		}
		results, err := n.embedder.Embed(ctx, texts)
		if err != nil {
			return workflowState{}, fmt.Errorf("%s: %w", nodeEmbedChunks, err)
		}
		if len(results) != len(state.pendingEmbedding) {
			return workflowState{}, fmt.Errorf("%s: %w: 返回数量不匹配", nodeEmbedChunks, ErrInvalidEmbedding)
		}
		state.embeddingRecords = make([]EmbeddingRecord, len(results))
		for index, result := range results {
			if result.TokenCount < 1 || len(result.Vector) != n.buildConfig.Model.Dimensions || !finiteVector(result.Vector) {
				return workflowState{}, fmt.Errorf("%s: %w: 第 %d 条结果", nodeEmbedChunks, ErrInvalidEmbedding, index+1)
			}
			state.embeddingRecords[index] = EmbeddingRecord{
				EmbeddingInput: state.pendingEmbedding[index],
				TokenCount:     result.TokenCount,
				Vector:         append([]float64(nil), result.Vector...),
			}
		}
	}
	state.index.GeneratedEmbeddingCount = len(state.embeddingRecords)
	state.stages = append(state.stages, StageResult{
		Name:   nodeEmbedChunks,
		Status: StageStatusCompleted,
		Detail: fmt.Sprintf("使用 %s 生成 %d 条向量，复用 %d 条已有向量", state.index.EmbeddingModel, state.index.GeneratedEmbeddingCount, state.index.ReusedEmbeddingCount),
	})
	return state, nil
}

func (n *workflowNodes) persistIndex(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodePersistIndex); err != nil {
		return workflowState{}, err
	}
	if len(state.embeddingRecords) > 0 {
		if err := n.store.SaveEmbeddings(ctx, state.build.Set.ID, state.embeddingRecords); err != nil {
			return workflowState{}, fmt.Errorf("%s: %w", nodePersistIndex, err)
		}
	}
	state.stages = append(state.stages, StageResult{
		Name:   nodePersistIndex,
		Status: StageStatusCompleted,
		Detail: fmt.Sprintf("索引包含 %d 个 Chunk 和 %d 条不可检索向量", state.index.ChunkCount, state.index.EmbeddingCount),
	})
	return state, nil
}

func (n *workflowNodes) validateIndex(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodeValidateIndex); err != nil {
		return workflowState{}, err
	}
	if err := n.store.Validate(ctx, state.build.Set.ID); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeValidateIndex, err)
	}
	state.index.ValidationPassed = true
	state.stages = append(state.stages, StageResult{
		Name:   nodeValidateIndex,
		Status: StageStatusCompleted,
		Detail: "持久化 Set、Chunk、关系、Profile 和 Embedding 完整性校验通过",
	})
	return state, nil
}

func (n *workflowNodes) publishIndex(ctx context.Context, state workflowState) (workflowState, error) {
	if err := contextError(ctx, nodePublishIndex); err != nil {
		return workflowState{}, err
	}
	if err := n.store.Publish(ctx, state.build.Set.ID); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodePublishIndex, err)
	}
	state.index.Published = true
	state.stages = append(state.stages, StageResult{
		Name:   nodePublishIndex,
		Status: StageStatusCompleted,
		Detail: fmt.Sprintf("索引版本 %s 已原子发布", state.build.Set.ID),
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
		Workflow: Descriptor().String(),
		RunID:    state.request.RunID,
		Status:   "published",
		Source:   state.source,
		Parser:   state.ingested.Parser,
		Chunking: state.chunking,
		Stages:   append([]StageResult(nil), state.stages...),
		Index:    state.index,
	}, nil
}

func (n *workflowNodes) buildSpec(state workflowState) (BuildSpec, error) {
	target := state.request.Index
	spec := BuildSpec{
		SetID: target.SetID,
		Scope: indexstore.Scope{
			TenantID:        target.TenantID,
			KnowledgeBaseID: target.KnowledgeBaseID,
		},
		Document: Document{
			ID:            target.DocumentID,
			SourceURI:     target.CanonicalURI,
			SourceName:    target.SourceName,
			Title:         target.Title,
			ContentSHA256: state.source.SHA256,
		},
		Profile: Profile{
			Name:    n.buildConfig.Chunk.ProfileName,
			Version: n.buildConfig.Chunk.ProfileVersion,
		},
		Model: n.buildConfig.Model,
	}
	switch state.chunking.StrategyName {
	case parentchild.ParentChildStrategyName:
		spec.ParentChild = &ParentChildConfig{
			ParentMaxRunes: n.buildConfig.Chunk.ParentMaxRunes,
			ChildMaxRunes:  n.buildConfig.Chunk.ChildMaxRunes,
		}
	case structureaware.StructureAwareStrategyName:
		spec.StructureAware = &StructureAwareConfig{
			MaxRunes:       n.buildConfig.Chunk.StructureMaxRunes,
			MinRunes:       n.buildConfig.Chunk.StructureMinRunes,
			HeadingContext: string(structureaware.HeadingContextPrepend),
		}
	default:
		return BuildSpec{}, fmt.Errorf("%w: 不支持 Chunk 策略 %q", ErrInvalidBuild, state.chunking.StrategyName)
	}
	return spec, nil
}

func validateIndexTarget(target IndexTarget) error {
	if strings.TrimSpace(string(target.SetID)) == "" || strings.TrimSpace(target.TenantID) == "" ||
		strings.TrimSpace(target.KnowledgeBaseID) == "" || strings.TrimSpace(target.DocumentID) == "" ||
		strings.TrimSpace(target.CanonicalURI) == "" || strings.TrimSpace(target.SourceName) == "" {
		return fmt.Errorf("%w: Index Target 必填字段不能为空", ErrInvalidRequest)
	}
	return nil
}

func finiteVector(vector []float64) bool {
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(float32(value)), 0) {
			return false
		}
	}
	return true
}

func validatePendingEmbeddingInputs(all, pending []EmbeddingInput) error {
	available := make(map[string]EmbeddingInput, len(all))
	for _, input := range all {
		available[embeddingInputKey(input)] = input
	}
	seen := make(map[string]struct{}, len(pending))
	for _, input := range pending {
		key := embeddingInputKey(input)
		expected, exists := available[key]
		if !exists || expected.Text != input.Text || expected.InputSHA256 != input.InputSHA256 {
			return fmt.Errorf("%w: Store 返回未知或已变化的 Embedding 输入", ErrInvalidEmbedding)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: Store 返回重复的 Embedding 输入", ErrInvalidEmbedding)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func embeddingInputKey(input EmbeddingInput) string {
	return string(input.Candidate.SetID) + "\x00" + input.Candidate.ChunkID + "\x00" + string(input.ModelKey)
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
