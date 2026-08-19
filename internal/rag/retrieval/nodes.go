package retrieval

import (
	"context"
	"fmt"
)

const (
	nodeEmbedQuery       = "embed_query"
	nodeRetrieveEvidence = "retrieve_evidence"
)

func (n *workflowNodes) embedQuery(ctx context.Context, request Request) (workflowState, error) {
	if ctx == nil {
		return workflowState{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return workflowState{}, err
	}
	request = normalizeRequest(request)
	if err := validateRequest(request); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeEmbedQuery, err)
	}
	results, err := n.embedder.Embed(ctx, []string{request.Query})
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeEmbedQuery, err)
	}
	result, err := validateEmbedding(results, n.model.Dimensions)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeEmbedQuery, err)
	}
	return workflowState{
		request:                  request,
		modelKey:                 n.modelKey,
		queryVector:              result.Vector,
		queryEmbeddingTokenCount: result.TokenCount,
	}, nil
}

func (n *workflowNodes) retrieveEvidence(ctx context.Context, state workflowState) (workflowState, error) {
	if ctx == nil {
		return workflowState{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return workflowState{}, err
	}
	request := SearchRequest{
		Scope:    state.request.Scope,
		ModelKey: state.modelKey,
		Vector:   append([]float64(nil), state.queryVector...),
		Limit:    state.request.TopK,
	}
	if err := ValidateSearchRequest(request); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeRetrieveEvidence, err)
	}
	evidence, err := n.store.Search(ctx, request)
	if err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeRetrieveEvidence, err)
	}
	if err := ValidateEvidence(evidence, request.Limit); err != nil {
		return workflowState{}, fmt.Errorf("%s: %w", nodeRetrieveEvidence, err)
	}
	state.evidence = cloneEvidence(evidence)
	return state, nil
}
