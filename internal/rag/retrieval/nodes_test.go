package retrieval

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

type stubEmbedder struct {
	results []embedding.Result
	err     error
	texts   []string
}

func (s *stubEmbedder) Embed(_ context.Context, texts []string) ([]embedding.Result, error) {
	s.texts = append([]string(nil), texts...)
	return s.results, s.err
}

type stubStore struct {
	evidence []Evidence
	err      error
	request  SearchRequest
}

func (s *stubStore) Search(_ context.Context, request SearchRequest) ([]Evidence, error) {
	s.request = request
	if len(s.request.Vector) > 0 {
		s.request.Vector[0] = 99
	}
	return s.evidence, s.err
}

func TestWorkflowNodesEmbedQueryNormalizesAndClonesVector(t *testing.T) {
	vector := validQueryVector()
	embedder := &stubEmbedder{results: []embedding.Result{{Vector: vector, TokenCount: 7}}}
	nodes := validWorkflowNodes(embedder, &stubStore{})

	state, err := nodes.embedQuery(context.Background(), Request{
		RunID: " run-1 ",
		Query: "  Markdown 使用什么策略？  ",
		Scope: indexstore.Scope{
			TenantID:        " tenant-1 ",
			KnowledgeBaseID: " kb-1 ",
		},
		TopK: 3,
	})
	if err != nil {
		t.Fatalf("embedQuery() error = %v", err)
	}
	if len(embedder.texts) != 1 || embedder.texts[0] != "Markdown 使用什么策略？" {
		t.Fatalf("Embed() texts = %#v", embedder.texts)
	}
	if state.request.RunID != "run-1" || state.request.Scope.TenantID != "tenant-1" ||
		state.request.Scope.KnowledgeBaseID != "kb-1" || state.queryEmbeddingTokenCount != 7 {
		t.Fatalf("embedQuery() state = %#v", state)
	}
	vector[0] = 42
	if state.queryVector[0] == 42 {
		t.Fatal("embedQuery() retained Embedder vector backing array")
	}
}

func TestWorkflowNodesEmbedQueryRejectsInvalidRequestBeforeModelCall(t *testing.T) {
	tests := []Request{
		{},
		{RunID: "run", Query: "query", Scope: indexstore.Scope{TenantID: "tenant", KnowledgeBaseID: "kb"}},
		{RunID: "run", Query: strings.Repeat("界", MaxQueryRunes+1), Scope: indexstore.Scope{TenantID: "tenant", KnowledgeBaseID: "kb"}, TopK: 1},
		{RunID: "run", Query: "query", Scope: indexstore.Scope{TenantID: "tenant", KnowledgeBaseID: "kb"}, TopK: MaxTopK + 1},
	}
	for _, request := range tests {
		embedder := &stubEmbedder{}
		nodes := validWorkflowNodes(embedder, &stubStore{})
		if _, err := nodes.embedQuery(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("embedQuery(%#v) error = %v, want ErrInvalidRequest", request, err)
		}
		if len(embedder.texts) != 0 {
			t.Fatalf("invalid request called Embed(%#v)", embedder.texts)
		}
	}
}

func TestWorkflowNodesEmbedQueryRejectsInvalidModelResult(t *testing.T) {
	tests := []embedding.Result{
		{Vector: validQueryVector()},
		{Vector: make([]float64, 1535), TokenCount: 1},
		{Vector: append([]float64{math.NaN()}, validQueryVector()[1:]...), TokenCount: 1},
	}
	for _, result := range tests {
		nodes := validWorkflowNodes(&stubEmbedder{results: []embedding.Result{result}}, &stubStore{})
		if _, err := nodes.embedQuery(context.Background(), validRequest()); !errors.Is(err, ErrInvalidEmbedding) {
			t.Fatalf("embedQuery(%#v) error = %v, want ErrInvalidEmbedding", result, err)
		}
	}
}

func TestWorkflowNodesRetrieveEvidenceValidatesAndClonesResult(t *testing.T) {
	items := []Evidence{validEvidence("chunk-1", 0.1), validEvidence("chunk-2", 0.2)}
	store := &stubStore{evidence: items}
	nodes := validWorkflowNodes(&stubEmbedder{}, store)
	state := validWorkflowState()

	got, err := nodes.retrieveEvidence(context.Background(), state)
	if err != nil {
		t.Fatalf("retrieveEvidence() error = %v", err)
	}
	if store.request.Scope != state.request.Scope || store.request.ModelKey != state.modelKey ||
		store.request.Limit != state.request.TopK || state.queryVector[0] == 99 {
		t.Fatalf("Search() request = %#v", store.request)
	}
	items[0].SourceUnitIDs[0] = "changed"
	items[0].Metadata[0] = '['
	if got.evidence[0].SourceUnitIDs[0] != "unit-1" || string(got.evidence[0].Metadata) != `{"section":"strategy"}` {
		t.Fatalf("retrieveEvidence() retained Store backing arrays: %#v", got.evidence[0])
	}
}

func TestWorkflowNodesRetrieveEvidenceRejectsInvalidStoreResult(t *testing.T) {
	items := []Evidence{validEvidence("chunk-2", 0.2), validEvidence("chunk-1", 0.1)}
	nodes := validWorkflowNodes(&stubEmbedder{}, &stubStore{evidence: items})
	if _, err := nodes.retrieveEvidence(context.Background(), validWorkflowState()); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("retrieveEvidence() error = %v, want ErrInvalidEvidence", err)
	}
}

func TestWorkflowNodesRetrieveEvidencePreservesStoreError(t *testing.T) {
	cause := errors.New("database unavailable")
	nodes := validWorkflowNodes(&stubEmbedder{}, &stubStore{err: cause})
	if _, err := nodes.retrieveEvidence(context.Background(), validWorkflowState()); !errors.Is(err, cause) {
		t.Fatalf("retrieveEvidence() error = %v, want cause", err)
	}
}

func validWorkflowNodes(embedder Embedder, store Store) *workflowNodes {
	profile := indexstore.ModelProfile{
		Model:         "text-embedding-v4",
		Dimensions:    1536,
		Distance:      indexstore.DistanceCosine,
		ConfigVersion: "v1",
	}
	modelKey, err := indexstore.NewModelKey(profile)
	if err != nil {
		panic(err)
	}
	return &workflowNodes{embedder: embedder, store: store, model: profile, modelKey: modelKey}
}

func validRequest() Request {
	return Request{
		RunID: "run-1",
		Query: "Markdown 使用什么策略？",
		Scope: indexstore.Scope{TenantID: "tenant-1", KnowledgeBaseID: "kb-1"},
		TopK:  3,
	}
}

func validWorkflowState() workflowState {
	nodes := validWorkflowNodes(&stubEmbedder{}, &stubStore{})
	return workflowState{
		request:     validRequest(),
		modelKey:    nodes.modelKey,
		queryVector: validQueryVector(),
	}
}

func validQueryVector() []float64 {
	vector := make([]float64, 1536)
	vector[0] = 0.25
	return vector
}

func validEvidence(chunkID string, distance float64) Evidence {
	return Evidence{
		Candidate:      indexstore.CandidateID{SetID: "11111111-1111-4111-8111-111111111111", ChunkID: chunkID},
		DocumentID:     "document-1",
		SourceURI:      "knowledge://local/document-1",
		SourceName:     "knowledge.md",
		Kind:           "structure",
		Sequence:       1,
		Content:        "结构化 Markdown 使用 Structure-aware Chunk。",
		TokenCount:     8,
		SourceUnitIDs:  []string{"unit-1"},
		Metadata:       []byte(`{"section":"strategy"}`),
		CosineDistance: distance,
	}
}
