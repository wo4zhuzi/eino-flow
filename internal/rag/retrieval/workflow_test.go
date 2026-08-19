package retrieval

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	workflowruntime "github.com/wo4zhuzi/eino-flow/internal/workflow"
)

func TestWorkflowExecutesVectorRetrievalLifecycle(t *testing.T) {
	vector := validQueryVector()
	evidence := []Evidence{
		validEvidence("chunk-1", 0.1),
		validEvidence("chunk-2", 0.2),
	}
	embedder := &stubEmbedder{results: []embedding.Result{{Vector: vector, TokenCount: 7}}}
	store := &stubStore{evidence: evidence}
	workflow := newRetrievalWorkflow(t, embedder, store)
	request := Request{
		RunID: " run-1 ",
		Query: "  Markdown 使用什么策略？  ",
		Scope: indexstore.Scope{
			TenantID:        " tenant-1 ",
			KnowledgeBaseID: " kb-1 ",
		},
		TopK: 3,
	}
	original := request

	result, err := workflow.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(request, original) {
		t.Fatalf("Run() mutated request: got=%#v want=%#v", request, original)
	}
	if !reflect.DeepEqual(embedder.texts, []string{"Markdown 使用什么策略？"}) {
		t.Fatalf("Embed() texts = %#v", embedder.texts)
	}
	if store.request.Scope != (indexstore.Scope{TenantID: "tenant-1", KnowledgeBaseID: "kb-1"}) ||
		store.request.Limit != 3 || len(store.request.Vector) != 1536 {
		t.Fatalf("Search() request = %#v", store.request)
	}
	wantModelKey, err := indexstore.NewModelKey(validRetrievalConfig().Model)
	if err != nil {
		t.Fatalf("NewModelKey() error = %v", err)
	}
	if store.request.ModelKey != wantModelKey || result.ModelKey != wantModelKey {
		t.Fatalf("model keys: request=%q result=%q want=%q", store.request.ModelKey, result.ModelKey, wantModelKey)
	}
	if result.Workflow != "rag_vector_retrieval@v1" || result.RunID != "run-1" ||
		result.Status != StatusCompleted || result.QueryEmbeddingTokenCount != 7 || len(result.Evidence) != 2 {
		t.Fatalf("result = %#v", result)
	}

	vector[0] = 42
	evidence[0].SourceUnitIDs[0] = "changed"
	evidence[0].Metadata[0] = '['
	if store.request.Vector[0] == 42 || result.Evidence[0].SourceUnitIDs[0] != "unit-1" ||
		string(result.Evidence[0].Metadata) != `{"section":"strategy"}` {
		t.Fatalf("workflow retained dependency backing arrays: request=%v result=%#v", store.request.Vector[0], result.Evidence[0])
	}
}

func TestWorkflowReturnsNonNilEmptyEvidence(t *testing.T) {
	workflow := newRetrievalWorkflow(
		t,
		&stubEmbedder{results: []embedding.Result{{Vector: validQueryVector(), TokenCount: 1}}},
		&stubStore{evidence: nil},
	)

	result, err := workflow.Run(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != StatusCompleted || result.Evidence == nil || len(result.Evidence) != 0 {
		t.Fatalf("result = %#v, want completed non-nil empty evidence", result)
	}
}

func TestWorkflowRejectsInvalidConstruction(t *testing.T) {
	var typedNilEmbedder *stubEmbedder
	var typedNilStore *stubStore
	tests := []struct {
		name         string
		ctx          context.Context
		dependencies Dependencies
		want         error
	}{
		{name: "nil context", dependencies: validRetrievalDependencies(&stubEmbedder{}, &stubStore{}), want: ErrNilContext},
		{name: "nil embedder", ctx: context.Background(), dependencies: validRetrievalDependencies(nil, &stubStore{}), want: ErrInvalidDependencies},
		{name: "typed nil embedder", ctx: context.Background(), dependencies: validRetrievalDependencies(typedNilEmbedder, &stubStore{}), want: ErrInvalidDependencies},
		{name: "nil store", ctx: context.Background(), dependencies: validRetrievalDependencies(&stubEmbedder{}, nil), want: ErrInvalidDependencies},
		{name: "typed nil store", ctx: context.Background(), dependencies: validRetrievalDependencies(&stubEmbedder{}, typedNilStore), want: ErrInvalidDependencies},
		{name: "invalid dimensions", ctx: context.Background(), dependencies: Dependencies{
			Embedder: &stubEmbedder{},
			Store:    &stubStore{},
			Config: Config{Model: indexstore.ModelProfile{
				Model:         "text-embedding-v4",
				Dimensions:    1,
				Distance:      indexstore.DistanceCosine,
				ConfigVersion: "v1",
			}},
		}, want: ErrInvalidDependencies},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.ctx, test.dependencies)
			if !errors.Is(err, test.want) {
				t.Fatalf("New() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestWorkflowRejectsInvalidRunBeforeModelCall(t *testing.T) {
	embedder := &stubEmbedder{}
	workflow := newRetrievalWorkflow(t, embedder, &stubStore{})
	request := validRequest()
	request.Query = " "

	_, err := workflow.Run(context.Background(), request)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Run() error = %v, want ErrInvalidRequest", err)
	}
	if len(embedder.texts) != 0 {
		t.Fatalf("invalid request called Embed(%#v)", embedder.texts)
	}

	var unavailable *Workflow
	if _, err := unavailable.Run(context.Background(), validRequest()); !errors.Is(err, ErrWorkflowUnavailable) {
		t.Fatalf("nil Workflow.Run() error = %v, want ErrWorkflowUnavailable", err)
	}
	if _, err := workflow.Run(nil, validRequest()); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Run(nil) error = %v, want ErrNilContext", err)
	}
}

func TestWorkflowPreservesDependencyErrors(t *testing.T) {
	tests := []struct {
		name     string
		node     string
		embedder Embedder
		store    Store
	}{
		{name: "embed", node: nodeEmbedQuery, embedder: &stubEmbedder{err: errors.New("embedding unavailable")}, store: &stubStore{}},
		{name: "search", node: nodeRetrieveEvidence, embedder: &stubEmbedder{results: []embedding.Result{{Vector: validQueryVector(), TokenCount: 1}}}, store: &stubStore{err: errors.New("database unavailable")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workflow := newRetrievalWorkflow(t, test.embedder, test.store)
			_, err := workflow.Run(context.Background(), validRequest())
			if err == nil || !strings.Contains(err.Error(), test.node) || !strings.Contains(err.Error(), `run_id="run-1"`) {
				t.Fatalf("Run() error = %v", err)
			}
			var operationError *workflowruntime.OperationError
			if !errors.As(err, &operationError) || operationError.Execution.Descriptor != Descriptor() ||
				operationError.Execution.RunID != "run-1" || operationError.Operation != workflowruntime.OperationRun {
				t.Fatalf("OperationError = %#v", operationError)
			}
		})
	}
}

func TestWorkflowSupportsConcurrentReuse(t *testing.T) {
	workflow := newRetrievalWorkflow(t, statelessEmbedder{}, statelessStore{})
	const runs = 24
	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, runs)
	for index := 0; index < runs; index++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			request := validRequest()
			request.RunID = fmt.Sprintf("concurrent-%d", index)
			result, err := workflow.Run(context.Background(), request)
			if err != nil {
				errorsChannel <- err
				return
			}
			if result.RunID != request.RunID || result.Status != StatusCompleted || len(result.Evidence) != 1 {
				errorsChannel <- fmt.Errorf("result = %#v", result)
			}
		}(index)
	}
	waitGroup.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent Run() error = %v", err)
	}
}

type statelessEmbedder struct{}

func (statelessEmbedder) Embed(_ context.Context, _ []string) ([]embedding.Result, error) {
	return []embedding.Result{{Vector: validQueryVector(), TokenCount: 1}}, nil
}

type statelessStore struct{}

func (statelessStore) Search(_ context.Context, _ SearchRequest) ([]Evidence, error) {
	return []Evidence{validEvidence("chunk-1", 0.1)}, nil
}

func newRetrievalWorkflow(t *testing.T, embedder Embedder, store Store) *Workflow {
	t.Helper()
	workflow, err := New(context.Background(), validRetrievalDependencies(embedder, store))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return workflow
}

func validRetrievalDependencies(embedder Embedder, store Store) Dependencies {
	return Dependencies{
		Embedder: embedder,
		Store:    store,
		Config:   validRetrievalConfig(),
	}
}

func validRetrievalConfig() Config {
	return Config{Model: indexstore.ModelProfile{
		Model:         "text-embedding-v4",
		Dimensions:    1536,
		Distance:      indexstore.DistanceCosine,
		ConfigVersion: "v1",
	}}
}
