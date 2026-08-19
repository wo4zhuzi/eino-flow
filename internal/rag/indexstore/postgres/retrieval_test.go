package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
)

func TestStoreSearchFiltersBeforeTopKAndMapsEvidence(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	request := validSearchRequest()

	mock.ExpectQuery(searchQueryPattern()).
		WithArgs(sqlmock.AnyArg(), request.Scope.TenantID, request.Scope.KnowledgeBaseID, string(request.ModelKey), request.Limit).
		WillReturnRows(searchRows().AddRow(
			string(testSetID),
			"structure-1",
			"document-1",
			"knowledge://local/document-1",
			"knowledge.md",
			"structure",
			1,
			"结构化 Markdown 使用 Structure-aware Chunk。",
			8,
			"{unit-1}",
			[]byte(`{"section":"strategy"}`),
			0.125,
		))

	evidence, err := store.Search(context.Background(), request)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(evidence) != 1 || evidence[0].Candidate.SetID != testSetID ||
		evidence[0].Candidate.ChunkID != "structure-1" || evidence[0].DocumentID != "document-1" ||
		len(evidence[0].SourceUnitIDs) != 1 || evidence[0].SourceUnitIDs[0] != "unit-1" ||
		evidence[0].CosineDistance != 0.125 {
		t.Fatalf("Search() evidence = %#v", evidence)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreSearchReturnsNonNilEmptyEvidence(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	request := validSearchRequest()
	mock.ExpectQuery(searchQueryPattern()).
		WithArgs(sqlmock.AnyArg(), request.Scope.TenantID, request.Scope.KnowledgeBaseID, string(request.ModelKey), request.Limit).
		WillReturnRows(searchRows())

	evidence, err := store.Search(context.Background(), request)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if evidence == nil || len(evidence) != 0 {
		t.Fatalf("Search() evidence = %#v, want non-nil empty slice", evidence)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreSearchRejectsInvalidRequestBeforeQuery(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	request := validSearchRequest()
	request.Scope.TenantID = ""
	if _, err := store.Search(context.Background(), request); !errors.Is(err, retrieval.ErrInvalidRequest) {
		t.Fatalf("Search() error = %v, want ErrInvalidRequest", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("invalid request queried database: %v", err)
	}
}

func TestStoreSearchClassifiesDatabaseFailureWithoutLeakingCause(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	request := validSearchRequest()
	cause := errors.New("database query included sensitive detail")
	mock.ExpectQuery(searchQueryPattern()).
		WithArgs(sqlmock.AnyArg(), request.Scope.TenantID, request.Scope.KnowledgeBaseID, string(request.ModelKey), request.Limit).
		WillReturnError(cause)

	_, err = store.Search(context.Background(), request)
	if !errors.Is(err, retrieval.ErrStoreUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("Search() error = %v, want classified cause", err)
	}
	formatted := fmt.Sprintf("%v | %+v | %#v", err, err, err)
	if strings.Contains(formatted, cause.Error()) {
		t.Fatalf("Search() leaked database detail: %q", formatted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func validSearchRequest() retrieval.SearchRequest {
	vector := make([]float64, 1536)
	vector[0] = 0.25
	return retrieval.SearchRequest{
		Scope:    indexstore.Scope{TenantID: "tenant-1", KnowledgeBaseID: "kb-1"},
		ModelKey: testModelKey,
		Vector:   vector,
		Limit:    3,
	}
}

func searchQueryPattern() string {
	return `(?s)` + regexp.QuoteMeta(`SELECT ce.chunk_set_id, ce.chunk_id,`) +
		`.*ce\.embedding <=> \$1 AS cosine_distance` +
		`.*FROM "vdb"\."chunk_embeddings" AS ce` +
		`.*JOIN "vdb"\."chunk_sets" AS cs ON cs\.id = ce\.chunk_set_id` +
		`.*JOIN "vdb"\."chunks" AS c` +
		`.*WHERE cs\.tenant_id = \$2` +
		`.*AND cs\.knowledge_base_id = \$3` +
		`.*AND cs\.status = 'active'` +
		`.*AND ce\.searchable = true` +
		`.*AND ce\.model_key = \$4` +
		`.*ORDER BY cosine_distance ASC, ce\.chunk_set_id ASC, ce\.chunk_id ASC` +
		`.*LIMIT \$5`
}

func searchRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"chunk_set_id",
		"chunk_id",
		"document_id",
		"source_uri",
		"source_name",
		"kind",
		"sequence",
		"content",
		"token_count",
		"source_unit_ids",
		"metadata",
		"cosine_distance",
	})
}
