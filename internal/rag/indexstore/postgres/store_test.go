package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

const (
	testSetID    = indexstore.SetID("11111111-1111-4111-8111-111111111111")
	testModelKey = indexstore.ModelKey("text-embedding-v4:test-model-key")
)

func TestStorePrepareBuildFirstWriteAndHashRetry(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	build := validStoreBuild()

	expectPrepareBuild(mock, build, sqlmock.NewRows([]string{"chunk_id", "model_key", "input_sha256"}))
	missing, err := store.PrepareBuild(context.Background(), build)
	if err != nil {
		t.Fatalf("PrepareBuild(first) error = %v", err)
	}
	if len(missing) != 1 || missing[0].Candidate != build.EmbeddingInputs[0].Candidate {
		t.Fatalf("PrepareBuild(first) missing = %#v", missing)
	}

	reusedRows := sqlmock.NewRows([]string{"chunk_id", "model_key", "input_sha256"}).AddRow(
		build.EmbeddingInputs[0].Candidate.ChunkID,
		string(build.EmbeddingInputs[0].ModelKey),
		build.EmbeddingInputs[0].InputSHA256,
	)
	expectPrepareBuild(mock, build, reusedRows)
	missing, err = store.PrepareBuild(context.Background(), build)
	if err != nil {
		t.Fatalf("PrepareBuild(retry) error = %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("PrepareBuild(retry) missing = %#v, want empty", missing)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStorePrepareBuildRejectsActiveSetWithoutMutation(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	build := validStoreBuild()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO "vdb"\."chunk_sets"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT \* FROM "vdb"\."chunk_sets".*FOR UPDATE`).
		WillReturnRows(storeSetRows(build, "active", true))
	mock.ExpectRollback()

	_, err = store.PrepareBuild(context.Background(), build)
	if !errors.Is(err, indexing.ErrBuildConflict) {
		t.Fatalf("PrepareBuild() error = %v, want ErrBuildConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("active Set was mutated or SQL expectations unmet: %v", err)
	}
}

func TestStorePrepareBuildRollsBackOnChunkFailure(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	build := validStoreBuild()
	cause := errors.New("chunk write failed")

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO "vdb"\."chunk_sets"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT \* FROM "vdb"\."chunk_sets".*FOR UPDATE`).
		WillReturnRows(storeSetRows(build, "building", false))
	mock.ExpectExec(`UPDATE "vdb"\."chunk_embeddings"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM "vdb"\."chunks"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO "vdb"\."chunks"`).WillReturnError(cause)
	mock.ExpectRollback()

	_, err = store.PrepareBuild(context.Background(), build)
	if !errors.Is(err, indexing.ErrStoreUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("PrepareBuild() error = %v, want classified cause", err)
	}
	if err.Error() != indexing.ErrStoreUnavailable.Error() {
		t.Fatalf("PrepareBuild() exposed database detail: %q", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreSaveEmbeddingsCommitsAndForcesUnsearchable(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	record := validEmbeddingRecord()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "status","activated_at" FROM "vdb"\."chunk_sets".*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"status", "activated_at"}).AddRow("building", nil))
	mock.ExpectExec(`INSERT INTO "vdb"\."chunk_embeddings".*"searchable".*ON CONFLICT.*DO UPDATE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := store.SaveEmbeddings(context.Background(), testSetID, []indexing.EmbeddingRecord{record}); err != nil {
		t.Fatalf("SaveEmbeddings() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreSaveEmbeddingsRollsBackPartialBatch(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	record := validEmbeddingRecord()
	second := record
	second.Candidate.ChunkID = "child-2"
	second.Text = "第二段"
	second.InputSHA256 = storeHash(second.Text)
	cause := errors.New("embedding batch failed")

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "status","activated_at" FROM "vdb"\."chunk_sets".*FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"status", "activated_at"}).AddRow("building", nil))
	mock.ExpectExec(`INSERT INTO "vdb"\."chunk_embeddings"`).WillReturnError(cause)
	mock.ExpectRollback()

	err = store.SaveEmbeddings(context.Background(), testSetID, []indexing.EmbeddingRecord{record, second})
	if !errors.Is(err, indexing.ErrStoreUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("SaveEmbeddings() error = %v, want classified cause", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreRejectsInvalidEmbeddingBeforeTransaction(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	record := validEmbeddingRecord()
	record.Vector = record.Vector[:appconfig.RequiredEmbeddingDimensions-1]

	err = store.SaveEmbeddings(context.Background(), testSetID, []indexing.EmbeddingRecord{record})
	if !errors.Is(err, indexing.ErrInvalidBuild) {
		t.Fatalf("SaveEmbeddings() error = %v, want ErrInvalidBuild", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("invalid record opened a transaction: %v", err)
	}
}

func expectPrepareBuild(mock sqlmock.Sqlmock, build indexing.BuildData, hashRows *sqlmock.Rows) {
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO "vdb"\."chunk_sets"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT \* FROM "vdb"\."chunk_sets".*FOR UPDATE`).
		WillReturnRows(storeSetRows(build, "building", false))
	mock.ExpectExec(`UPDATE "vdb"\."chunk_embeddings"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM "vdb"\."chunks"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO "vdb"\."chunks"`).WillReturnResult(sqlmock.NewResult(0, int64(len(build.Chunks))))
	mock.ExpectExec(`DELETE FROM "vdb"\."chunk_embeddings"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT "chunk_id","model_key","input_sha256" FROM "vdb"\."chunk_embeddings"`).
		WillReturnRows(hashRows)
	mock.ExpectCommit()
}

func storeSetRows(build indexing.BuildData, status string, activated bool) *sqlmock.Rows {
	var activatedAt any
	if activated {
		activatedAt = time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	}
	return sqlmock.NewRows([]string{
		"id",
		"tenant_id",
		"knowledge_base_id",
		"document_id",
		"source_uri",
		"source_name",
		"content_sha256",
		"strategy_name",
		"profile_name",
		"profile_version",
		"config",
		"status",
		"activated_at",
	}).AddRow(
		string(build.Set.ID),
		build.Set.Scope.TenantID,
		build.Set.Scope.KnowledgeBaseID,
		build.Set.Document.ID,
		build.Set.Document.SourceURI,
		build.Set.Document.SourceName,
		build.Set.Document.ContentSHA256,
		build.Set.StrategyName,
		build.Set.Profile.Name,
		build.Set.Profile.Version,
		[]byte(build.Set.Config),
		status,
		activatedAt,
	)
}

func validStoreBuild() indexing.BuildData {
	parentID := "parent-1"
	text := "安全标题\n\n子块正文"
	return indexing.BuildData{
		Set: indexing.SetRecord{
			ID: testSetID,
			Scope: indexstore.Scope{
				TenantID:        "tenant-1",
				KnowledgeBaseID: "kb-1",
			},
			Document: indexing.Document{
				ID:            "document-1",
				SourceURI:     "knowledge://documents/document-1",
				SourceName:    "安全文档.md",
				Title:         "安全标题",
				ContentSHA256: storeHash("document"),
			},
			StrategyName: "parent_child",
			Profile:      indexing.Profile{Name: "default", Version: "v1"},
			Config:       []byte(`{"parent_max_runes":2000,"child_max_runes":500,"embedding_input_policy":"v1"}`),
		},
		Chunks: []indexing.ChunkRecord{
			{
				Candidate:      indexstore.CandidateID{SetID: testSetID, ChunkID: parentID},
				Kind:           "parent",
				Level:          0,
				Sequence:       1,
				Content:        "父块正文",
				CharacterCount: 4,
				TokenCount:     2,
				SourceUnitIDs:  []string{"unit-1"},
				Metadata:       []byte(`{}`),
			},
			{
				Candidate:      indexstore.CandidateID{SetID: testSetID, ChunkID: "child-1"},
				Kind:           "child",
				Level:          1,
				ParentChunkID:  &parentID,
				Sequence:       2,
				Content:        "子块正文",
				CharacterCount: 4,
				TokenCount:     2,
				SourceUnitIDs:  []string{"unit-1"},
				Metadata:       []byte(`{}`),
			},
		},
		EmbeddingInputs: []indexing.EmbeddingInput{{
			Candidate:   indexstore.CandidateID{SetID: testSetID, ChunkID: "child-1"},
			ModelKey:    testModelKey,
			Text:        text,
			InputSHA256: storeHash(text),
		}},
	}
}

func validEmbeddingRecord() indexing.EmbeddingRecord {
	input := validStoreBuild().EmbeddingInputs[0]
	vector := make([]float64, appconfig.RequiredEmbeddingDimensions)
	for index := range vector {
		vector[index] = float64(index+1) / float64(appconfig.RequiredEmbeddingDimensions)
	}
	return indexing.EmbeddingRecord{
		EmbeddingInput: input,
		TokenCount:     8,
		Vector:         vector,
	}
}

func storeHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest)
}

func TestStoreSQLPatternsAreValid(t *testing.T) {
	for _, pattern := range []string{
		`INSERT INTO "vdb"\."chunk_sets"`,
		`SELECT \* FROM "vdb"\."chunk_sets".*FOR UPDATE`,
	} {
		if _, err := regexp.Compile(pattern); err != nil {
			t.Fatalf("invalid SQL expectation %q: %v", pattern, err)
		}
	}
}

func TestIntegrationUUIDMatchesPostgreSQLIDContract(t *testing.T) {
	id := integrationUUID(t)
	if !uuidPattern.MatchString(string(id)) {
		t.Fatalf("integrationUUID() = %q, want canonical UUID", id)
	}
}
