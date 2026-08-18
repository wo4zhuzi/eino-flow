package postgres

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pgvector/pgvector-go"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

const oldTestSetID = indexstore.SetID("22222222-2222-4222-8222-222222222222")

func TestStorePublishFirstAndReplacementAreAtomic(t *testing.T) {
	tests := []struct {
		name      string
		oldActive bool
	}{
		{name: "首次发布"},
		{name: "替换旧版本", oldActive: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, cleanup := newMockDatabase(t)
			defer cleanup()
			store, err := NewStore(db, "vdb")
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			build := validStoreBuild()

			expectPublishValidation(mock, build, publishEmbeddingRows(build))
			if test.oldActive {
				oldBuild := build
				oldBuild.Set.ID = oldTestSetID
				mock.ExpectQuery(activeSetQueryPattern()).
					WillReturnRows(storeSetRows(oldBuild, setStatusActive, true))
				mock.ExpectExec(updateEmbeddingPattern()).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec(updateSetPattern()).
					WillReturnResult(sqlmock.NewResult(0, 1))
			} else {
				mock.ExpectQuery(activeSetQueryPattern()).
					WillReturnRows(emptySetRows())
			}
			mock.ExpectExec(updateEmbeddingPattern()).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(updateSetPattern()).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			if err := store.Publish(context.Background(), build.Set.ID); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet SQL expectations: %v", err)
			}
		})
	}
}

func TestStorePublishRejectsRepeatedPublishWithoutMutation(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	build := validStoreBuild()

	mock.ExpectBegin()
	mock.ExpectQuery(setQueryPattern()).WillReturnRows(storeSetRows(build, setStatusActive, true))
	mock.ExpectExec(advisoryLockPattern()).
		WithArgs(publishLockKeyForBuild(build)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(lockedSetQueryPattern()).WillReturnRows(storeSetRows(build, setStatusActive, true))
	mock.ExpectRollback()

	err = store.Publish(context.Background(), build.Set.ID)
	if !errors.Is(err, indexing.ErrBuildConflict) {
		t.Fatalf("Publish() error = %v, want ErrBuildConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("repeated publish mutated data: %v", err)
	}
}

func TestStorePublishValidationFailureKeepsOldActive(t *testing.T) {
	tests := []struct {
		name          string
		profileRows   *sqlmock.Rows
		embeddingRows *sqlmock.Rows
	}{
		{
			name:          "Embedding 缺失",
			profileRows:   profileConfigRows(validStoreBuild().Set.Config),
			embeddingRows: emptyEmbeddingRows(),
		},
		{
			name:        "Profile 配置冲突",
			profileRows: profileConfigRows([]byte(`{"parent_max_runes":999,"child_max_runes":500,"embedding_input_policy":"v1"}`)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, cleanup := newMockDatabase(t)
			defer cleanup()
			store, err := NewStore(db, "vdb")
			if err != nil {
				t.Fatalf("NewStore() error = %v", err)
			}
			build := validStoreBuild()

			mock.ExpectBegin()
			mock.ExpectQuery(setQueryPattern()).WillReturnRows(storeSetRows(build, setStatusBuilding, false))
			mock.ExpectExec(advisoryLockPattern()).
				WithArgs(publishLockKeyForBuild(build)).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(lockedSetQueryPattern()).WillReturnRows(storeSetRows(build, setStatusBuilding, false))
			mock.ExpectQuery(profileConfigQueryPattern()).WillReturnRows(test.profileRows)
			if test.embeddingRows != nil {
				mock.ExpectQuery(chunkQueryPattern()).WillReturnRows(publishChunkRows(build))
				mock.ExpectQuery(embeddingQueryPattern()).WillReturnRows(test.embeddingRows)
			}
			mock.ExpectRollback()

			err = store.Publish(context.Background(), build.Set.ID)
			if !errors.Is(err, indexing.ErrInvalidBuild) {
				t.Fatalf("Publish() error = %v, want ErrInvalidBuild", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("validation failure reached publish updates: %v", err)
			}
		})
	}
}

func TestStorePublishRollsBackDatabaseFailure(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	build := validStoreBuild()
	cause := errors.New("publish update failed")

	expectPublishValidation(mock, build, publishEmbeddingRows(build))
	mock.ExpectQuery(activeSetQueryPattern()).WillReturnRows(emptySetRows())
	mock.ExpectExec(updateEmbeddingPattern()).WillReturnError(cause)
	mock.ExpectRollback()

	err = store.Publish(context.Background(), build.Set.ID)
	if !errors.Is(err, indexing.ErrStoreUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("Publish() error = %v, want classified database cause", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStorePublishBoundaries(t *testing.T) {
	var store *Store
	if err := store.Publish(context.Background(), testSetID); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil Store Publish() error = %v", err)
	}
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	store, err := NewStore(db, "vdb")
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := store.Publish(nil, testSetID); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Publish(nil) error = %v", err)
	}
	if err := store.Publish(context.Background(), "not-a-uuid"); !errors.Is(err, indexing.ErrInvalidBuild) {
		t.Fatalf("Publish(invalid ID) error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("boundary validation opened transaction: %v", err)
	}
}

func TestPublishAdvisoryLockKeyIsStableAndUnambiguous(t *testing.T) {
	base := indexstore.Scope{TenantID: "tenant", KnowledgeBaseID: "kb"}
	first := publishAdvisoryLockKey(base, "document", "parent_child")
	if first != publishAdvisoryLockKey(base, "document", "parent_child") {
		t.Fatal("equal publish scopes produced different lock keys")
	}
	tests := []struct {
		scope    indexstore.Scope
		document string
		strategy string
	}{
		{scope: indexstore.Scope{TenantID: "tenant-2", KnowledgeBaseID: "kb"}, document: "document", strategy: "parent_child"},
		{scope: indexstore.Scope{TenantID: "tenant", KnowledgeBaseID: "kb-2"}, document: "document", strategy: "parent_child"},
		{scope: base, document: "document-2", strategy: "parent_child"},
		{scope: base, document: "document", strategy: "structure_aware"},
		{scope: indexstore.Scope{TenantID: "tenantk", KnowledgeBaseID: "b"}, document: "document", strategy: "parent_child"},
	}
	for _, test := range tests {
		if got := publishAdvisoryLockKey(test.scope, test.document, test.strategy); got == first {
			t.Fatalf("different publish scope produced same lock key: %#v", test)
		}
	}
}

func expectPublishValidation(mock sqlmock.Sqlmock, build indexing.BuildData, embeddings *sqlmock.Rows) {
	mock.ExpectBegin()
	mock.ExpectQuery(setQueryPattern()).WillReturnRows(storeSetRows(build, setStatusBuilding, false))
	mock.ExpectExec(advisoryLockPattern()).
		WithArgs(publishLockKeyForBuild(build)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(lockedSetQueryPattern()).WillReturnRows(storeSetRows(build, setStatusBuilding, false))
	mock.ExpectQuery(profileConfigQueryPattern()).WillReturnRows(profileConfigRows(build.Set.Config))
	mock.ExpectQuery(chunkQueryPattern()).WillReturnRows(publishChunkRows(build))
	mock.ExpectQuery(embeddingQueryPattern()).WillReturnRows(embeddings)
}

func publishLockKeyForBuild(build indexing.BuildData) int64 {
	return publishAdvisoryLockKey(build.Set.Scope, build.Set.Document.ID, build.Set.StrategyName)
}

func setQueryPattern() string {
	return `SELECT \* FROM "vdb"\."chunk_sets".*WHERE id = .*LIMIT`
}

func lockedSetQueryPattern() string {
	return `SELECT \* FROM "vdb"\."chunk_sets".*WHERE id = .*LIMIT.*FOR UPDATE`
}

func advisoryLockPattern() string {
	return regexp.QuoteMeta("SELECT pg_advisory_xact_lock($1)")
}

func profileConfigQueryPattern() string {
	return `SELECT "config" FROM "vdb"\."chunk_sets".*strategy_name`
}

func chunkQueryPattern() string {
	return `SELECT chunk_set_id, chunk_id, kind, level, parent_chunk_id, previous_chunk_id,.*to_json\(source_unit_ids\) AS source_unit_ids_json, metadata FROM "vdb"\."chunks".*ORDER BY sequence ASC`
}

func embeddingQueryPattern() string {
	return `SELECT \* FROM "vdb"\."chunk_embeddings".*ORDER BY chunk_id ASC, model_key ASC`
}

func activeSetQueryPattern() string {
	return `SELECT \* FROM "vdb"\."chunk_sets".*status.*FOR UPDATE`
}

func updateEmbeddingPattern() string {
	return `UPDATE "vdb"\."chunk_embeddings" SET "searchable"=.*WHERE chunk_set_id`
}

func updateSetPattern() string {
	return `UPDATE "vdb"\."chunk_sets" SET .*WHERE id`
}

func profileConfigRows(config []byte) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"config"}).AddRow(config)
}

func emptySetRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "tenant_id", "knowledge_base_id", "document_id", "source_uri", "source_name",
		"content_sha256", "strategy_name", "profile_name", "profile_version", "config", "status", "activated_at",
	})
}

func publishChunkRows(build indexing.BuildData) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"chunk_set_id", "chunk_id", "kind", "level", "parent_chunk_id", "previous_chunk_id",
		"next_chunk_id", "sequence", "content", "character_count", "token_count", "source_unit_ids_json", "metadata",
	})
	for _, chunk := range build.Chunks {
		rows.AddRow(
			string(build.Set.ID),
			chunk.Candidate.ChunkID,
			chunk.Kind,
			chunk.Level,
			chunk.ParentChunkID,
			chunk.PreviousChunkID,
			chunk.NextChunkID,
			chunk.Sequence,
			chunk.Content,
			chunk.CharacterCount,
			chunk.TokenCount,
			[]byte(`["`+chunk.SourceUnitIDs[0]+`"]`),
			[]byte(chunk.Metadata),
		)
	}
	return rows
}

func publishEmbeddingRows(build indexing.BuildData) *sqlmock.Rows {
	rows := emptyEmbeddingRows()
	vector := make([]float32, appconfig.RequiredEmbeddingDimensions)
	for index := range vector {
		vector[index] = float32(index+1) / float32(appconfig.RequiredEmbeddingDimensions)
	}
	for _, input := range build.EmbeddingInputs {
		rows.AddRow(
			string(build.Set.ID),
			input.Candidate.ChunkID,
			string(input.ModelKey),
			input.Text,
			8,
			input.InputSHA256,
			pgvector.NewVector(vector).String(),
			false,
		)
	}
	return rows
}

func emptyEmbeddingRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"chunk_set_id", "chunk_id", "model_key", "embedding_text", "embedding_token_count",
		"input_sha256", "embedding", "searchable",
	})
}

func TestPublishSQLPatternsAreValid(t *testing.T) {
	for _, pattern := range []string{
		setQueryPattern(),
		lockedSetQueryPattern(),
		advisoryLockPattern(),
		profileConfigQueryPattern(),
		chunkQueryPattern(),
		embeddingQueryPattern(),
		activeSetQueryPattern(),
		updateEmbeddingPattern(),
		updateSetPattern(),
	} {
		if _, err := regexp.Compile(pattern); err != nil {
			t.Fatalf("invalid SQL expectation %q: %v", pattern, err)
		}
	}
}
