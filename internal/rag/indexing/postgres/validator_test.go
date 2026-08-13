package postgres

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	postgresdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	extensionQuery = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)"
	schemaQuery    = "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)"
)

func TestValidatorAcceptsExpectedSchema(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	expectMetadataQueries(mock, true, true, expectedColumnRows())

	validator, err := NewValidator(db, "vdb")
	if err != nil {
		t.Fatalf("NewValidator() error = %v", err)
	}
	if err := validator.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestValidatorRejectsMissingExtensionAndSchema(t *testing.T) {
	tests := []struct {
		name            string
		extensionExists bool
		schemaExists    bool
	}{
		{name: "missing vector extension", extensionExists: false},
		{name: "missing schema", extensionExists: true, schemaExists: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, cleanup := newMockDatabase(t)
			defer cleanup()
			mock.ExpectQuery(regexp.QuoteMeta(extensionQuery)).
				WithArgs("vector").
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(test.extensionExists))
			if test.extensionExists {
				mock.ExpectQuery(regexp.QuoteMeta(schemaQuery)).
					WithArgs("vdb").
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(test.schemaExists))
			}

			validator, err := NewValidator(db, "vdb")
			if err != nil {
				t.Fatalf("NewValidator() error = %v", err)
			}
			if err := validator.Validate(context.Background()); !errors.Is(err, ErrSchemaMismatch) {
				t.Fatalf("Validate() error = %v, want ErrSchemaMismatch", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet SQL expectations: %v", err)
			}
		})
	}
}

func TestValidatorRejectsSchemaDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]databaseColumn) []databaseColumn
		want   string
	}{
		{
			name: "missing table",
			mutate: func(columns []databaseColumn) []databaseColumn {
				return filterColumns(columns, func(column databaseColumn) bool {
					return column.TableName != "chunks"
				})
			},
			want: "缺少表 chunks",
		},
		{
			name: "missing column",
			mutate: func(columns []databaseColumn) []databaseColumn {
				return filterColumns(columns, func(column databaseColumn) bool {
					return column.TableName != "chunk_sets" || column.ColumnName != "source_uri"
				})
			},
			want: "缺少列 chunk_sets.source_uri",
		},
		{
			name: "wrong vector dimensions",
			mutate: func(columns []databaseColumn) []databaseColumn {
				for index := range columns {
					if columns[index].TableName == "chunk_embeddings" && columns[index].ColumnName == "embedding" {
						columns[index].DataType = "vector(1024)"
					}
				}
				return columns
			},
			want: "期望 vector(1536)",
		},
		{
			name: "wrong nullability",
			mutate: func(columns []databaseColumn) []databaseColumn {
				for index := range columns {
					if columns[index].TableName == "chunks" && columns[index].ColumnName == "content" {
						columns[index].NotNull = false
					}
				}
				return columns
			},
			want: "列 chunks.content 可空性不匹配",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, cleanup := newMockDatabase(t)
			defer cleanup()
			expectMetadataQueries(mock, true, true, rowsFromColumns(test.mutate(expectedColumns())))

			validator, err := NewValidator(db, "vdb")
			if err != nil {
				t.Fatalf("NewValidator() error = %v", err)
			}
			err = validator.Validate(context.Background())
			if !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet SQL expectations: %v", err)
			}
		})
	}
}

func TestValidatorPreservesMetadataQueryError(t *testing.T) {
	db, mock, cleanup := newMockDatabase(t)
	defer cleanup()
	cause := errors.New("catalog unavailable")
	mock.ExpectQuery(regexp.QuoteMeta(extensionQuery)).
		WithArgs("vector").
		WillReturnError(cause)

	validator, err := NewValidator(db, "vdb")
	if err != nil {
		t.Fatalf("NewValidator() error = %v", err)
	}
	err = validator.Validate(context.Background())
	if !errors.Is(err, ErrSchemaQuery) || !errors.Is(err, cause) {
		t.Fatalf("Validate() error = %v, want ErrSchemaQuery and cause", err)
	}
}

func TestValidatorBoundariesAndExplicitTables(t *testing.T) {
	if _, err := NewValidator(nil, "vdb"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewValidator(nil) error = %v", err)
	}
	if _, err := newTables("vdb; drop schema vdb"); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("newTables(unsafe) error = %v", err)
	}
	tables, err := newTables("vdb")
	if err != nil {
		t.Fatalf("newTables() error = %v", err)
	}
	if tables.ChunkSets != `"vdb"."chunk_sets"` ||
		tables.Chunks != `"vdb"."chunks"` ||
		tables.ChunkEmbeddings != `"vdb"."chunk_embeddings"` {
		t.Fatalf("newTables() = %#v", tables)
	}
	var validator *Validator
	if err := validator.Validate(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil Validate() error = %v", err)
	}
}

func TestModelsUseExplicitColumnsAndVectorType(t *testing.T) {
	tests := []struct {
		name       string
		model      any
		table      string
		primaryKey []string
	}{
		{name: "chunk set", model: &chunkSet{}, table: chunkSetsTable, primaryKey: []string{"id"}},
		{name: "chunk", model: &chunk{}, table: chunksTable, primaryKey: []string{"chunk_set_id", "chunk_id"}},
		{name: "embedding", model: &chunkEmbedding{}, table: chunkEmbeddingsTable, primaryKey: []string{"chunk_set_id", "chunk_id", "model_key"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := schema.Parse(test.model, &sync.Map{}, schema.NamingStrategy{})
			if err != nil {
				t.Fatalf("schema.Parse() error = %v", err)
			}
			if parsed.Table != test.table {
				t.Fatalf("table = %q, want %q", parsed.Table, test.table)
			}
			keys := make([]string, 0, len(parsed.PrimaryFields))
			for _, field := range parsed.PrimaryFields {
				keys = append(keys, field.DBName)
			}
			if !slices.Equal(keys, test.primaryKey) {
				t.Fatalf("primary keys = %v, want %v", keys, test.primaryKey)
			}
			for _, field := range parsed.Fields {
				if field.DBName == "" {
					t.Fatalf("field %s has no explicit database column", field.Name)
				}
			}
			if test.name == "chunk set" {
				for _, column := range []string{"source_uri", "source_name", "content_sha256"} {
					if parsed.LookUpField(column) == nil {
						t.Fatalf("chunk set missing column %q", column)
					}
				}
			}
			if test.name == "embedding" {
				field := parsed.LookUpField("Embedding")
				if field == nil || field.TagSettings["TYPE"] != "vector(1536)" {
					t.Fatalf("Embedding type = %#v", field)
				}
			}
		})
	}
}

func newMockDatabase(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	db, err := gorm.Open(
		postgresdriver.New(postgresdriver.Config{Conn: sqlDB}),
		&gorm.Config{DisableAutomaticPing: true},
	)
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("gorm.Open() error = %v", err)
	}
	return db, mock, func() { _ = sqlDB.Close() }
}

func expectMetadataQueries(mock sqlmock.Sqlmock, extensionExists, schemaExists bool, rows *sqlmock.Rows) {
	mock.ExpectQuery(regexp.QuoteMeta(extensionQuery)).
		WithArgs("vector").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(extensionExists))
	if !extensionExists {
		return
	}
	mock.ExpectQuery(regexp.QuoteMeta(schemaQuery)).
		WithArgs("vdb").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(schemaExists))
	if !schemaExists {
		return
	}
	mock.ExpectQuery("(?s)SELECT.*FROM pg_catalog\\.pg_attribute.*ORDER BY c\\.relname, a\\.attnum").
		WithArgs("vdb").
		WillReturnRows(rows)
}

func expectedColumnRows() *sqlmock.Rows {
	return rowsFromColumns(expectedColumns())
}

func expectedColumns() []databaseColumn {
	columns := make([]databaseColumn, 0, 38)
	for tableName, table := range requiredTables {
		for columnName, spec := range table {
			columns = append(columns, databaseColumn{
				TableName:  tableName,
				ColumnName: columnName,
				DataType:   spec.typeName,
				NotNull:    spec.notNull,
			})
		}
	}
	return columns
}

func rowsFromColumns(columns []databaseColumn) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"table_name", "column_name", "data_type", "not_null"})
	for _, column := range columns {
		rows.AddRow(column.TableName, column.ColumnName, column.DataType, column.NotNull)
	}
	return rows
}

func filterColumns(columns []databaseColumn, keep func(databaseColumn) bool) []databaseColumn {
	filtered := make([]databaseColumn, 0, len(columns))
	for _, column := range columns {
		if keep(column) {
			filtered = append(filtered, column)
		}
	}
	return filtered
}
