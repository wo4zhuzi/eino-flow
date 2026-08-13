// Package postgres 实现 RAG 索引 Feature 的 PostgreSQL 映射与结构校验。
package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"gorm.io/gorm"
)

var (
	// ErrNilContext 表示 schema 校验没有可用的 context。
	ErrNilContext = errors.New("索引 PostgreSQL 校验 context 不能为空")
	// ErrUnavailable 表示没有可用的 GORM 连接。
	ErrUnavailable = errors.New("索引 PostgreSQL 连接不可用")
	// ErrInvalidSchema 表示 schema 不是可安全引用的普通标识符。
	ErrInvalidSchema = errors.New("索引 PostgreSQL schema 无效")
	// ErrSchemaQuery 表示读取 PostgreSQL 元数据失败。
	ErrSchemaQuery = errors.New("查询索引 PostgreSQL schema 失败")
	// ErrSchemaMismatch 表示数据库结构不满足索引存储契约。
	ErrSchemaMismatch = errors.New("索引 PostgreSQL schema 不匹配")

	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

type columnSpec struct {
	typeName string
	notNull  bool
}

var requiredTables = map[string]map[string]columnSpec{
	chunkSetsTable: {
		"id":                {typeName: "uuid", notNull: true},
		"tenant_id":         {typeName: "text", notNull: true},
		"knowledge_base_id": {typeName: "text", notNull: true},
		"document_id":       {typeName: "text", notNull: true},
		"source_uri":        {typeName: "text", notNull: true},
		"source_name":       {typeName: "text", notNull: true},
		"content_sha256":    {typeName: "text", notNull: true},
		"strategy_name":     {typeName: "text", notNull: true},
		"profile_name":      {typeName: "text", notNull: true},
		"profile_version":   {typeName: "text", notNull: true},
		"config":            {typeName: "jsonb", notNull: true},
		"status":            {typeName: "text", notNull: true},
		"created_at":        {typeName: "timestamp with time zone", notNull: true},
		"activated_at":      {typeName: "timestamp with time zone", notNull: false},
	},
	chunksTable: {
		"chunk_set_id":      {typeName: "uuid", notNull: true},
		"chunk_id":          {typeName: "text", notNull: true},
		"kind":              {typeName: "text", notNull: true},
		"level":             {typeName: "integer", notNull: true},
		"parent_chunk_id":   {typeName: "text", notNull: false},
		"previous_chunk_id": {typeName: "text", notNull: false},
		"next_chunk_id":     {typeName: "text", notNull: false},
		"sequence":          {typeName: "integer", notNull: true},
		"content":           {typeName: "text", notNull: true},
		"character_count":   {typeName: "integer", notNull: true},
		"token_count":       {typeName: "integer", notNull: true},
		"source_unit_ids":   {typeName: "text[]", notNull: true},
		"metadata":          {typeName: "jsonb", notNull: true},
		"created_at":        {typeName: "timestamp with time zone", notNull: true},
	},
	chunkEmbeddingsTable: {
		"chunk_set_id":          {typeName: "uuid", notNull: true},
		"chunk_id":              {typeName: "text", notNull: true},
		"model_key":             {typeName: "text", notNull: true},
		"embedding_text":        {typeName: "text", notNull: true},
		"embedding_token_count": {typeName: "integer", notNull: true},
		"input_sha256":          {typeName: "text", notNull: true},
		"embedding":             {typeName: "vector(1536)", notNull: true},
		"searchable":            {typeName: "boolean", notNull: true},
		"created_at":            {typeName: "timestamp with time zone", notNull: true},
		"updated_at":            {typeName: "timestamp with time zone", notNull: true},
	},
}

type tables struct {
	ChunkSets       string
	Chunks          string
	ChunkEmbeddings string
}

func newTables(schema string) (tables, error) {
	schema = strings.TrimSpace(schema)
	if !identifierPattern.MatchString(schema) {
		return tables{}, ErrInvalidSchema
	}
	return tables{
		ChunkSets:       qualifiedTable(schema, chunkSetsTable),
		Chunks:          qualifiedTable(schema, chunksTable),
		ChunkEmbeddings: qualifiedTable(schema, chunkEmbeddingsTable),
	}, nil
}

// Validator 对已存在的索引数据库结构执行只读启动校验。
type Validator struct {
	db     *gorm.DB
	schema string
}

// NewValidator 创建索引 schema 校验器。
func NewValidator(db *gorm.DB, schema string) (*Validator, error) {
	if db == nil {
		return nil, ErrUnavailable
	}
	_, err := newTables(schema)
	if err != nil {
		return nil, err
	}
	return &Validator{db: db, schema: strings.TrimSpace(schema)}, nil
}

// Validate 验证 vector 扩展、目标 schema、三张正式表及全部既定列。
func (v *Validator) Validate(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if v == nil || v.db == nil {
		return ErrUnavailable
	}
	var extensionExists bool
	if err := v.db.WithContext(ctx).
		Raw("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = ?)", "vector").
		Scan(&extensionExists).Error; err != nil {
		return queryError(err)
	}
	if !extensionExists {
		return mismatch("缺少 vector 扩展")
	}

	var schemaExists bool
	if err := v.db.WithContext(ctx).
		Raw("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = ?)", v.schema).
		Scan(&schemaExists).Error; err != nil {
		return queryError(err)
	}
	if !schemaExists {
		return mismatch("缺少 schema " + v.schema)
	}

	var columns []databaseColumn
	if err := v.db.WithContext(ctx).Raw(`
SELECT
    c.relname AS table_name,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnotnull AS not_null
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ?
  AND c.relkind IN ('r', 'p')
  AND c.relname IN ('chunk_sets', 'chunks', 'chunk_embeddings')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY c.relname, a.attnum`, v.schema).Scan(&columns).Error; err != nil {
		return queryError(err)
	}
	return compareSchema(columns)
}

type databaseColumn struct {
	TableName  string `gorm:"column:table_name"`
	ColumnName string `gorm:"column:column_name"`
	DataType   string `gorm:"column:data_type"`
	NotNull    bool   `gorm:"column:not_null"`
}

func compareSchema(columns []databaseColumn) error {
	actual := make(map[string]map[string]columnSpec, len(requiredTables))
	for _, column := range columns {
		if actual[column.TableName] == nil {
			actual[column.TableName] = make(map[string]columnSpec)
		}
		actual[column.TableName][column.ColumnName] = columnSpec{
			typeName: strings.ToLower(strings.TrimSpace(column.DataType)),
			notNull:  column.NotNull,
		}
	}

	tableNames := make([]string, 0, len(requiredTables))
	for tableName := range requiredTables {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	for _, tableName := range tableNames {
		actualColumns, ok := actual[tableName]
		if !ok {
			return mismatch("缺少表 " + tableName)
		}
		columnNames := make([]string, 0, len(requiredTables[tableName]))
		for columnName := range requiredTables[tableName] {
			columnNames = append(columnNames, columnName)
		}
		sort.Strings(columnNames)
		for _, columnName := range columnNames {
			expected := requiredTables[tableName][columnName]
			observed, ok := actualColumns[columnName]
			if !ok {
				return mismatch(fmt.Sprintf("缺少列 %s.%s", tableName, columnName))
			}
			if observed.typeName != expected.typeName {
				return mismatch(fmt.Sprintf(
					"列 %s.%s 类型为 %s，期望 %s",
					tableName,
					columnName,
					observed.typeName,
					expected.typeName,
				))
			}
			if observed.notNull != expected.notNull {
				return mismatch(fmt.Sprintf("列 %s.%s 可空性不匹配", tableName, columnName))
			}
		}
	}
	return nil
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func qualifiedTable(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func queryError(cause error) error {
	return fmt.Errorf("%w: %w", ErrSchemaQuery, cause)
}

func mismatch(detail string) error {
	return fmt.Errorf("%w: %s", ErrSchemaMismatch, detail)
}
