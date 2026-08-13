package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	EnvPostgresHost            = "POSTGRES_HOST"
	EnvPostgresPort            = "POSTGRES_PORT"
	EnvPostgresUser            = "POSTGRES_USER"
	EnvPostgresPassword        = "POSTGRES_PASSWORD"
	EnvPostgresDB              = "POSTGRES_DB"
	EnvPostgresSSLMode         = "POSTGRES_SSLMODE"
	EnvPostgresSchema          = "POSTGRES_SCHEMA"
	EnvPostgresMaxOpenConns    = "POSTGRES_MAX_OPEN_CONNS"
	EnvPostgresMaxIdleConns    = "POSTGRES_MAX_IDLE_CONNS"
	EnvPostgresConnMaxLifetime = "POSTGRES_CONN_MAX_LIFETIME"
	EnvEmbeddingBaseURL        = "EMBEDDING_BASE_URL"
	EnvEmbeddingKey            = "EMBEDDING_KEY"
	EnvEmbeddingModel          = "EMBEDDING_MODEL"
	EnvEmbeddingDimensions     = "EMBEDDING_DIMENSIONS"
	EnvEmbeddingTimeout        = "EMBEDDING_TIMEOUT"
	EnvEmbeddingBatchSize      = "EMBEDDING_BATCH_SIZE"

	DefaultPostgresPort            = 5432
	DefaultPostgresSSLMode         = "require"
	DefaultPostgresSchema          = "vdb"
	DefaultPostgresMaxOpenConns    = 25
	DefaultPostgresMaxIdleConns    = 5
	DefaultPostgresConnMaxLifetime = 30 * time.Minute
	RequiredEmbeddingDimensions    = 1536
	DefaultEmbeddingTimeout        = 30 * time.Second
	DefaultEmbeddingBatchSize      = 32
)

var (
	// ErrInvalidConfig 表示运行配置缺失或不满足约束。
	ErrInvalidConfig = errors.New("运行配置无效")
	// ErrNilLookupEnv 表示没有提供环境变量查询函数。
	ErrNilLookupEnv = errors.New("环境变量查询函数不能为空")

	schemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// LookupEnv 是可替换的环境变量查询函数。
type LookupEnv func(key string) (string, bool)

// Error 描述单个环境变量的配置错误，且不保存或输出原始值。
type Error struct {
	variable string
	reason   string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%v: %s: %s", ErrInvalidConfig, e.variable, e.reason)
}

func (e *Error) Unwrap() error {
	return ErrInvalidConfig
}

// Variable 返回发生错误的环境变量名。
func (e *Error) Variable() string {
	if e == nil {
		return ""
	}
	return e.variable
}

// Reason 返回不包含原始变量值的固定错误原因。
func (e *Error) Reason() string {
	if e == nil {
		return ""
	}
	return e.reason
}

// Load 从当前进程环境一次性加载并校验运行配置。
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom 使用指定查询函数加载配置，便于测试和受控装配。
func LoadFrom(lookup LookupEnv) (Config, error) {
	if lookup == nil {
		return Config{}, ErrNilLookupEnv
	}

	postgres, err := loadPostgreSQL(lookup)
	if err != nil {
		return Config{}, err
	}
	embedding, err := loadEmbedding(lookup)
	if err != nil {
		return Config{}, err
	}
	return Config{postgres: postgres, embedding: embedding}, nil
}

func loadPostgreSQL(lookup LookupEnv) (PostgreSQL, error) {
	host, err := requiredText(lookup, EnvPostgresHost)
	if err != nil {
		return PostgreSQL{}, err
	}
	port, err := integer(lookup, EnvPostgresPort, DefaultPostgresPort, 1, 65535)
	if err != nil {
		return PostgreSQL{}, err
	}
	user, err := requiredText(lookup, EnvPostgresUser)
	if err != nil {
		return PostgreSQL{}, err
	}
	password, err := requiredSecret(lookup, EnvPostgresPassword)
	if err != nil {
		return PostgreSQL{}, err
	}
	database, err := requiredText(lookup, EnvPostgresDB)
	if err != nil {
		return PostgreSQL{}, err
	}
	sslMode := optionalText(lookup, EnvPostgresSSLMode, DefaultPostgresSSLMode)
	if !validSSLMode(sslMode) {
		return PostgreSQL{}, invalid(EnvPostgresSSLMode, "必须是 disable、allow、prefer、require、verify-ca 或 verify-full")
	}
	schema := optionalText(lookup, EnvPostgresSchema, DefaultPostgresSchema)
	if !schemaPattern.MatchString(schema) {
		return PostgreSQL{}, invalid(EnvPostgresSchema, "必须是合法的 PostgreSQL 普通标识符")
	}
	maxOpenConns, err := integer(lookup, EnvPostgresMaxOpenConns, DefaultPostgresMaxOpenConns, 1, int(^uint(0)>>1))
	if err != nil {
		return PostgreSQL{}, err
	}
	maxIdleConns, err := integer(lookup, EnvPostgresMaxIdleConns, DefaultPostgresMaxIdleConns, 0, int(^uint(0)>>1))
	if err != nil {
		return PostgreSQL{}, err
	}
	if maxIdleConns > maxOpenConns {
		return PostgreSQL{}, invalid(EnvPostgresMaxIdleConns, "不得大于 POSTGRES_MAX_OPEN_CONNS")
	}
	connMaxLifetime, err := duration(lookup, EnvPostgresConnMaxLifetime, DefaultPostgresConnMaxLifetime)
	if err != nil {
		return PostgreSQL{}, err
	}

	return PostgreSQL{
		host:            host,
		port:            port,
		user:            user,
		password:        password,
		database:        database,
		sslMode:         sslMode,
		schema:          schema,
		maxOpenConns:    maxOpenConns,
		maxIdleConns:    maxIdleConns,
		connMaxLifetime: connMaxLifetime,
	}, nil
}

func loadEmbedding(lookup LookupEnv) (Embedding, error) {
	baseURL, err := requiredText(lookup, EnvEmbeddingBaseURL)
	if err != nil {
		return Embedding{}, err
	}
	baseURL, err = normalizeBaseURL(baseURL)
	if err != nil {
		return Embedding{}, invalid(EnvEmbeddingBaseURL, "必须是 http/https API 根地址，且不能包含认证信息、查询参数、片段或 /embeddings endpoint")
	}
	key, err := requiredSecret(lookup, EnvEmbeddingKey)
	if err != nil {
		return Embedding{}, err
	}
	model, err := requiredText(lookup, EnvEmbeddingModel)
	if err != nil {
		return Embedding{}, err
	}
	dimensions, err := integer(
		lookup,
		EnvEmbeddingDimensions,
		RequiredEmbeddingDimensions,
		RequiredEmbeddingDimensions,
		RequiredEmbeddingDimensions,
	)
	if err != nil {
		return Embedding{}, err
	}
	timeout, err := duration(lookup, EnvEmbeddingTimeout, DefaultEmbeddingTimeout)
	if err != nil {
		return Embedding{}, err
	}
	batchSize, err := integer(lookup, EnvEmbeddingBatchSize, DefaultEmbeddingBatchSize, 1, int(^uint(0)>>1))
	if err != nil {
		return Embedding{}, err
	}

	return Embedding{
		baseURL:    baseURL,
		key:        key,
		model:      model,
		dimensions: dimensions,
		timeout:    timeout,
		batchSize:  batchSize,
	}, nil
}

func requiredText(lookup LookupEnv, variable string) (string, error) {
	value, ok := lookup(variable)
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return "", invalid(variable, "必须设置且不能为空")
	}
	return value, nil
}

func requiredSecret(lookup LookupEnv, variable string) (Secret, error) {
	value, ok := lookup(variable)
	if !ok || strings.TrimSpace(value) == "" {
		return Secret{}, invalid(variable, "必须设置且不能为空")
	}
	return Secret{value: value}, nil
}

func optionalText(lookup LookupEnv, variable, fallback string) string {
	value, ok := lookup(variable)
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func integer(lookup LookupEnv, variable string, fallback, minimum, maximum int) (int, error) {
	raw, ok := lookup(variable)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < minimum || value > maximum {
		return 0, invalid(variable, fmt.Sprintf("必须是 %d 到 %d 之间的整数", minimum, maximum))
	}
	return value, nil
}

func duration(lookup LookupEnv, variable string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(variable)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, invalid(variable, "必须是大于 0 的 Go duration，例如 30s 或 5m")
	}
	return value, nil
}

func normalizeBaseURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Opaque != "" || parsed.Host == "" {
		return "", ErrInvalidConfig
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", ErrInvalidConfig
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", ErrInvalidConfig
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	if strings.EqualFold(path.Base(parsed.Path), "embeddings") {
		return "", ErrInvalidConfig
	}
	return parsed.String(), nil
}

func validSSLMode(value string) bool {
	switch value {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func invalid(variable, reason string) error {
	return &Error{variable: variable, reason: reason}
}
