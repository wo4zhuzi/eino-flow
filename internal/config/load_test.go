package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	testPassword = "database-secret-value"
	testAPIKey   = "embedding-secret-value"
)

func TestLoadFromValidConfiguration(t *testing.T) {
	environment := validEnvironment()
	environment[EnvPostgresPort] = " 15432 "
	environment[EnvPostgresSSLMode] = "verify-full"
	environment[EnvPostgresSchema] = "index_v1"
	environment[EnvPostgresMaxOpenConns] = "40"
	environment[EnvPostgresMaxIdleConns] = "10"
	environment[EnvPostgresConnMaxLifetime] = "45m"
	environment[EnvEmbeddingBaseURL] = "https://dashscope.aliyuncs.com/compatible-mode/v1/"
	environment[EnvEmbeddingDimensions] = "1536"
	environment[EnvEmbeddingTimeout] = "20s"
	environment[EnvEmbeddingBatchSize] = "8"

	configuration, err := LoadFrom(mapLookup(environment))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	postgres := configuration.PostgreSQL()
	if postgres.Host() != "127.0.0.1" || postgres.Port() != 15432 {
		t.Fatalf("PostgreSQL address = %q:%d", postgres.Host(), postgres.Port())
	}
	if postgres.User() != "application" || postgres.Password().Reveal() != testPassword {
		t.Fatalf("PostgreSQL credentials were not loaded")
	}
	if postgres.Database() != "knowledge" || postgres.SSLMode() != "verify-full" || postgres.Schema() != "index_v1" {
		t.Fatalf("PostgreSQL target = %#v", postgres)
	}
	if postgres.MaxOpenConns() != 40 || postgres.MaxIdleConns() != 10 || postgres.ConnMaxLifetime() != 45*time.Minute {
		t.Fatalf("PostgreSQL pool = %#v", postgres)
	}

	embedding := configuration.Embedding()
	if embedding.BaseURL() != "https://dashscope.aliyuncs.com/compatible-mode/v1" {
		t.Fatalf("Embedding BaseURL = %q", embedding.BaseURL())
	}
	if embedding.Key().Reveal() != testAPIKey || embedding.Model() != "text-embedding-v4" {
		t.Fatalf("Embedding credentials or model were not loaded")
	}
	if embedding.Dimensions() != 1536 || embedding.Timeout() != 20*time.Second || embedding.BatchSize() != 8 {
		t.Fatalf("Embedding request config = %#v", embedding)
	}
}

func TestLoadFromUsesOnlyNonSensitiveDefaults(t *testing.T) {
	configuration, err := LoadFrom(mapLookup(validEnvironment()))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	postgres := configuration.PostgreSQL()
	if postgres.Port() != DefaultPostgresPort ||
		postgres.SSLMode() != DefaultPostgresSSLMode ||
		postgres.Schema() != DefaultPostgresSchema ||
		postgres.MaxOpenConns() != DefaultPostgresMaxOpenConns ||
		postgres.MaxIdleConns() != DefaultPostgresMaxIdleConns ||
		postgres.ConnMaxLifetime() != DefaultPostgresConnMaxLifetime {
		t.Fatalf("PostgreSQL defaults = %#v", postgres)
	}
	embedding := configuration.Embedding()
	if embedding.Dimensions() != RequiredEmbeddingDimensions ||
		embedding.Timeout() != DefaultEmbeddingTimeout ||
		embedding.BatchSize() != DefaultEmbeddingBatchSize {
		t.Fatalf("Embedding defaults = %#v", embedding)
	}
}

func TestLoadFromRejectsMissingRequiredVariables(t *testing.T) {
	required := []string{
		EnvPostgresHost,
		EnvPostgresUser,
		EnvPostgresPassword,
		EnvPostgresDB,
		EnvEmbeddingBaseURL,
		EnvEmbeddingKey,
		EnvEmbeddingModel,
	}
	for _, variable := range required {
		t.Run(variable, func(t *testing.T) {
			environment := validEnvironment()
			delete(environment, variable)

			_, err := LoadFrom(mapLookup(environment))
			assertConfigError(t, err, variable)
		})
	}
}

func TestLoadFromRejectsInvalidPostgreSQLValues(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
		mutate   func(map[string]string)
	}{
		{name: "non-numeric port", variable: EnvPostgresPort, value: "not-a-port"},
		{name: "port below range", variable: EnvPostgresPort, value: "0"},
		{name: "port above range", variable: EnvPostgresPort, value: "65536"},
		{name: "invalid ssl mode", variable: EnvPostgresSSLMode, value: "enabled"},
		{name: "unsafe schema", variable: EnvPostgresSchema, value: "vdb;drop schema vdb"},
		{name: "zero max open", variable: EnvPostgresMaxOpenConns, value: "0"},
		{name: "negative max idle", variable: EnvPostgresMaxIdleConns, value: "-1"},
		{
			name:     "max idle exceeds max open",
			variable: EnvPostgresMaxIdleConns,
			mutate: func(environment map[string]string) {
				environment[EnvPostgresMaxOpenConns] = "2"
				environment[EnvPostgresMaxIdleConns] = "3"
			},
		},
		{name: "invalid lifetime", variable: EnvPostgresConnMaxLifetime, value: "thirty"},
		{name: "zero lifetime", variable: EnvPostgresConnMaxLifetime, value: "0s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := validEnvironment()
			if test.mutate != nil {
				test.mutate(environment)
			} else {
				environment[test.variable] = test.value
			}

			_, err := LoadFrom(mapLookup(environment))
			assertConfigError(t, err, test.variable)
			assertDistinctValueNotLeaked(t, err, test.value)
		})
	}
}

func TestLoadFromRejectsInvalidEmbeddingValues(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
	}{
		{name: "relative URL", variable: EnvEmbeddingBaseURL, value: "/compatible-mode/v1"},
		{name: "unsupported URL scheme", variable: EnvEmbeddingBaseURL, value: "ftp://example.com/v1"},
		{name: "URL credentials", variable: EnvEmbeddingBaseURL, value: "https://user:password@example.com/v1"},
		{name: "URL query", variable: EnvEmbeddingBaseURL, value: "https://example.com/v1?key=value"},
		{name: "URL fragment", variable: EnvEmbeddingBaseURL, value: "https://example.com/v1#fragment"},
		{name: "embeddings endpoint", variable: EnvEmbeddingBaseURL, value: "https://example.com/v1/embeddings"},
		{name: "embeddings endpoint trailing slash", variable: EnvEmbeddingBaseURL, value: "https://example.com/v1/EMBEDDINGS/"},
		{name: "unsupported model", variable: EnvEmbeddingModel, value: "text-embedding-v3"},
		{name: "wrong dimensions", variable: EnvEmbeddingDimensions, value: "1024"},
		{name: "invalid dimensions", variable: EnvEmbeddingDimensions, value: "many"},
		{name: "invalid timeout", variable: EnvEmbeddingTimeout, value: "soon"},
		{name: "negative timeout", variable: EnvEmbeddingTimeout, value: "-1s"},
		{name: "zero batch size", variable: EnvEmbeddingBatchSize, value: "0"},
		{name: "batch size above provider limit", variable: EnvEmbeddingBatchSize, value: "11"},
		{name: "invalid batch size", variable: EnvEmbeddingBatchSize, value: "several"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := validEnvironment()
			environment[test.variable] = test.value

			_, err := LoadFrom(mapLookup(environment))
			assertConfigError(t, err, test.variable)
			assertDistinctValueNotLeaked(t, err, test.value)
		})
	}
}

func TestConfigurationFormattingRedactsSecrets(t *testing.T) {
	configuration, err := LoadFrom(mapLookup(validEnvironment()))
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	values := []any{
		configuration,
		configuration.PostgreSQL(),
		configuration.Embedding(),
		configuration.PostgreSQL().Password(),
		configuration.Embedding().Key(),
	}
	verbs := []string{"%s", "%v", "%+v", "%#v", "%q"}
	for _, value := range values {
		for _, verb := range verbs {
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, testPassword) || strings.Contains(formatted, testAPIKey) {
				t.Fatalf("fmt.Sprintf(%q, %T) leaked a secret: %s", verb, value, formatted)
			}
		}
	}
}

func TestInvalidSecretDoesNotLeak(t *testing.T) {
	for _, variable := range []string{EnvPostgresPassword, EnvEmbeddingKey} {
		t.Run(variable, func(t *testing.T) {
			environment := validEnvironment()
			environment[variable] = "   "

			_, err := LoadFrom(mapLookup(environment))
			assertConfigError(t, err, variable)
			formatted := fmt.Sprintf("%v | %+v | %#v", err, err, err)
			if strings.Contains(formatted, environment[variable]) && environment[variable] != "" {
				t.Fatalf("error leaked secret whitespace: %q", formatted)
			}
		})
	}
}

func TestLoadFromRejectsNilLookup(t *testing.T) {
	_, err := LoadFrom(nil)
	if !errors.Is(err, ErrNilLookupEnv) {
		t.Fatalf("LoadFrom(nil) error = %v", err)
	}
}

func TestLoadProcessEnvironment(t *testing.T) {
	if os.Getenv("EINO_FLOW_CONFIG_INTEGRATION") != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_CONFIG_INTEGRATION=1")
	}
	configuration, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	formatted := fmt.Sprintf("%#v", configuration)
	if strings.Contains(formatted, configuration.PostgreSQL().Password().Reveal()) ||
		strings.Contains(formatted, configuration.Embedding().Key().Reveal()) {
		t.Fatalf("Load() formatted configuration leaked a secret")
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		EnvPostgresHost:     "127.0.0.1",
		EnvPostgresUser:     "application",
		EnvPostgresPassword: testPassword,
		EnvPostgresDB:       "knowledge",
		EnvEmbeddingBaseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1",
		EnvEmbeddingKey:     testAPIKey,
		EnvEmbeddingModel:   "text-embedding-v4",
	}
}

func mapLookup(environment map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := environment[key]
		return value, ok
	}
}

func assertConfigError(t *testing.T, err error, variable string) {
	t.Helper()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("LoadFrom() error = %v, want ErrInvalidConfig", err)
	}
	var configError *Error
	if !errors.As(err, &configError) || configError.Variable() != variable {
		t.Fatalf("LoadFrom() error = %#v, want variable %s", err, variable)
	}
	formatted := fmt.Sprintf("%v | %+v | %#v", err, err, err)
	if strings.Contains(formatted, testPassword) || strings.Contains(formatted, testAPIKey) {
		t.Fatalf("error leaked a secret: %s", formatted)
	}
}

func assertDistinctValueNotLeaked(t *testing.T, err error, value string) {
	t.Helper()
	// 极短边界值可能自然出现在固定错误说明中，不能用子串匹配判断泄漏。
	if len(value) < 4 {
		return
	}
	if strings.Contains(fmt.Sprintf("%#v", err), value) {
		t.Fatalf("error leaked invalid value %q: %#v", value, err)
	}
}
