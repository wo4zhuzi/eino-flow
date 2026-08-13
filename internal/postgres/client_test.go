package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	postgresdriver "gorm.io/driver/postgres"
)

const testPassword = "database-password-that-must-not-leak"

func TestOpenConfiguresPoolAndLifecycle(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	mock.ExpectPing()
	mock.ExpectPing()
	mock.ExpectClose()

	client, err := open(
		context.Background(),
		testConfig(t),
		postgresdriver.New(postgresdriver.Config{Conn: sqlDB}),
	)
	if err != nil {
		t.Fatalf("open() error = %v", err)
	}
	db, err := client.DB()
	if err != nil || db == nil {
		t.Fatalf("DB() = %#v, %v", db, err)
	}
	if stats := sqlDB.Stats(); stats.MaxOpenConnections != 7 {
		t.Fatalf("MaxOpenConnections = %d, want 7", stats.MaxOpenConnections)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := client.DB(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("DB() after Close error = %v", err)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping() after Close error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestOpenClosesPoolWhenPingFailsAndRedactsCause(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	cause := errors.New("connection rejected password=" + testPassword)
	mock.ExpectPing().WillReturnError(cause)
	mock.ExpectClose()

	_, err = open(
		context.Background(),
		testConfig(t),
		postgresdriver.New(postgresdriver.Config{Conn: sqlDB}),
	)
	if !errors.Is(err, ErrPing) || !errors.Is(err, cause) {
		t.Fatalf("open() error = %v, want ErrPing and cause", err)
	}
	formatted := fmt.Sprintf("%v | %+v | %#v", err, err, err)
	if strings.Contains(formatted, testPassword) {
		t.Fatalf("open() error leaked password: %s", formatted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestClientRejectsInvalidState(t *testing.T) {
	var client *Client
	if _, err := client.DB(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil DB() error = %v", err)
	}
	if err := client.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil Ping() error = %v", err)
	}
	if err := client.Close(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil Close() error = %v", err)
	}
	if _, err := Open(nil, appconfig.PostgreSQL{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Open(nil) error = %v", err)
	}
}

func TestConnectionURLHandlesReservedCharacters(t *testing.T) {
	configuration := testConfig(t)
	dsn := connectionURL(configuration)
	if !strings.Contains(dsn, "postgres://application:") ||
		!strings.Contains(dsn, "@127.0.0.1:5432/knowledge") ||
		!strings.Contains(dsn, "sslmode=disable") {
		t.Fatalf("connectionURL() = %q", dsn)
	}
	if strings.Contains(dsn, "p@ss word/") {
		t.Fatalf("connectionURL() did not escape reserved password characters")
	}
}

func testConfig(t *testing.T) appconfig.PostgreSQL {
	t.Helper()
	environment := map[string]string{
		appconfig.EnvPostgresHost:            "127.0.0.1",
		appconfig.EnvPostgresPort:            "5432",
		appconfig.EnvPostgresUser:            "application",
		appconfig.EnvPostgresPassword:        "p@ss word/" + testPassword,
		appconfig.EnvPostgresDB:              "knowledge",
		appconfig.EnvPostgresSSLMode:         "disable",
		appconfig.EnvPostgresSchema:          "vdb",
		appconfig.EnvPostgresMaxOpenConns:    "7",
		appconfig.EnvPostgresMaxIdleConns:    "3",
		appconfig.EnvPostgresConnMaxLifetime: "10m",
		appconfig.EnvEmbeddingBaseURL:        "https://example.com/v1",
		appconfig.EnvEmbeddingKey:            "embedding-key",
		appconfig.EnvEmbeddingModel:          "embedding-model",
	}
	configuration, err := appconfig.LoadFrom(func(key string) (string, bool) {
		value, ok := environment[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("config.LoadFrom() error = %v", err)
	}
	return configuration.PostgreSQL()
}
