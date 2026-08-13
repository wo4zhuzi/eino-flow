package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
)

func TestValidateConfiguredDatabase(t *testing.T) {
	if os.Getenv("EINO_FLOW_POSTGRES_INTEGRATION") != "1" {
		t.Skip("需要显式注入运行环境并设置 EINO_FLOW_POSTGRES_INTEGRATION=1")
	}
	configuration, err := appconfig.Load()
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := dbpostgres.Open(ctx, configuration.PostgreSQL())
	if err != nil {
		t.Fatalf("postgres.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("postgres.Close() error = %v", err)
		}
	})
	db, err := client.DB()
	if err != nil {
		t.Fatalf("postgres.DB() error = %v", err)
	}
	validator, err := NewValidator(db, configuration.PostgreSQL().Schema())
	if err != nil {
		t.Fatalf("NewValidator() error = %v", err)
	}
	if err := validator.Validate(ctx); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
