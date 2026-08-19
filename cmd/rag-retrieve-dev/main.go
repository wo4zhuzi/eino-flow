package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/cloudwego/eino-ext/devops"
	"github.com/google/uuid"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	indexpostgres "github.com/wo4zhuzi/eino-flow/internal/rag/indexstore/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
)

const (
	einoDevEnv         = "EINO_DEV"
	devTenantID        = "local-development"
	devKnowledgeBaseID = "default"
	defaultQuery       = "Markdown 文档使用什么切分策略？"
	defaultTopK        = 3
)

func main() {
	if err := run(context.Background(), os.Args, os.Stdout); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, args []string, output io.Writer) (runErr error) {
	if ctx == nil {
		return retrieval.ErrNilContext
	}
	query, topK, err := parseArguments(args)
	if err != nil {
		return err
	}
	einoDevEnabled := os.Getenv(einoDevEnv) == "true"
	if einoDevEnabled {
		if err := devops.Init(ctx); err != nil {
			return fmt.Errorf("初始化 Eino DevOps: %w", err)
		}
	}

	configuration, err := appconfig.Load()
	if err != nil {
		return fmt.Errorf("加载运行配置: %w", err)
	}
	postgresClient, err := dbpostgres.Open(ctx, configuration.PostgreSQL())
	if err != nil {
		return fmt.Errorf("连接 PostgreSQL: %w", err)
	}
	defer func() {
		if closeErr := postgresClient.Close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("关闭 PostgreSQL: %w", closeErr))
		}
	}()
	db, err := postgresClient.DB()
	if err != nil {
		return fmt.Errorf("获取 PostgreSQL 连接: %w", err)
	}
	validator, err := indexpostgres.NewValidator(db, configuration.PostgreSQL().Schema())
	if err != nil {
		return fmt.Errorf("创建索引 schema 校验器: %w", err)
	}
	if err := validator.Validate(ctx); err != nil {
		return fmt.Errorf("校验索引 schema: %w", err)
	}
	store, err := indexpostgres.NewStore(db, configuration.PostgreSQL().Schema())
	if err != nil {
		return fmt.Errorf("创建 Index Store: %w", err)
	}
	embedder, err := embedding.New(ctx, configuration.Embedding())
	if err != nil {
		return fmt.Errorf("创建 Embedding 客户端: %w", err)
	}
	workflow, err := retrieval.New(ctx, retrieval.Dependencies{
		Embedder: embedder,
		Store:    store,
		Config: retrieval.Config{Model: indexstore.ModelProfile{
			Model:         configuration.Embedding().Model(),
			Dimensions:    configuration.Embedding().Dimensions(),
			Distance:      indexstore.DistanceCosine,
			ConfigVersion: appconfig.EmbeddingModelConfigVersion,
		}},
	})
	if err != nil {
		return err
	}
	result, err := workflow.Run(ctx, retrieval.Request{
		RunID: "rag-retrieve-dev-" + uuid.NewString(),
		Query: query,
		Scope: indexstore.Scope{
			TenantID:        devTenantID,
			KnowledgeBaseID: devKnowledgeBaseID,
		},
		TopK: topK,
	})
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("输出工作流结果: %w", err)
	}
	if einoDevEnabled {
		waitForEinoDev(ctx)
	}
	return nil
}

func parseArguments(args []string) (string, int, error) {
	query := defaultQuery
	topK := defaultTopK
	if len(args) > 3 {
		return "", 0, fmt.Errorf("%w: 只允许查询文本和 TopK 两个参数", retrieval.ErrInvalidRequest)
	}
	if len(args) > 1 {
		query = strings.TrimSpace(args[1])
	}
	if len(args) > 2 {
		parsed, err := strconv.Atoi(strings.TrimSpace(args[2]))
		if err != nil || parsed < 1 || parsed > retrieval.MaxTopK {
			return "", 0, fmt.Errorf("%w: TopK 必须在 1 到 %d 之间", retrieval.ErrInvalidRequest, retrieval.MaxTopK)
		}
		topK = parsed
	}
	if query == "" {
		return "", 0, fmt.Errorf("%w: 查询文本不能为空", retrieval.ErrInvalidRequest)
	}
	return query, topK, nil
}

func waitForEinoDev(parent context.Context) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("eino_dev=ready address=127.0.0.1:52538")
	fmt.Println("按 Ctrl+C 停止 Eino Dev 模式")
	<-ctx.Done()
}
