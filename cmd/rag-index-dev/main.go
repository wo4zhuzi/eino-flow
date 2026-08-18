package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	pathpkg "path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/cloudwego/eino-ext/devops"
	"github.com/google/uuid"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-document-parser-structured/markdown"
	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	dbpostgres "github.com/wo4zhuzi/eino-flow/internal/postgres"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
	indexpostgres "github.com/wo4zhuzi/eino-flow/internal/rag/indexstore/postgres"
)

const (
	einoDevEnv                  = "EINO_DEV"
	devTenantID                 = "local-development"
	devKnowledgeBaseID          = "default"
	embeddingModelConfigVersion = "v1"
)

func main() {
	if err := run(context.Background(), os.Args, os.Stdout); err != nil {
		panic(err)
	}
}

func run(ctx context.Context, args []string, output io.Writer) (runErr error) {
	sourceURI := filepath.Join("testdata", "knowledge.md")
	if len(args) > 1 {
		sourceURI = args[1]
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

	registry, err := ingestion.NewDefaultRegistry(ctx)
	if err != nil {
		return fmt.Errorf("创建默认 Parser 注册表: %w", err)
	}
	if err := registry.ReplaceParser(
		ingestion.ExtensionMarkdown,
		markdown.ParserInfo(),
		markdown.New(),
	); err != nil {
		return fmt.Errorf("替换结构化 Markdown Parser: %w", err)
	}
	ingestor, err := ingestion.New(ctx, ingestion.Config{
		MaxFileBytes: ingestion.DefaultMaxFileBytes,
		Registry:     registry,
	})
	if err != nil {
		return fmt.Errorf("创建文档摄取器: %w", err)
	}
	chunkConfig := indexing.DefaultChunkConfig()
	chunker, err := indexing.NewAutomaticChunker(chunkConfig)
	if err != nil {
		return fmt.Errorf("创建自动 Chunker: %w", err)
	}
	workflow, err := indexing.New(ctx, indexing.Dependencies{
		Ingestor: ingestor,
		Chunker:  chunker,
		Embedder: embedder,
		Store:    store,
		BuildConfig: indexing.BuildConfig{
			Chunk: chunkConfig,
			Model: indexing.ModelProfile{
				Model:         configuration.Embedding().Model(),
				Dimensions:    configuration.Embedding().Dimensions(),
				Distance:      indexing.DistanceCosine,
				ConfigVersion: embeddingModelConfigVersion,
			},
		},
	})
	if err != nil {
		return err
	}
	setID := indexstore.SetID(uuid.NewString())
	if len(args) > 2 {
		setID = indexstore.SetID(strings.TrimSpace(args[2]))
	}
	documentID := stableDocumentID(sourceURI)
	sourceName := displaySourceName(sourceURI)
	result, err := workflow.Run(ctx, indexing.Request{
		RunID:     "rag-index-dev-" + string(setID),
		SourceURI: sourceURI,
		Index: indexing.IndexTarget{
			SetID:           setID,
			TenantID:        devTenantID,
			KnowledgeBaseID: devKnowledgeBaseID,
			DocumentID:      documentID,
			CanonicalURI:    "knowledge://local/" + documentID,
			SourceName:      sourceName,
			Title:           strings.TrimSuffix(sourceName, filepath.Ext(sourceName)),
		},
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

func stableDocumentID(sourceURI string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(sourceURI)))
	return fmt.Sprintf("document-%x", digest[:16])
}

func displaySourceName(sourceURI string) string {
	trimmed := strings.TrimSpace(sourceURI)
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" {
		if name := pathpkg.Base(parsed.Path); name != "." && name != "/" && name != "" {
			return name
		}
	}
	if name := filepath.Base(trimmed); name != "." && name != string(filepath.Separator) && name != "" {
		return name
	}
	return "document"
}

func waitForEinoDev(parent context.Context) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("eino_dev=ready address=127.0.0.1:52538")
	fmt.Println("按 Ctrl+C 停止 Eino Dev 模式")
	<-ctx.Done()
}
