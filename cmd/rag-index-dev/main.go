package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cloudwego/eino-ext/devops"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
	"github.com/wo4zhuzi/eino-document-parser-structured/markdown"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexing"
)

const einoDevEnv = "EINO_DEV"

func main() {
	sourceURI := filepath.Join("testdata", "knowledge.md")
	if len(os.Args) > 1 {
		sourceURI = os.Args[1]
	}

	ctx := context.Background()
	einoDevEnabled := os.Getenv(einoDevEnv) == "true"
	if einoDevEnabled {
		if err := devops.Init(ctx); err != nil {
			panic(fmt.Errorf("初始化 Eino DevOps: %w", err))
		}
	}

	registry, err := ingestion.NewDefaultRegistry(ctx)
	if err != nil {
		panic(fmt.Errorf("创建默认 Parser 注册表: %w", err))
	}
	if err := registry.ReplaceParser(
		ingestion.ExtensionMarkdown,
		markdown.ParserInfo(),
		markdown.New(),
	); err != nil {
		panic(fmt.Errorf("替换结构化 Markdown Parser: %w", err))
	}
	ingestor, err := ingestion.New(ctx, ingestion.Config{
		MaxFileBytes: ingestion.DefaultMaxFileBytes,
		Registry:     registry,
	})
	if err != nil {
		panic(fmt.Errorf("创建文档摄取器: %w", err))
	}
	chunker, err := indexing.NewAutomaticChunker(indexing.DefaultChunkConfig())
	if err != nil {
		panic(fmt.Errorf("创建自动 Chunker: %w", err))
	}
	workflow, err := indexing.New(ctx, indexing.Dependencies{
		Ingestor: ingestor,
		Chunker:  chunker,
	})
	if err != nil {
		panic(err)
	}
	result, err := workflow.Run(ctx, indexing.Request{
		RunID:     "demo19-local-run",
		SourceURI: sourceURI,
	})
	if err != nil {
		panic(err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		panic(fmt.Errorf("输出工作流结果: %w", err))
	}

	if einoDevEnabled {
		waitForEinoDev(ctx)
	}
}

func waitForEinoDev(parent context.Context) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("eino_dev=ready address=127.0.0.1:52538")
	fmt.Println("按 Ctrl+C 停止 Eino Dev 模式")
	<-ctx.Done()
}
