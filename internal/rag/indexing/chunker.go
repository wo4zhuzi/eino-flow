package indexing

import (
	"context"
	"fmt"
	"strings"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/adapter"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
)

const (
	defaultProfileName       = "rag-document-indexing"
	defaultProfileVersion    = "v2"
	defaultParentMaxRunes    = 2000
	defaultChildMaxRunes     = 500
	defaultStructureMaxRunes = 1800
	defaultStructureMinRunes = 600
)

// ChunkConfig 配置两类 Chunk 策略及稳定 Profile。
type ChunkConfig struct {
	ProfileName       string
	ProfileVersion    string
	ParentMaxRunes    int
	ChildMaxRunes     int
	StructureMaxRunes int
	StructureMinRunes int
}

// DefaultChunkConfig 返回适合中英混合知识文档的基线配置。
func DefaultChunkConfig() ChunkConfig {
	return ChunkConfig{
		ProfileName:       defaultProfileName,
		ProfileVersion:    defaultProfileVersion,
		ParentMaxRunes:    defaultParentMaxRunes,
		ChildMaxRunes:     defaultChildMaxRunes,
		StructureMaxRunes: defaultStructureMaxRunes,
		StructureMinRunes: defaultStructureMinRunes,
	}
}

// AutomaticChunker 根据 Parser 输出能力选择 Package 提供的 Chunk 策略。
type AutomaticChunker struct {
	profile        chunking.Profile
	parentChild    chunking.Strategy
	structureAware chunking.Strategy
}

// NewAutomaticChunker 创建可并发复用的自动 Chunk 组件。
func NewAutomaticChunker(config ChunkConfig) (*AutomaticChunker, error) {
	profile := chunking.Profile{
		Name:    strings.TrimSpace(config.ProfileName),
		Version: strings.TrimSpace(config.ProfileVersion),
	}
	if profile.Name == "" || profile.Version == "" {
		return nil, fmt.Errorf("%w: ProfileName 和 ProfileVersion 不能为空", ErrInvalidChunkConfig)
	}
	parentBuilder, err := parentchild.NewBoundedParentBuilder(parentchild.BoundedParentBuilderConfig{
		MaxRunes: config.ParentMaxRunes,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: 创建父 Chunk 构造器: %w", ErrInvalidChunkConfig, err)
	}
	childSplitter, err := parentchild.NewBoundedTextSplitter(parentchild.BoundedTextSplitterConfig{
		MaxRunes: config.ChildMaxRunes,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: 创建子 Chunk 切分器: %w", ErrInvalidChunkConfig, err)
	}
	parentChild, err := parentchild.NewParentChildStrategy(parentchild.ParentChildConfig{
		ParentBuilder: parentBuilder,
		ChildSplitter: childSplitter,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: 创建 Parent-child 策略: %w", ErrInvalidChunkConfig, err)
	}
	structureAware, err := structureaware.NewStructureAwareStrategy(structureaware.StructureAwareConfig{
		MaxRunes:       config.StructureMaxRunes,
		MinRunes:       config.StructureMinRunes,
		HeadingContext: structureaware.HeadingContextPrepend,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: 创建 Structure-aware 策略: %w", ErrInvalidChunkConfig, err)
	}
	return &AutomaticChunker{
		profile:        profile,
		parentChild:    parentChild,
		structureAware: structureAware,
	}, nil
}

// Chunk 使用 ingestion 标准输出创建适配器并执行匹配的 Chunk 策略。
func (c *AutomaticChunker) Chunk(ctx context.Context, result *ingestion.Result) (*chunking.Result, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if c == nil || c.parentChild == nil || c.structureAware == nil {
		return nil, fmt.Errorf("%w: AutomaticChunker 不可用", ErrInvalidChunkConfig)
	}
	if result == nil {
		return nil, ingestion.ErrNoParsedContent
	}
	formatAdapter, err := adapter.NewIngestionAdapter(result.Parser)
	if err != nil {
		return nil, fmt.Errorf("创建 ingestion adapter: %w", err)
	}
	strategy := c.parentChild
	if result.Parser.Output.Structured {
		strategy = c.structureAware
	}
	engine, err := chunking.NewEngine(chunking.EngineConfig{
		Profile:  c.profile,
		Adapter:  formatAdapter,
		Strategy: strategy,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 Chunk Engine: %w", err)
	}
	chunked, err := engine.Chunk(ctx, result.Documents)
	if err != nil {
		return nil, fmt.Errorf("执行 %s Chunk: %w", strategy.Name(), err)
	}
	return chunked, nil
}

var _ Chunker = (*AutomaticChunker)(nil)
