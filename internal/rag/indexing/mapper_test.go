package indexing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

func TestMapBuildParentChild(t *testing.T) {
	spec := parentChildSpec()
	result := parentChildResult()
	original := cloneChunkingResult(t, result)

	first, err := MapBuild(spec, result)
	if err != nil {
		t.Fatalf("MapBuild() error = %v", err)
	}
	second, err := MapBuild(spec, result)
	if err != nil {
		t.Fatalf("MapBuild() second error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("MapBuild() output is not deterministic:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if !reflect.DeepEqual(result, original) {
		t.Fatalf("MapBuild() mutated input:\ngot=%#v\nwant=%#v", result, original)
	}

	if got, want := string(first.Set.Config), `{"parent_max_runes":2000,"child_max_runes":500,"embedding_input_policy":"v1"}`; got != want {
		t.Fatalf("config = %s, want %s", got, want)
	}
	if len(first.Chunks) != 3 || len(first.EmbeddingInputs) != 2 {
		t.Fatalf("record counts = chunks:%d inputs:%d", len(first.Chunks), len(first.EmbeddingInputs))
	}
	parent := first.Chunks[0]
	if parent.ParentChunkID != nil || parent.PreviousChunkID != nil || parent.NextChunkID != nil {
		t.Fatalf("parent nullable relations = %#v", parent)
	}
	child := first.Chunks[1]
	if child.ParentChunkID == nil || *child.ParentChunkID != "parent-1" || child.PreviousChunkID != nil ||
		child.NextChunkID == nil || *child.NextChunkID != "child-2" {
		t.Fatalf("child relations = %#v", child)
	}
	for index, input := range first.EmbeddingInputs {
		wantText := []string{"安全标题\n\n子内容一", "安全标题\n\n子内容二"}[index]
		if input.Text != wantText || input.InputSHA256 != hashForTest(wantText) {
			t.Fatalf("input[%d] = %#v", index, input)
		}
		if input.ModelKey == "" || input.Candidate.SetID != spec.SetID {
			t.Fatalf("input[%d] identity = %#v", index, input)
		}
	}
	if first.EmbeddingInputs[0].ModelKey != first.EmbeddingInputs[1].ModelKey {
		t.Fatal("same model profile produced different model keys")
	}

	result.Chunks[1].SourceUnitIDs[0] = "changed"
	result.Chunks[1].Metadata["nested"].(map[string]any)["key"] = "changed"
	if first.Chunks[1].SourceUnitIDs[0] != "unit-1" || string(first.Chunks[1].Metadata) != `{"nested":{"key":"value"}}` {
		t.Fatalf("mapped output aliases input = %#v", first.Chunks[1])
	}
}

func TestMapBuildStructureAware(t *testing.T) {
	tests := []struct {
		name           string
		headingContext string
		metadata       map[string]any
		content        string
		wantText       string
	}{
		{
			name:           "prepend content is final input",
			headingContext: " prepend ",
			metadata: map[string]any{
				structureaware.MetadataStructureSemanticPath: []string{"指南", "安装"},
			},
			content:  "指南 > 安装\n\n执行安装。",
			wantText: "指南 > 安装\n\n执行安装。",
		},
		{
			name:           "metadata only prepends semantic path",
			headingContext: string(structureaware.HeadingContextMetadataOnly),
			metadata: map[string]any{
				structureaware.MetadataStructureSemanticPath: []any{" 指南 ", "安装"},
			},
			content:  "执行安装。",
			wantText: "指南 > 安装\n\n执行安装。",
		},
		{
			name:           "metadata only without path uses content",
			headingContext: string(structureaware.HeadingContextMetadataOnly),
			metadata:       map[string]any{},
			content:        "执行安装。",
			wantText:       "执行安装。",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := structureSpec(tt.headingContext)
			originalSpec := spec
			originalConfig := *spec.StructureAware
			result := &chunking.Result{
				Profile:      chunking.Profile{Name: spec.Profile.Name, Version: spec.Profile.Version},
				StrategyName: structureaware.StructureAwareStrategyName,
				Chunks: []chunking.Chunk{{
					ID:             "structure-1",
					Kind:           structureaware.ChunkKindStructure,
					Content:        tt.content,
					DocumentID:     spec.Document.ID,
					Level:          0,
					SourceUnitIDs:  []string{"unit-1"},
					Sequence:       1,
					CharacterCount: len([]rune(tt.content)),
					Metadata:       tt.metadata,
				}},
			}
			build, err := MapBuild(spec, result)
			if err != nil {
				t.Fatalf("MapBuild() error = %v", err)
			}
			if len(build.EmbeddingInputs) != 1 || build.EmbeddingInputs[0].Text != tt.wantText ||
				build.EmbeddingInputs[0].InputSHA256 != hashForTest(tt.wantText) {
				t.Fatalf("embedding input = %#v", build.EmbeddingInputs)
			}
			wantHeadingContext := tt.headingContext
			if tt.headingContext == " prepend " {
				wantHeadingContext = string(structureaware.HeadingContextPrepend)
			}
			wantConfig := `{"max_runes":1800,"min_runes":600,"heading_context":"` + wantHeadingContext + `","embedding_input_policy":"v1"}`
			if string(build.Set.Config) != wantConfig {
				t.Fatalf("config = %s, want %s", build.Set.Config, wantConfig)
			}
			if !reflect.DeepEqual(spec, originalSpec) || !reflect.DeepEqual(*spec.StructureAware, originalConfig) {
				t.Fatalf("MapBuild() mutated BuildSpec: got=%#v want=%#v", spec, originalSpec)
			}
		})
	}
}

func TestMapBuildRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name   string
		spec   func() BuildSpec
		result func() *chunking.Result
	}{
		{
			name: "nil result",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				return nil
			},
		},
		{
			name: "unsafe source uri",
			spec: func() BuildSpec {
				spec := parentChildSpec()
				spec.Document.SourceURI = "https://user:secret@example.com/file"
				return spec
			},
			result: parentChildResult,
		},
		{
			name: "relative source uri",
			spec: func() BuildSpec {
				spec := parentChildSpec()
				spec.Document.SourceURI = "documents/document-1"
				return spec
			},
			result: parentChildResult,
		},
		{
			name: "profile mismatch",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Profile.Version = "v-other"
				return result
			},
		},
		{
			name: "mixed strategy config",
			spec: func() BuildSpec {
				spec := parentChildSpec()
				spec.StructureAware = &StructureAwareConfig{MaxRunes: 10, MinRunes: 5, HeadingContext: "prepend"}
				return spec
			},
			result: parentChildResult,
		},
		{
			name: "parent is embedded kind",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Chunks[0].Kind = chunking.ChunkKindChild
				return result
			},
		},
		{
			name: "missing parent",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Chunks[1].ParentID = "unknown"
				return result
			},
		},
		{
			name: "asymmetric adjacency",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Chunks[2].PreviousID = ""
				return result
			},
		},
		{
			name: "document mismatch",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Chunks[2].DocumentID = "other-document"
				return result
			},
		},
		{
			name: "invalid metadata json",
			spec: parentChildSpec,
			result: func() *chunking.Result {
				result := parentChildResult()
				result.Chunks[1].Metadata["invalid"] = make(chan int)
				return result
			},
		},
		{
			name: "invalid semantic path",
			spec: func() BuildSpec {
				return structureSpec(string(structureaware.HeadingContextMetadataOnly))
			},
			result: func() *chunking.Result {
				spec := structureSpec(string(structureaware.HeadingContextMetadataOnly))
				return &chunking.Result{
					Profile:      chunking.Profile{Name: spec.Profile.Name, Version: spec.Profile.Version},
					StrategyName: structureaware.StructureAwareStrategyName,
					Chunks: []chunking.Chunk{{
						ID:             "structure-1",
						Kind:           structureaware.ChunkKindStructure,
						Content:        "正文",
						DocumentID:     spec.Document.ID,
						SourceUnitIDs:  []string{"unit-1"},
						Sequence:       1,
						CharacterCount: 2,
						Metadata: map[string]any{
							structureaware.MetadataStructureSemanticPath: []any{"指南", 1},
						},
					}},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := MapBuild(tt.spec(), tt.result())
			if !errors.Is(err, ErrInvalidBuild) {
				t.Fatalf("MapBuild() error = %v, want ErrInvalidBuild", err)
			}
		})
	}
}

func TestModelKeyStabilityAndSeparation(t *testing.T) {
	base := parentChildSpec()
	first, err := MapBuild(base, parentChildResult())
	if err != nil {
		t.Fatalf("MapBuild() error = %v", err)
	}
	spaced := base
	spaced.Model.Model = " text-embedding-v4 "
	spaced.Model.Distance = " cosine "
	spaced.Model.ConfigVersion = " model-v1 "
	second, err := MapBuild(spaced, parentChildResult())
	if err != nil {
		t.Fatalf("MapBuild() normalized error = %v", err)
	}
	if first.EmbeddingInputs[0].ModelKey != second.EmbeddingInputs[0].ModelKey {
		t.Fatal("equivalent model profiles produced different keys")
	}
	changed := base
	changed.Model.ConfigVersion = "model-v2"
	third, err := MapBuild(changed, parentChildResult())
	if err != nil {
		t.Fatalf("MapBuild() changed error = %v", err)
	}
	if first.EmbeddingInputs[0].ModelKey == third.EmbeddingInputs[0].ModelKey {
		t.Fatal("different model config versions produced the same key")
	}
}

func TestStoreContract(t *testing.T) {
	var _ Store = storeStub{}
}

type storeStub struct{}

func (storeStub) PrepareBuild(context.Context, BuildData) ([]EmbeddingInput, error) {
	return nil, nil
}

func (storeStub) SaveEmbeddings(context.Context, indexstore.SetID, []EmbeddingRecord) error {
	return nil
}

func (storeStub) Validate(context.Context, indexstore.SetID) error {
	return nil
}

func (storeStub) Publish(context.Context, indexstore.SetID) error {
	return nil
}

func parentChildSpec() BuildSpec {
	return BuildSpec{
		SetID: "set-1",
		Scope: indexstore.Scope{
			TenantID:        "tenant-1",
			KnowledgeBaseID: "kb-1",
		},
		Document: Document{
			ID:            "document-1",
			SourceURI:     "knowledge://documents/document-1",
			SourceName:    "安全文档.md",
			Title:         "安全标题",
			ContentSHA256: hashForTest("original document"),
		},
		Profile: Profile{Name: "default", Version: "v1"},
		Model: ModelProfile{
			Model:         "text-embedding-v4",
			Dimensions:    1536,
			Distance:      DistanceCosine,
			ConfigVersion: "model-v1",
		},
		ParentChild: &ParentChildConfig{ParentMaxRunes: 2000, ChildMaxRunes: 500},
	}
}

func structureSpec(headingContext string) BuildSpec {
	spec := parentChildSpec()
	spec.ParentChild = nil
	spec.StructureAware = &StructureAwareConfig{
		MaxRunes:       1800,
		MinRunes:       600,
		HeadingContext: headingContext,
	}
	return spec
}

func parentChildResult() *chunking.Result {
	return &chunking.Result{
		Profile:      chunking.Profile{Name: "default", Version: "v1"},
		StrategyName: parentchild.ParentChildStrategyName,
		Chunks: []chunking.Chunk{
			{
				ID:             "parent-1",
				Kind:           chunking.ChunkKindParent,
				Content:        "父内容",
				DocumentID:     "document-1",
				Level:          0,
				SourceUnitIDs:  []string{"unit-1"},
				Sequence:       1,
				CharacterCount: 3,
				Metadata:       map[string]any{"parent": true},
			},
			{
				ID:             "child-1",
				Kind:           chunking.ChunkKindChild,
				Content:        "子内容一",
				DocumentID:     "document-1",
				Level:          1,
				ParentID:       "parent-1",
				NextID:         "child-2",
				SourceUnitIDs:  []string{"unit-1"},
				Sequence:       2,
				CharacterCount: 4,
				Metadata: map[string]any{
					"nested":      map[string]any{"key": "value"},
					"document_id": "untrusted-document",
					"source_uri":  "/private/source.md",
				},
			},
			{
				ID:             "child-2",
				Kind:           chunking.ChunkKindChild,
				Content:        "子内容二",
				DocumentID:     "document-1",
				Level:          1,
				ParentID:       "parent-1",
				PreviousID:     "child-1",
				SourceUnitIDs:  []string{"unit-1"},
				Sequence:       3,
				CharacterCount: 4,
				Metadata:       map[string]any{},
			},
		},
	}
}

func cloneChunkingResult(t *testing.T, result *chunking.Result) *chunking.Result {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var cloned chunking.Result
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return &cloned
}

func hashForTest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
