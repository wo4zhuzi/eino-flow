package indexing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"unicode/utf8"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

type configSnapshot struct {
	ParentMaxRunes       int    `json:"parent_max_runes,omitempty"`
	ChildMaxRunes        int    `json:"child_max_runes,omitempty"`
	MaxRunes             int    `json:"max_runes,omitempty"`
	MinRunes             int    `json:"min_runes,omitempty"`
	HeadingContext       string `json:"heading_context,omitempty"`
	EmbeddingInputPolicy string `json:"embedding_input_policy"`
}

var setLevelMetadataKeys = map[string]struct{}{
	"content_sha256":      {},
	"document_id":         {},
	"document_version_id": {},
	"source_name":         {},
	"source_uri":          {},
}

// MapBuild 把一个 Chunking Result 转换为可持久化的确定性构建数据。
func MapBuild(spec BuildSpec, result *chunking.Result) (BuildData, error) {
	normalized, modelKey, err := normalizeBuildSpec(spec)
	if err != nil {
		return BuildData{}, err
	}
	if result == nil || len(result.Chunks) == 0 {
		return BuildData{}, invalidBuild("Chunking Result 不能为空")
	}
	if strings.TrimSpace(result.Profile.Name) != normalized.Profile.Name ||
		strings.TrimSpace(result.Profile.Version) != normalized.Profile.Version {
		return BuildData{}, invalidBuild("Chunking Profile 与 Build Spec 不一致")
	}
	strategy := strings.TrimSpace(result.StrategyName)
	config, err := normalized.configFor(strategy)
	if err != nil {
		return BuildData{}, err
	}

	chunkByID := make(map[string]chunking.Chunk, len(result.Chunks))
	sequenceIDs := make(map[int]string, len(result.Chunks))
	for index, chunk := range result.Chunks {
		if err := validateChunkBase(index, chunk, chunkByID, sequenceIDs); err != nil {
			return BuildData{}, err
		}
		if strings.TrimSpace(chunk.DocumentID) != normalized.Document.ID {
			return BuildData{}, invalidBuild("Chunk %q 不属于 Build Spec 文档", chunk.ID)
		}
		chunkByID[chunk.ID] = chunk
		sequenceIDs[chunk.Sequence] = chunk.ID
	}

	chunks := make([]ChunkRecord, 0, len(result.Chunks))
	inputs := make([]EmbeddingInput, 0, len(result.Chunks))
	for _, chunk := range result.Chunks {
		embed, err := validateStrategyChunk(strategy, chunk, chunkByID)
		if err != nil {
			return BuildData{}, err
		}
		if err := validateAdjacency(chunk, chunkByID); err != nil {
			return BuildData{}, err
		}
		metadata, err := canonicalObject(chunk.Metadata)
		if err != nil {
			return BuildData{}, invalidBuild("Chunk %q Metadata 无法编码为 JSON: %v", chunk.ID, err)
		}
		candidate := indexstore.CandidateID{SetID: normalized.SetID, ChunkID: chunk.ID}
		chunks = append(chunks, ChunkRecord{
			Candidate:       candidate,
			Kind:            string(chunk.Kind),
			Level:           chunk.Level,
			ParentChunkID:   optionalString(chunk.ParentID),
			PreviousChunkID: optionalString(chunk.PreviousID),
			NextChunkID:     optionalString(chunk.NextID),
			Sequence:        chunk.Sequence,
			Content:         chunk.Content,
			CharacterCount:  chunk.CharacterCount,
			TokenCount:      chunk.TokenCount,
			SourceUnitIDs:   append([]string(nil), chunk.SourceUnitIDs...),
			Metadata:        metadata,
		})
		if !embed {
			continue
		}
		text, err := embeddingText(normalized, strategy, chunk)
		if err != nil {
			return BuildData{}, err
		}
		inputs = append(inputs, EmbeddingInput{
			Candidate:   candidate,
			ModelKey:    modelKey,
			Text:        text,
			InputSHA256: sha256Hex(text),
		})
	}
	if len(inputs) == 0 {
		return BuildData{}, invalidBuild("策略 %q 没有可向量化的 Chunk", strategy)
	}
	return BuildData{
		Set: SetRecord{
			ID:           normalized.SetID,
			Scope:        normalized.Scope,
			Document:     normalized.Document,
			StrategyName: strategy,
			Profile:      normalized.Profile,
			Config:       config,
		},
		Chunks:          chunks,
		EmbeddingInputs: inputs,
	}, nil
}

func normalizeBuildSpec(spec BuildSpec) (BuildSpec, indexstore.ModelKey, error) {
	spec.SetID = indexstore.SetID(strings.TrimSpace(string(spec.SetID)))
	spec.Scope.TenantID = strings.TrimSpace(spec.Scope.TenantID)
	spec.Scope.KnowledgeBaseID = strings.TrimSpace(spec.Scope.KnowledgeBaseID)
	spec.Document.ID = strings.TrimSpace(spec.Document.ID)
	spec.Document.SourceURI = strings.TrimSpace(spec.Document.SourceURI)
	spec.Document.SourceName = strings.TrimSpace(spec.Document.SourceName)
	spec.Document.Title = strings.TrimSpace(spec.Document.Title)
	spec.Document.ContentSHA256 = strings.TrimSpace(spec.Document.ContentSHA256)
	spec.Profile.Name = strings.TrimSpace(spec.Profile.Name)
	spec.Profile.Version = strings.TrimSpace(spec.Profile.Version)
	spec.Model.Model = strings.TrimSpace(spec.Model.Model)
	spec.Model.Distance = strings.TrimSpace(spec.Model.Distance)
	spec.Model.ConfigVersion = strings.TrimSpace(spec.Model.ConfigVersion)
	if spec.ParentChild != nil {
		config := *spec.ParentChild
		spec.ParentChild = &config
	}
	if spec.StructureAware != nil {
		config := *spec.StructureAware
		config.HeadingContext = strings.TrimSpace(config.HeadingContext)
		spec.StructureAware = &config
	}
	if spec.SetID == "" || spec.Scope.TenantID == "" || spec.Scope.KnowledgeBaseID == "" ||
		spec.Document.ID == "" || spec.Document.SourceName == "" ||
		spec.Profile.Name == "" || spec.Profile.Version == "" {
		return BuildSpec{}, "", invalidBuild("Set、作用域、文档和 Profile 标识不能为空")
	}
	if err := validateSourceURI(spec.Document.SourceURI); err != nil {
		return BuildSpec{}, "", err
	}
	if !isLowerSHA256(spec.Document.ContentSHA256) {
		return BuildSpec{}, "", invalidBuild("content_sha256 必须是 64 位小写十六进制")
	}
	modelKey, err := indexstore.NewModelKey(spec.Model)
	if err != nil {
		return BuildSpec{}, "", fmt.Errorf("%w: %w", ErrInvalidBuild, err)
	}
	return spec, modelKey, nil
}

func (spec BuildSpec) configFor(strategy string) (json.RawMessage, error) {
	var snapshot configSnapshot
	switch strategy {
	case parentchild.ParentChildStrategyName:
		if spec.ParentChild == nil || spec.StructureAware != nil ||
			spec.ParentChild.ParentMaxRunes < 1 || spec.ParentChild.ChildMaxRunes < 1 {
			return nil, invalidBuild("Parent-child 策略配置组合无效")
		}
		snapshot = configSnapshot{
			ParentMaxRunes:       spec.ParentChild.ParentMaxRunes,
			ChildMaxRunes:        spec.ParentChild.ChildMaxRunes,
			EmbeddingInputPolicy: EmbeddingInputPolicyV1,
		}
	case structureaware.StructureAwareStrategyName:
		if spec.StructureAware == nil || spec.ParentChild != nil ||
			spec.StructureAware.MaxRunes < 1 || spec.StructureAware.MinRunes < 1 ||
			spec.StructureAware.MinRunes > spec.StructureAware.MaxRunes {
			return nil, invalidBuild("Structure-aware 策略配置组合无效")
		}
		heading := strings.TrimSpace(spec.StructureAware.HeadingContext)
		if heading != string(structureaware.HeadingContextPrepend) &&
			heading != string(structureaware.HeadingContextMetadataOnly) {
			return nil, invalidBuild("Structure-aware heading_context 无效")
		}
		snapshot = configSnapshot{
			MaxRunes:             spec.StructureAware.MaxRunes,
			MinRunes:             spec.StructureAware.MinRunes,
			HeadingContext:       heading,
			EmbeddingInputPolicy: EmbeddingInputPolicyV1,
		}
	default:
		return nil, invalidBuild("不支持 Chunk 策略 %q", strategy)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, invalidBuild("编码配置快照: %v", err)
	}
	return encoded, nil
}

func validateChunkBase(
	index int,
	chunk chunking.Chunk,
	chunkByID map[string]chunking.Chunk,
	sequenceIDs map[int]string,
) error {
	if strings.TrimSpace(chunk.ID) == "" || strings.TrimSpace(chunk.DocumentID) == "" ||
		strings.TrimSpace(chunk.Content) == "" || chunk.Kind == "" {
		return invalidBuild("第 %d 个 Chunk 的标识、正文或类型为空", index+1)
	}
	if chunk.ID != strings.TrimSpace(chunk.ID) || chunk.DocumentID != strings.TrimSpace(chunk.DocumentID) {
		return invalidBuild("Chunk %q 的 Chunk ID 或 Document ID 含首尾空白", chunk.ID)
	}
	if _, exists := chunkByID[chunk.ID]; exists {
		return invalidBuild("Chunk ID %q 重复", chunk.ID)
	}
	if chunk.Sequence < 1 {
		return invalidBuild("Chunk %q Sequence 必须为正数", chunk.ID)
	}
	if existing, exists := sequenceIDs[chunk.Sequence]; exists {
		return invalidBuild("Chunk %q 与 %q 的 Sequence 重复", chunk.ID, existing)
	}
	if chunk.Level < 0 || chunk.CharacterCount < 1 || chunk.TokenCount < 0 {
		return invalidBuild("Chunk %q 的层级或长度统计无效", chunk.ID)
	}
	if chunk.CharacterCount != utf8.RuneCountInString(chunk.Content) {
		return invalidBuild("Chunk %q 的 character_count 与正文不一致", chunk.ID)
	}
	if len(chunk.SourceUnitIDs) == 0 {
		return invalidBuild("Chunk %q 没有来源单元", chunk.ID)
	}
	for _, sourceUnitID := range chunk.SourceUnitIDs {
		if strings.TrimSpace(sourceUnitID) == "" {
			return invalidBuild("Chunk %q 包含空来源单元", chunk.ID)
		}
	}
	return nil
}

func validateStrategyChunk(
	strategy string,
	chunk chunking.Chunk,
	chunkByID map[string]chunking.Chunk,
) (bool, error) {
	switch strategy {
	case parentchild.ParentChildStrategyName:
		switch chunk.Kind {
		case chunking.ChunkKindParent:
			if chunk.Level != 0 || chunk.ParentID != "" {
				return false, invalidBuild("Parent Chunk %q 的 Level 或 Parent 无效", chunk.ID)
			}
			return false, nil
		case chunking.ChunkKindChild:
			parent, exists := chunkByID[chunk.ParentID]
			if chunk.Level != 1 || chunk.ParentID == "" || !exists ||
				parent.Kind != chunking.ChunkKindParent || parent.Level != 0 ||
				parent.DocumentID != chunk.DocumentID {
				return false, invalidBuild("Child Chunk %q 的 Parent 关系无效", chunk.ID)
			}
			return true, nil
		default:
			return false, invalidBuild("Parent-child 策略包含非法 Chunk 类型 %q", chunk.Kind)
		}
	case structureaware.StructureAwareStrategyName:
		if chunk.Kind != structureaware.ChunkKindStructure || chunk.Level != 0 || chunk.ParentID != "" {
			return false, invalidBuild("Structure-aware Chunk %q 的类型、Level 或 Parent 无效", chunk.ID)
		}
		return true, nil
	default:
		return false, invalidBuild("不支持 Chunk 策略 %q", strategy)
	}
}

func validateAdjacency(chunk chunking.Chunk, chunkByID map[string]chunking.Chunk) error {
	for _, adjacent := range []struct {
		id      string
		forward bool
	}{
		{id: chunk.PreviousID, forward: false},
		{id: chunk.NextID, forward: true},
	} {
		if adjacent.id == "" {
			continue
		}
		other, exists := chunkByID[adjacent.id]
		if !exists || other.ID == chunk.ID || other.Kind != chunk.Kind || other.Level != chunk.Level ||
			other.DocumentID != chunk.DocumentID || other.ParentID != chunk.ParentID {
			return invalidBuild("Chunk %q 的相邻关系 %q 无效", chunk.ID, adjacent.id)
		}
		if adjacent.forward && other.PreviousID != chunk.ID {
			return invalidBuild("Chunk %q 的 Next 关系不是双向关系", chunk.ID)
		}
		if !adjacent.forward && other.NextID != chunk.ID {
			return invalidBuild("Chunk %q 的 Previous 关系不是双向关系", chunk.ID)
		}
	}
	return nil
}

func embeddingText(spec BuildSpec, strategy string, chunk chunking.Chunk) (string, error) {
	switch strategy {
	case parentchild.ParentChildStrategyName:
		if spec.Document.Title == "" {
			return chunk.Content, nil
		}
		return spec.Document.Title + "\n\n" + chunk.Content, nil
	case structureaware.StructureAwareStrategyName:
		if spec.StructureAware.HeadingContext == string(structureaware.HeadingContextPrepend) {
			return chunk.Content, nil
		}
		path, err := semanticPath(chunk.Metadata)
		if err != nil {
			return "", invalidBuild("Chunk %q 的 SemanticPath 无效: %v", chunk.ID, err)
		}
		if len(path) == 0 {
			return chunk.Content, nil
		}
		return strings.Join(path, " > ") + "\n\n" + chunk.Content, nil
	default:
		return "", invalidBuild("不支持 Chunk 策略 %q", strategy)
	}
}

func semanticPath(metadata map[string]any) ([]string, error) {
	value, exists := metadata[structureaware.MetadataStructureSemanticPath]
	if !exists || value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case []string:
		return normalizePath(typed)
	case []any:
		items := make([]string, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("第 %d 项不是字符串", index+1)
			}
			items[index] = text
		}
		return normalizePath(items)
	default:
		return nil, fmt.Errorf("必须是字符串数组")
	}
}

func normalizePath(path []string) ([]string, error) {
	normalized := make([]string, len(path))
	for index, item := range path {
		normalized[index] = strings.TrimSpace(item)
		if normalized[index] == "" {
			return nil, fmt.Errorf("第 %d 项为空", index+1)
		}
	}
	return normalized, nil
}

func canonicalObject(metadata map[string]any) (json.RawMessage, error) {
	if metadata == nil {
		return json.RawMessage(`{}`), nil
	}
	filtered := make(map[string]any, len(metadata))
	for key, value := range metadata {
		if _, setLevel := setLevelMetadataKeys[key]; setLevel {
			continue
		}
		filtered[key] = value
	}
	encoded, err := json.Marshal(filtered)
	if err != nil {
		return nil, err
	}
	if string(encoded) == "null" {
		return json.RawMessage(`{}`), nil
	}
	return append(json.RawMessage(nil), encoded...), nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func validateSourceURI(raw string) error {
	if raw == "" {
		return invalidBuild("source_uri 不能为空")
	}
	if filepath.IsAbs(raw) || (len(raw) >= 3 && raw[1] == ':' && (raw[2] == '\\' || raw[2] == '/')) {
		return invalidBuild("source_uri 不能是部署绝对路径")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return invalidBuild("source_uri 无效")
	}
	if parsed.Scheme == "" {
		return invalidBuild("source_uri 必须是带 Scheme 的逻辑 URI")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return invalidBuild("source_uri 不能包含认证信息、查询参数或 Fragment")
	}
	if strings.EqualFold(parsed.Scheme, "file") {
		return invalidBuild("source_uri 不能使用 file URI")
	}
	if (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) && parsed.Host == "" {
		return invalidBuild("HTTP source_uri 缺少 Host")
	}
	return nil
}

func isLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func invalidBuild(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidBuild, fmt.Sprintf(format, args...))
}
