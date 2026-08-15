package indexing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	chunking "github.com/wo4zhuzi/eino-document-chunking"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/parentchild"
	"github.com/wo4zhuzi/eino-document-chunking/strategy/structureaware"
)

// ValidateBuildData 校验非标准写入方提交的完整构建聚合，防止绕过 MapBuild 的领域约束。
func ValidateBuildData(build BuildData) error {
	if err := validateSetRecord(build.Set); err != nil {
		return err
	}
	if len(build.Chunks) == 0 || len(build.EmbeddingInputs) == 0 {
		return invalidBuild("Chunk 和 Embedding 输入不能为空")
	}

	chunks := make(map[string]ChunkRecord, len(build.Chunks))
	sequences := make(map[int]string, len(build.Chunks))
	for index, chunk := range build.Chunks {
		if chunk.Candidate.SetID != build.Set.ID || strings.TrimSpace(chunk.Candidate.ChunkID) == "" ||
			chunk.Candidate.ChunkID != strings.TrimSpace(chunk.Candidate.ChunkID) {
			return invalidBuild("第 %d 个 Chunk 的候选标识无效", index+1)
		}
		if _, exists := chunks[chunk.Candidate.ChunkID]; exists {
			return invalidBuild("Chunk ID %q 重复", chunk.Candidate.ChunkID)
		}
		if existing, exists := sequences[chunk.Sequence]; exists {
			return invalidBuild("Chunk %q 与 %q 的 Sequence 重复", chunk.Candidate.ChunkID, existing)
		}
		if strings.TrimSpace(chunk.Kind) == "" || chunk.Level < 0 || chunk.Sequence < 1 ||
			strings.TrimSpace(chunk.Content) == "" || chunk.CharacterCount < 1 || chunk.TokenCount < 0 {
			return invalidBuild("Chunk %q 的类型、层级、序号、正文或长度统计无效", chunk.Candidate.ChunkID)
		}
		if chunk.CharacterCount != utf8.RuneCountInString(chunk.Content) {
			return invalidBuild("Chunk %q 的 character_count 与正文不一致", chunk.Candidate.ChunkID)
		}
		if len(chunk.SourceUnitIDs) == 0 {
			return invalidBuild("Chunk %q 没有来源单元", chunk.Candidate.ChunkID)
		}
		for _, sourceUnitID := range chunk.SourceUnitIDs {
			if strings.TrimSpace(sourceUnitID) == "" {
				return invalidBuild("Chunk %q 包含空来源单元", chunk.Candidate.ChunkID)
			}
		}
		if err := validateJSONObject(chunk.Metadata); err != nil {
			return invalidBuild("Chunk %q Metadata 无效: %v", chunk.Candidate.ChunkID, err)
		}
		for _, relation := range []*string{chunk.ParentChunkID, chunk.PreviousChunkID, chunk.NextChunkID} {
			if relation != nil && (strings.TrimSpace(*relation) == "" || *relation == chunk.Candidate.ChunkID) {
				return invalidBuild("Chunk %q 包含无效关系", chunk.Candidate.ChunkID)
			}
		}
		chunks[chunk.Candidate.ChunkID] = chunk
		sequences[chunk.Sequence] = chunk.Candidate.ChunkID
	}

	expectedInputs := make(map[string]struct{}, len(build.Chunks))
	for _, chunk := range build.Chunks {
		embed, err := validateStoredChunk(build.Set.StrategyName, chunk, chunks)
		if err != nil {
			return err
		}
		if embed {
			expectedInputs[chunk.Candidate.ChunkID] = struct{}{}
		}
	}

	var modelKey string
	seenInputs := make(map[string]struct{}, len(build.EmbeddingInputs))
	for index, input := range build.EmbeddingInputs {
		if input.Candidate.SetID != build.Set.ID {
			return invalidBuild("第 %d 个 Embedding 输入不属于目标 Set", index+1)
		}
		if _, exists := chunks[input.Candidate.ChunkID]; !exists {
			return invalidBuild("Embedding 输入引用不存在的 Chunk %q", input.Candidate.ChunkID)
		}
		if _, expected := expectedInputs[input.Candidate.ChunkID]; !expected {
			return invalidBuild("Chunk %q 不应生成 Embedding", input.Candidate.ChunkID)
		}
		if strings.TrimSpace(string(input.ModelKey)) == "" || strings.TrimSpace(input.Text) == "" ||
			!isLowerSHA256(input.InputSHA256) || input.InputSHA256 != sha256Hex(input.Text) {
			return invalidBuild("Chunk %q 的 Embedding 输入无效", input.Candidate.ChunkID)
		}
		if modelKey == "" {
			modelKey = string(input.ModelKey)
		} else if string(input.ModelKey) != modelKey {
			return invalidBuild("一次 V1 构建不能混用多个模型空间")
		}
		key := input.Candidate.ChunkID + "\x00" + string(input.ModelKey)
		if _, exists := seenInputs[key]; exists {
			return invalidBuild("Chunk %q 的 Embedding 输入重复", input.Candidate.ChunkID)
		}
		seenInputs[key] = struct{}{}
	}
	if len(seenInputs) != len(expectedInputs) {
		return invalidBuild("应向量化 Chunk 与 Embedding 输入不完整")
	}
	return nil
}

func validateSetRecord(set SetRecord) error {
	if strings.TrimSpace(string(set.ID)) == "" || string(set.ID) != strings.TrimSpace(string(set.ID)) ||
		strings.TrimSpace(set.Scope.TenantID) == "" || set.Scope.TenantID != strings.TrimSpace(set.Scope.TenantID) ||
		strings.TrimSpace(set.Scope.KnowledgeBaseID) == "" || set.Scope.KnowledgeBaseID != strings.TrimSpace(set.Scope.KnowledgeBaseID) ||
		strings.TrimSpace(set.Document.ID) == "" || set.Document.ID != strings.TrimSpace(set.Document.ID) ||
		strings.TrimSpace(set.Document.SourceName) == "" || set.Document.SourceName != strings.TrimSpace(set.Document.SourceName) ||
		strings.TrimSpace(set.StrategyName) == "" || set.StrategyName != strings.TrimSpace(set.StrategyName) ||
		strings.TrimSpace(set.Profile.Name) == "" || set.Profile.Name != strings.TrimSpace(set.Profile.Name) ||
		strings.TrimSpace(set.Profile.Version) == "" || set.Profile.Version != strings.TrimSpace(set.Profile.Version) {
		return invalidBuild("Set、作用域、文档、策略和 Profile 标识无效")
	}
	if err := validateSourceURI(set.Document.SourceURI); err != nil {
		return err
	}
	if !isLowerSHA256(set.Document.ContentSHA256) {
		return invalidBuild("content_sha256 必须是 64 位小写十六进制")
	}
	snapshot, err := decodeConfigSnapshot(set.Config)
	if err != nil {
		return invalidBuild("配置快照无效: %v", err)
	}
	switch set.StrategyName {
	case parentchild.ParentChildStrategyName:
		if snapshot.ParentMaxRunes < 1 || snapshot.ChildMaxRunes < 1 || snapshot.MaxRunes != 0 ||
			snapshot.MinRunes != 0 || snapshot.HeadingContext != "" ||
			snapshot.EmbeddingInputPolicy != EmbeddingInputPolicyV1 {
			return invalidBuild("Parent-child 配置快照无效")
		}
	case structureaware.StructureAwareStrategyName:
		if snapshot.ParentMaxRunes != 0 || snapshot.ChildMaxRunes != 0 || snapshot.MaxRunes < 1 ||
			snapshot.MinRunes < 1 || snapshot.MinRunes > snapshot.MaxRunes ||
			(snapshot.HeadingContext != string(structureaware.HeadingContextPrepend) &&
				snapshot.HeadingContext != string(structureaware.HeadingContextMetadataOnly)) ||
			snapshot.EmbeddingInputPolicy != EmbeddingInputPolicyV1 {
			return invalidBuild("Structure-aware 配置快照无效")
		}
	default:
		return invalidBuild("不支持 Chunk 策略 %q", set.StrategyName)
	}
	return nil
}

func validateStoredChunk(strategy string, chunk ChunkRecord, chunks map[string]ChunkRecord) (bool, error) {
	chunkID := chunk.Candidate.ChunkID
	parentID := dereference(chunk.ParentChunkID)
	switch strategy {
	case parentchild.ParentChildStrategyName:
		switch chunk.Kind {
		case string(chunking.ChunkKindParent):
			if chunk.Level != 0 || parentID != "" {
				return false, invalidBuild("Parent Chunk %q 的 Level 或 Parent 无效", chunkID)
			}
		case string(chunking.ChunkKindChild):
			parent, exists := chunks[parentID]
			if chunk.Level != 1 || !exists || parent.Kind != string(chunking.ChunkKindParent) ||
				parent.Level != 0 || parent.ParentChunkID != nil {
				return false, invalidBuild("Child Chunk %q 的 Parent 关系无效", chunkID)
			}
		default:
			return false, invalidBuild("Parent-child 策略包含非法 Chunk 类型 %q", chunk.Kind)
		}
	case structureaware.StructureAwareStrategyName:
		if chunk.Kind != string(structureaware.ChunkKindStructure) || chunk.Level != 0 || parentID != "" {
			return false, invalidBuild("Structure-aware Chunk %q 的类型、Level 或 Parent 无效", chunkID)
		}
	default:
		return false, invalidBuild("不支持 Chunk 策略 %q", strategy)
	}

	for _, adjacent := range []struct {
		id      string
		forward bool
	}{
		{id: dereference(chunk.PreviousChunkID)},
		{id: dereference(chunk.NextChunkID), forward: true},
	} {
		if adjacent.id == "" {
			continue
		}
		other, exists := chunks[adjacent.id]
		if !exists || other.Kind != chunk.Kind || other.Level != chunk.Level ||
			dereference(other.ParentChunkID) != parentID {
			return false, invalidBuild("Chunk %q 的相邻关系 %q 无效", chunkID, adjacent.id)
		}
		if adjacent.forward && dereference(other.PreviousChunkID) != chunkID {
			return false, invalidBuild("Chunk %q 的 Next 关系不是双向关系", chunkID)
		}
		if !adjacent.forward && dereference(other.NextChunkID) != chunkID {
			return false, invalidBuild("Chunk %q 的 Previous 关系不是双向关系", chunkID)
		}
	}
	return chunk.Kind != string(chunking.ChunkKindParent), nil
}

func decodeConfigSnapshot(raw json.RawMessage) (configSnapshot, error) {
	if err := validateJSONObject(raw); err != nil {
		return configSnapshot{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var snapshot configSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return configSnapshot{}, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return configSnapshot{}, err
	}
	return snapshot, nil
}

func validateJSONObject(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return fmt.Errorf("必须是有效 JSON 对象")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return fmt.Errorf("必须是有效 JSON 对象")
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("包含多余 JSON 值")
		}
		return err
	}
	return nil
}

func dereference(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
