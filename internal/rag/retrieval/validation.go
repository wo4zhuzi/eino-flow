package retrieval

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	appconfig "github.com/wo4zhuzi/eino-flow/internal/config"
	"github.com/wo4zhuzi/eino-flow/internal/embedding"
	"github.com/wo4zhuzi/eino-flow/internal/rag/indexstore"
)

func normalizeRequest(request Request) Request {
	request.RunID = strings.TrimSpace(request.RunID)
	request.Query = strings.TrimSpace(request.Query)
	request.Scope.TenantID = strings.TrimSpace(request.Scope.TenantID)
	request.Scope.KnowledgeBaseID = strings.TrimSpace(request.Scope.KnowledgeBaseID)
	return request
}

func validateRequest(request Request) error {
	if request.RunID == "" || request.Query == "" || request.Scope.TenantID == "" ||
		request.Scope.KnowledgeBaseID == "" {
		return fmt.Errorf("%w: RunID、查询文本和可信作用域不能为空", ErrInvalidRequest)
	}
	if !utf8.ValidString(request.Query) || utf8.RuneCountInString(request.Query) > MaxQueryRunes {
		return fmt.Errorf("%w: 查询文本必须是不超过 %d 个字符的有效 UTF-8", ErrInvalidRequest, MaxQueryRunes)
	}
	if request.TopK < 1 || request.TopK > MaxTopK {
		return fmt.Errorf("%w: TopK 必须在 1 到 %d 之间", ErrInvalidRequest, MaxTopK)
	}
	return nil
}

func validateEmbedding(results []embedding.Result, dimensions int) (embedding.Result, error) {
	if len(results) != 1 || results[0].TokenCount < 1 || len(results[0].Vector) != dimensions ||
		!finiteVector(results[0].Vector) {
		return embedding.Result{}, ErrInvalidEmbedding
	}
	return embedding.Result{
		Vector:     append([]float64(nil), results[0].Vector...),
		TokenCount: results[0].TokenCount,
	}, nil
}

// ValidateSearchRequest 防止 Store 的非工作流调用绕过检索输入边界。
func ValidateSearchRequest(request SearchRequest) error {
	if request.Scope.TenantID == "" || request.Scope.TenantID != strings.TrimSpace(request.Scope.TenantID) ||
		request.Scope.KnowledgeBaseID == "" ||
		request.Scope.KnowledgeBaseID != strings.TrimSpace(request.Scope.KnowledgeBaseID) ||
		request.ModelKey == "" || string(request.ModelKey) != strings.TrimSpace(string(request.ModelKey)) ||
		request.Limit < 1 || request.Limit > MaxTopK ||
		len(request.Vector) != appconfig.RequiredEmbeddingDimensions || !finiteVector(request.Vector) {
		return ErrInvalidRequest
	}
	return nil
}

// ValidateEvidence 校验 Store 返回的数量、字段、唯一性和确定性排序。
func ValidateEvidence(evidence []Evidence, limit int) error {
	if limit < 1 || limit > MaxTopK || len(evidence) > limit {
		return ErrInvalidEvidence
	}
	seen := make(map[string]struct{}, len(evidence))
	for index, item := range evidence {
		if err := validateEvidenceItem(item); err != nil {
			return fmt.Errorf("%w: 第 %d 条证据: %v", ErrInvalidEvidence, index+1, err)
		}
		key := string(item.Candidate.SetID) + "\x00" + item.Candidate.ChunkID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: 候选标识重复", ErrInvalidEvidence)
		}
		seen[key] = struct{}{}
		if index > 0 && evidenceLess(item, evidence[index-1]) {
			return fmt.Errorf("%w: 证据顺序不稳定", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateEvidenceItem(item Evidence) error {
	if item.Candidate.SetID == "" || string(item.Candidate.SetID) != strings.TrimSpace(string(item.Candidate.SetID)) ||
		item.Candidate.ChunkID == "" || item.Candidate.ChunkID != strings.TrimSpace(item.Candidate.ChunkID) ||
		strings.TrimSpace(item.DocumentID) == "" || strings.TrimSpace(item.SourceURI) == "" ||
		strings.TrimSpace(item.SourceName) == "" || strings.TrimSpace(item.Kind) == "" ||
		item.Sequence < 1 || strings.TrimSpace(item.Content) == "" || item.TokenCount < 0 ||
		len(item.SourceUnitIDs) == 0 || math.IsNaN(item.CosineDistance) ||
		math.IsInf(item.CosineDistance, 0) || item.CosineDistance < 0 || item.CosineDistance > 2 {
		return ErrInvalidEvidence
	}
	for _, sourceUnitID := range item.SourceUnitIDs {
		if strings.TrimSpace(sourceUnitID) == "" {
			return ErrInvalidEvidence
		}
	}
	var metadata map[string]json.RawMessage
	if len(item.Metadata) == 0 || json.Unmarshal(item.Metadata, &metadata) != nil || metadata == nil {
		return ErrInvalidEvidence
	}
	return nil
}

func evidenceLess(left, right Evidence) bool {
	if left.CosineDistance != right.CosineDistance {
		return left.CosineDistance < right.CosineDistance
	}
	if left.Candidate.SetID != right.Candidate.SetID {
		return left.Candidate.SetID < right.Candidate.SetID
	}
	return left.Candidate.ChunkID < right.Candidate.ChunkID
}

func cloneEvidence(items []Evidence) []Evidence {
	cloned := make([]Evidence, len(items))
	for index, item := range items {
		cloned[index] = item
		cloned[index].SourceUnitIDs = append([]string(nil), item.SourceUnitIDs...)
		cloned[index].Metadata = append(json.RawMessage(nil), item.Metadata...)
	}
	return cloned
}

func finiteVector(vector []float64) bool {
	for _, value := range vector {
		if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(float32(value)), 0) {
			return false
		}
	}
	return true
}

func validModelProfile(profile indexstore.ModelProfile) bool {
	return profile.Model != "" && profile.Dimensions == appconfig.RequiredEmbeddingDimensions &&
		profile.Distance == indexstore.DistanceCosine && profile.ConfigVersion != ""
}
