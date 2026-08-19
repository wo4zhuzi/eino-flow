// Package indexstore 定义 RAG 索引构建与检索共享的稳定领域标识。
package indexstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// DistanceCosine 表示模型空间使用余弦距离。
	DistanceCosine = "cosine"
)

var (
	// ErrInvalidModelProfile 表示模型空间缺少生成稳定 Key 所需的属性。
	ErrInvalidModelProfile = errors.New("模型 Profile 无效")
)

// SetID 标识一次可重试、可发布的索引构建。
type SetID string

// ModelKey 稳定标识一个不可混用的向量模型空间。
type ModelKey string

// ModelProfile 描述影响向量兼容性的全部稳定模型属性。
type ModelProfile struct {
	Model         string
	Dimensions    int
	Distance      string
	ConfigVersion string
}

// NewModelKey 规范化模型 Profile 并生成跨索引与检索稳定的模型空间标识。
func NewModelKey(profile ModelProfile) (ModelKey, error) {
	profile.Model = strings.TrimSpace(profile.Model)
	profile.Distance = strings.TrimSpace(profile.Distance)
	profile.ConfigVersion = strings.TrimSpace(profile.ConfigVersion)
	if profile.Model == "" || profile.Dimensions < 1 || profile.Distance == "" || profile.ConfigVersion == "" {
		return "", fmt.Errorf("%w: 模型、维度、距离算法和配置版本不能为空", ErrInvalidModelProfile)
	}
	if profile.Distance != DistanceCosine {
		return "", fmt.Errorf("%w: 不支持距离算法 %q", ErrInvalidModelProfile, profile.Distance)
	}
	canonical := profile.Model + "\x00" + strconv.Itoa(profile.Dimensions) + "\x00" +
		profile.Distance + "\x00" + profile.ConfigVersion
	digest := sha256.Sum256([]byte(canonical))
	return ModelKey(fmt.Sprintf("%s:%x", profile.Model, digest)), nil
}

// Scope 是索引构建与检索必须携带的可信隔离范围。
type Scope struct {
	TenantID        string `json:"tenant_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
}

// CandidateID 是不同召回通道共享的候选标识。
type CandidateID struct {
	SetID   SetID  `json:"set_id"`
	ChunkID string `json:"chunk_id"`
}
