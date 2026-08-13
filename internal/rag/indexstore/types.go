// Package indexstore 定义 RAG 索引构建与检索共享的稳定领域标识。
package indexstore

// SetID 标识一次可重试、可发布的索引构建。
type SetID string

// ModelKey 稳定标识一个不可混用的向量模型空间。
type ModelKey string

// Scope 是索引构建与检索必须携带的可信隔离范围。
type Scope struct {
	TenantID        string
	KnowledgeBaseID string
}

// CandidateID 是不同召回通道共享的候选标识。
type CandidateID struct {
	SetID   SetID
	ChunkID string
}
