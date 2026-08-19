package indexstore

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestNewModelKeyKeepsPublishedFormatStable(t *testing.T) {
	profile := ModelProfile{
		Model:         "text-embedding-v4",
		Dimensions:    1536,
		Distance:      DistanceCosine,
		ConfigVersion: "model-v1",
	}
	key, err := NewModelKey(profile)
	if err != nil {
		t.Fatalf("NewModelKey() error = %v", err)
	}
	const expected = ModelKey("text-embedding-v4:b9fb56c67146c5f8f5a6d46db6e828d3a6ef9f00a4d835bcd6c4910cd8478005")
	if key != expected {
		t.Fatalf("NewModelKey() = %q, want %q", key, expected)
	}

	profile.Model = " text-embedding-v4 "
	profile.Distance = " cosine "
	profile.ConfigVersion = " model-v1 "
	normalized, err := NewModelKey(profile)
	if err != nil {
		t.Fatalf("NewModelKey(normalized) error = %v", err)
	}
	if normalized != expected {
		t.Fatalf("NewModelKey(normalized) = %q, want %q", normalized, expected)
	}
}

func TestNewModelKeyRejectsInvalidProfile(t *testing.T) {
	tests := []ModelProfile{
		{},
		{Model: "model", Dimensions: 1536, Distance: "l2", ConfigVersion: "v1"},
	}
	for _, profile := range tests {
		if _, err := NewModelKey(profile); !errors.Is(err, ErrInvalidModelProfile) {
			t.Fatalf("NewModelKey(%#v) error = %v, want ErrInvalidModelProfile", profile, err)
		}
	}
}

func TestSharedIdentifiersUseStableJSONFields(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Scope     Scope       `json:"scope"`
		Candidate CandidateID `json:"candidate"`
	}{
		Scope:     Scope{TenantID: "tenant-1", KnowledgeBaseID: "kb-1"},
		Candidate: CandidateID{SetID: "set-1", ChunkID: "chunk-1"},
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	const expected = `{"scope":{"tenant_id":"tenant-1","knowledge_base_id":"kb-1"},"candidate":{"set_id":"set-1","chunk_id":"chunk-1"}}`
	if string(encoded) != expected {
		t.Fatalf("json.Marshal() = %s, want %s", encoded, expected)
	}
}
