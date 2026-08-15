package indexing

import (
	"errors"
	"testing"
)

func TestValidateBuildDataAcceptsMappedBuild(t *testing.T) {
	build, err := MapBuild(parentChildSpec(), parentChildResult())
	if err != nil {
		t.Fatalf("MapBuild() error = %v", err)
	}
	if err := ValidateBuildData(build); err != nil {
		t.Fatalf("ValidateBuildData() error = %v", err)
	}
}

func TestValidateBuildDataRejectsBrokenPersistenceContracts(t *testing.T) {
	base, err := MapBuild(parentChildSpec(), parentChildResult())
	if err != nil {
		t.Fatalf("MapBuild() error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*BuildData)
	}{
		{
			name: "unknown config field",
			mutate: func(build *BuildData) {
				build.Set.Config = []byte(`{"parent_max_runes":2000,"child_max_runes":500,"embedding_input_policy":"v1","unknown":true}`)
			},
		},
		{
			name: "cross set chunk",
			mutate: func(build *BuildData) {
				build.Chunks[0].Candidate.SetID = "another-set"
			},
		},
		{
			name: "missing parent",
			mutate: func(build *BuildData) {
				missing := "missing-parent"
				build.Chunks[1].ParentChunkID = &missing
			},
		},
		{
			name: "parent has embedding",
			mutate: func(build *BuildData) {
				input := build.EmbeddingInputs[0]
				input.Candidate.ChunkID = build.Chunks[0].Candidate.ChunkID
				build.EmbeddingInputs = append(build.EmbeddingInputs, input)
			},
		},
		{
			name: "input hash mismatch",
			mutate: func(build *BuildData) {
				build.EmbeddingInputs[0].InputSHA256 = hashForTest("different")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			build := cloneBuildDataForValidation(base)
			tt.mutate(&build)
			err := ValidateBuildData(build)
			if !errors.Is(err, ErrInvalidBuild) {
				t.Fatalf("ValidateBuildData() error = %v, want ErrInvalidBuild", err)
			}
		})
	}
}

func cloneBuildDataForValidation(source BuildData) BuildData {
	clone := source
	clone.Set.Config = append([]byte(nil), source.Set.Config...)
	clone.Chunks = make([]ChunkRecord, len(source.Chunks))
	for index, chunk := range source.Chunks {
		clone.Chunks[index] = chunk
		clone.Chunks[index].ParentChunkID = cloneValidationString(chunk.ParentChunkID)
		clone.Chunks[index].PreviousChunkID = cloneValidationString(chunk.PreviousChunkID)
		clone.Chunks[index].NextChunkID = cloneValidationString(chunk.NextChunkID)
		clone.Chunks[index].SourceUnitIDs = append([]string(nil), chunk.SourceUnitIDs...)
		clone.Chunks[index].Metadata = append([]byte(nil), chunk.Metadata...)
	}
	clone.EmbeddingInputs = append([]EmbeddingInput(nil), source.EmbeddingInputs...)
	return clone
}

func cloneValidationString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
