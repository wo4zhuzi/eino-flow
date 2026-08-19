package main

import (
	"errors"
	"testing"

	"github.com/wo4zhuzi/eino-flow/internal/rag/retrieval"
)

func TestParseArguments(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantQuery string
		wantTopK  int
		wantErr   error
	}{
		{name: "defaults", args: []string{"rag-retrieve-dev"}, wantQuery: defaultQuery, wantTopK: defaultTopK},
		{name: "explicit query", args: []string{"rag-retrieve-dev", "  如何切分 Markdown？  "}, wantQuery: "如何切分 Markdown？", wantTopK: defaultTopK},
		{name: "explicit top k", args: []string{"rag-retrieve-dev", "问题", " 20 "}, wantQuery: "问题", wantTopK: 20},
		{name: "empty query", args: []string{"rag-retrieve-dev", " "}, wantErr: retrieval.ErrInvalidRequest},
		{name: "non numeric top k", args: []string{"rag-retrieve-dev", "问题", "three"}, wantErr: retrieval.ErrInvalidRequest},
		{name: "zero top k", args: []string{"rag-retrieve-dev", "问题", "0"}, wantErr: retrieval.ErrInvalidRequest},
		{name: "top k too large", args: []string{"rag-retrieve-dev", "问题", "21"}, wantErr: retrieval.ErrInvalidRequest},
		{name: "too many arguments", args: []string{"rag-retrieve-dev", "问题", "3", "extra"}, wantErr: retrieval.ErrInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, topK, err := parseArguments(test.args)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("parseArguments() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArguments() error = %v", err)
			}
			if query != test.wantQuery || topK != test.wantTopK {
				t.Fatalf("parseArguments() = (%q, %d), want (%q, %d)", query, topK, test.wantQuery, test.wantTopK)
			}
		})
	}
}
