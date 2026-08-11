package indexing

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/schema"
	ingestion "github.com/wo4zhuzi/eino-document-ingestion"
)

func prepareForIndexing(result *ingestion.Result) (SourceInfo, *ingestion.Result, error) {
	documentHash := sha256.Sum256([]byte("source:" + result.Source.URI))
	source := SourceInfo{
		URI:         result.Source.URI,
		ResolvedURI: result.Source.ResolvedURI,
		FileName:    result.Source.FileName,
		Extension:   result.Source.Extension,
		MIMEType:    result.Source.MIMEType,
		SizeBytes:   result.Source.SizeBytes,
		SHA256:      result.Source.SHA256,
		DocumentID:  hex.EncodeToString(documentHash[:]),
		VersionID:   result.Source.SHA256,
	}

	documents := make([]*schema.Document, 0, len(result.Documents))
	for _, document := range result.Documents {
		if document == nil || strings.TrimSpace(document.Content) == "" {
			continue
		}
		documents = append(documents, &schema.Document{
			ID:       document.ID,
			Content:  document.Content,
			MetaData: cloneMetadata(document.MetaData),
		})
	}
	if len(documents) == 0 {
		return SourceInfo{}, nil, ingestion.ErrNoParsedContent
	}

	granularity := result.Parser.Output.Granularity
	if granularity == "" {
		granularity = ingestion.GranularityDocument
	}
	for index, document := range documents {
		unitIndex := index + 1
		document.MetaData["source_uri"] = source.URI
		document.MetaData["mime_type"] = source.MIMEType
		document.MetaData["document_id"] = source.DocumentID
		document.MetaData["document_version_id"] = source.VersionID
		document.MetaData["unit_type"] = string(granularity)
		document.MetaData["unit_index"] = unitIndex
		if strings.TrimSpace(document.ID) == "" {
			unitHash := sha256.Sum256([]byte(
				source.VersionID + ":" + strconv.Itoa(unitIndex) + ":" + document.Content,
			))
			document.ID = hex.EncodeToString(unitHash[:])
		}
	}
	return source, &ingestion.Result{
		Source:    result.Source,
		Parser:    result.Parser,
		Documents: documents,
	}, nil
}

func cloneMetadata(metadata map[string]any) map[string]any {
	cloned := make(map[string]any, len(metadata)+6)
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}
