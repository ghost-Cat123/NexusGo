package retrieval

import (
	"strings"

	"github.com/cloudwego/eino/schema"
)

// MinMessageSemanticScore rejects weak vector matches before source-message
// lookup. A score below this gate is treated as no semantic candidate so the
// caller may fall back to FULLTEXT or return no result.
// Calibrated against the frozen v1.0 simulated chat-history evaluation set.
// Re-evaluate this value when the dataset or embedding model changes.
const MinMessageSemanticScore = 0.67

func FilterDocumentsByScore(documents []*schema.Document, minScore float64) []*schema.Document {
	filtered := make([]*schema.Document, 0, len(documents))
	for _, document := range documents {
		if document != nil && document.Score() >= minScore {
			filtered = append(filtered, document)
		}
	}
	return filtered
}

// HasContentKeywordCoverage verifies that the semantic candidate set contains
// direct evidence for every keyword supplied by the Agent. It prevents a
// semantically similar message from answering an unsupported attribute question.
func HasContentKeywordCoverage(contents []string, keywords []string) bool {
	if len(contents) == 0 {
		return false
	}

	for _, keyword := range keywords {
		term := strings.ToLower(strings.TrimSpace(keyword))
		if term == "" {
			continue
		}

		matched := false
		for _, content := range contents {
			if strings.Contains(strings.ToLower(content), term) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
