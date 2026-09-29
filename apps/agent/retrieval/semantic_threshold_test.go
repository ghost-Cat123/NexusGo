package retrieval

import (
	"testing"
)

func TestHasKeywordCoverage(t *testing.T) {
	contents := []string{
		"周五晚上七点以后我在家",
		"投影仪用完再送回来",
	}

	if !HasContentKeywordCoverage(contents, []string{"投影仪", "周五", "七点"}) {
		t.Fatal("keywords split across candidates should be covered")
	}
	if HasContentKeywordCoverage(contents, []string{"投影仪", "价格"}) {
		t.Fatal("missing attribute must not be treated as evidence")
	}
}
