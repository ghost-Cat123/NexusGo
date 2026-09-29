package eval

import "testing"

func TestBuildReportUsesHumanLabelsForNDCGAndHitRate(t *testing.T) {
	dataset := &EvalDataset{Queries: []EvalQuery{
		{ID: "labeled", Topic: "single", Difficulty: "easy", ExpectedMsgIDs: []int64{11}},
		{ID: "unlabeled", Topic: "single", Difficulty: "easy"},
	}}
	results := []StrategyResult{{
		QueryID:     "labeled",
		Strategy:    "RRF",
		ExpectedIDs: []int64{11},
		ReturnedIDs: []int64{11, 12},
		RecallAt1:   1,
		RecallAt3:   1,
		RecallAt5:   1,
		MRR:         1,
		Hit:         true,
	}}

	report := BuildReport(results, dataset)
	if report.LabeledQueries != 1 {
		t.Fatalf("labeled queries = %d, want 1", report.LabeledQueries)
	}
	if len(report.ByStrategy) != 1 || report.ByStrategy[0].NDCGAt5 != 1 || report.ByStrategy[0].HitRate != 1 {
		t.Fatalf("unexpected strategy report: %#v", report.ByStrategy)
	}
}
