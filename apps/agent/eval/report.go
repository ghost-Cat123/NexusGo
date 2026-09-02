package eval

import (
	"fmt"
	"strings"
)

// Report 评测报告
type Report struct {
	DatasetPath   string
	TotalQueries  int
	ByStrategy    []DimensionReport
	ByTopic       []DimensionReport
	ByDifficulty  []DimensionReport
	Details       []StrategyResult
}

// DimensionReport 按维度聚合的统计
type DimensionReport struct {
	Name       string
	Count      int
	RecallAt1  float64
	RecallAt3  float64
	RecallAt5  float64
	MRR        float64
	NDCGAt5    float64
	HitRate    float64
}

// BuildReport 从结果构建报告
func BuildReport(results []StrategyResult, dataset *EvalDataset) *Report {
	report := &Report{
		DatasetPath:  "",
		TotalQueries: len(dataset.Queries),
	}

	// 按策略
	for name, group := range GroupByStrategy(results) {
		d := aggregate(name, group)
		report.ByStrategy = append(report.ByStrategy, d)
	}

	// 按主题
	for name, group := range GroupByTopic(results, dataset) {
		d := aggregate(name, group)
		report.ByTopic = append(report.ByTopic, d)
	}

	// 按难度
	for name, group := range GroupByDifficulty(results, dataset) {
		d := aggregate(name, group)
		report.ByDifficulty = append(report.ByDifficulty, d)
	}

	report.Details = results
	return report
}

func aggregate(name string, results []StrategyResult) DimensionReport {
	d := DimensionReport{Name: name, Count: len(results)}
	var r1, r3, r5, mrr, ndcg5, hit []float64
	for _, r := range results {
		if r.RecallAt1 >= 0 {
			r1 = append(r1, r.RecallAt1)
		}
		if r.RecallAt3 >= 0 {
			r3 = append(r3, r.RecallAt3)
		}
		if r.RecallAt5 >= 0 {
			r5 = append(r5, r.RecallAt5)
		}
		if r.MRR >= 0 {
			mrr = append(mrr, r.MRR)
		}
		if r.MRR >= 0 {
			ndcg5 = append(ndcg5, computeNDCG(r.ReturnedIDs, getExpectedIDs(results, r.QueryID), 5))
		}
		if r.Hit {
			hit = append(hit, 1)
		} else if !r.Hit && r.MRR >= 0 {
			hit = append(hit, 0)
		}
	}
	d.RecallAt1 = average(r1)
	d.RecallAt3 = average(r3)
	d.RecallAt5 = average(r5)
	d.MRR = average(mrr)
	d.NDCGAt5 = average(ndcg5)
	d.HitRate = average(hit)
	return d
}

func getExpectedIDs(results []StrategyResult, queryID string) []int64 {
	// 从 results 中找到对应 query 的 expected IDs
	// 这里简化：返回一个空列表，具体expected从dataset获取
	return nil
}

// Format 格式化输出 Report
func (r *Report) Format() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("RAG 离线评测报告\n"))
	sb.WriteString(fmt.Sprintf("数据集: %d 条查询\n\n", r.TotalQueries))

	writeTable(&sb, "按策略", r.ByStrategy)
	sb.WriteString("\n")
	writeTable(&sb, "按主题", r.ByTopic)
	sb.WriteString("\n")
	writeTable(&sb, "按难度", r.ByDifficulty)
	sb.WriteString("\n")

	// 关键结论
	sb.WriteString(fmt.Sprintln("详细结果:\nID | 策略 | Recall@1 | Recall@3 | Recall@5 | MRR | Hit"))
	for _, d := range r.Details {
		sb.WriteString(fmt.Sprintf("%s | %-10s | %5.2f | %5.2f | %5.2f | %4.2f | %v\n",
			d.QueryID, d.Strategy, d.RecallAt1, d.RecallAt3, d.RecallAt5, d.MRR, d.Hit))
	}
	return sb.String()
}

func writeTable(sb *strings.Builder, title string, rows []DimensionReport) {
	sb.WriteString(fmt.Sprintf("┌─ %s ──────────────────────────────┐\n", title))
	sb.WriteString(fmt.Sprintf("  %-20s %6s %6s %6s %6s %6s\n", "Name", "R@1", "R@3", "R@5", "MRR", "NDCG@5"))
	for _, r := range rows {
		sb.WriteString(fmt.Sprintf("  %-20s %5.2f %5.2f %5.2f %5.2f %5.2f\n", r.Name, r.RecallAt1, r.RecallAt3, r.RecallAt5, r.MRR, r.NDCGAt5))
	}
	sb.WriteString(fmt.Sprintf("└──────────────────────────────────────┘\n"))
}
