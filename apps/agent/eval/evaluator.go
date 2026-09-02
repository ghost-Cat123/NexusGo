package eval

import (
	"math"
	"sort"
)

// SearchResult 搜索返回的一条消息
type SearchResult struct {
	MsgID  int64
	Score  float64 // Milvus 相似度分，FULLTEXT 为 0
	Source string  // "fulltext" | "milvus"
}

// StrategyResult 单条查询 × 单个策略的结果
type StrategyResult struct {
	QueryID    string
	Strategy   string
	ReturnedIDs []int64
	RecallAt1  float64
	RecallAt3  float64
	RecallAt5  float64
	MRR        float64
	Hit        bool
}

// computeRecall 计算 Recall@K
func computeRecall(returnedIDs, expectedIDs []int64, k int) float64 {
	if len(expectedIDs) == 0 {
		// 没有标注预期结果 → 跳过
		return -1
	}
	expected := toSet(expectedIDs)
	limit := k
	if len(returnedIDs) < limit {
		limit = len(returnedIDs)
	}
	hit := 0
	for i := 0; i < limit; i++ {
		if expected[returnedIDs[i]] {
			hit++
		}
	}
	return float64(hit) / float64(len(expectedIDs))
}

// computeMRR 计算 Mean Reciprocal Rank
func computeMRR(returnedIDs, expectedIDs []int64) float64 {
	if len(expectedIDs) == 0 {
		return -1
	}
	expected := toSet(expectedIDs)
	for i, id := range returnedIDs {
		if expected[id] {
			return 1.0 / float64(i+1)
		}
	}
	return 0
}

// computeNDCG@K 计算 NDCG@K: 相关性排名评估
func computeNDCG(returnedIDs, expectedIDs []int64, k int) float64 {
	if len(expectedIDs) == 0 {
		return -1
	}
	expected := toSet(expectedIDs)
	limit := k
	if len(returnedIDs) < limit {
		limit = len(returnedIDs)
	}

	// DCG
	var dcg float64
	for i := 0; i < limit; i++ {
		if expected[returnedIDs[i]] {
			dcg += 1.0 / math.Log2(float64(i+2)) // i+2 because log2(1)=0
		}
	}

	// IDCG (ideal) — 所有命中的排在最前面
	idealCount := len(expectedIDs)
	if idealCount > limit {
		idealCount = limit
	}
	var idcg float64
	for i := 0; i < idealCount; i++ {
		idcg += 1.0 / math.Log2(float64(i+2))
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

func toSet(ids []int64) map[int64]bool {
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// GroupByStrategy 按策略分组统计
func GroupByStrategy(results []StrategyResult) map[string][]StrategyResult {
	groups := make(map[string][]StrategyResult)
	for _, r := range results {
		groups[r.Strategy] = append(groups[r.Strategy], r)
	}
	return groups
}

// GroupByTopic 按主题分组统计
func GroupByTopic(results []StrategyResult, dataset *EvalDataset) map[string][]StrategyResult {
	queryTopics := make(map[string]string)
	for _, q := range dataset.Queries {
		queryTopics[q.ID] = q.Topic
	}
	groups := make(map[string][]StrategyResult)
	for _, r := range results {
		topic := queryTopics[r.QueryID]
		groups[topic] = append(groups[topic], r)
	}
	return groups
}

// GroupByDifficulty 按难度分组统计
func GroupByDifficulty(results []StrategyResult, dataset *EvalDataset) map[string][]StrategyResult {
	queryDiff := make(map[string]string)
	for _, q := range dataset.Queries {
		queryDiff[q.ID] = q.Difficulty
	}
	groups := make(map[string][]StrategyResult)
	for _, r := range results {
		diff := queryDiff[r.QueryID]
		groups[diff] = append(groups[diff], r)
	}
	return groups
}

// average 计算平均值
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// median 计算中位数
func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}
