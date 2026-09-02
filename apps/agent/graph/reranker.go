package graph

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/schema"
)

// ── LLMReranker：默认实现，使用 ChatModel 对候选文档逐一打分 ──

const rerankSystemPrompt = `你是一个检索重排序助手。根据用户查询，对以下文档片段逐一评分。
只输出 JSON 数组，每个元素包含 id（序号，从 0 开始）和 score（0-10 整数，10=完全相关）。
不要输出其他内容。

示例输出格式：[{"id":0,"score":8},{"id":1,"score":3}]`

// LLMReranker 基于 LLM 进行相关性打分重排
// 优点：精准、不需额外模型；缺点：延迟较高、消耗 token
type LLMReranker struct {
	cm ChatModel
}

// NewLLMReranker 创建 LLM 重排器
func NewLLMReranker(cm ChatModel) *LLMReranker {
	return &LLMReranker{cm: cm}
}

// Rerank 实现 Reranker 接口
func (r *LLMReranker) Rerank(ctx context.Context, query string, docs []Document) []Document {
	if len(docs) <= 1 {
		return docs
	}

	prompt := r.buildPrompt(query, docs)
	resp, err := r.cm.Generate(ctx, []*schema.Message{
		schema.SystemMessage(rerankSystemPrompt),
		schema.UserMessage(prompt),
	})
	if err != nil {
		// 重排失败不中断流程，返回原始顺序
		return docs
	}

	scores := parseScores(resp.Content, len(docs))
	return r.sortByScore(docs, scores)
}

func (r *LLMReranker) buildPrompt(query string, docs []Document) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("用户查询: %s\n\n候选文档:\n", query))
	for i, d := range docs {
		content := d.Content
		if len([]rune(content)) > 200 {
			content = string([]rune(content)[:200]) + "..."
		}
		sb.WriteString(fmt.Sprintf("[%d] %s\n", i, content))
	}
	sb.WriteString("\n请对每条文档与查询的相关性打分 (0-10)。")
	return sb.String()
}

func (r *LLMReranker) sortByScore(docs []Document, scores []int) []Document {
	type indexed struct{ idx int; doc Document; score int }
	items := make([]indexed, len(docs))
	for i, d := range docs {
		items[i] = indexed{idx: i, doc: d, score: scores[i]}
	}
	// 冒泡排序（候选数通常 ≤ 20，不需要快排）
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[j].score > items[i].score {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	result := make([]Document, len(items))
	for i, item := range items {
		result[i] = item.doc
	}
	return result
}

// ── 未来可扩展的重排实现（示例注释）──
//
// CrossEncoderReranker：调用 BGE-Reranker / Cohere Rerank API
//   type CrossEncoderReranker struct { endpoint string; apiKey string }
//
// FusionReranker：Reciprocal Rank Fusion，数学融合无需 LLM
//   type FusionReranker struct { k int } // RRF 参数 k
//
// HybridReranker：组合多个 Reranker 的结果取平均
//   type HybridReranker struct { rerankers []Reranker; weights []float64 }
//
// 替换方式：
//   graph.SetChatModel(cm, graph.WithReranker(NewCrossEncoderReranker(...)))

// ── LLM 输出解析（从 rag_graph.go 迁出）──

func parseScores(raw string, expectedCount int) []int {
	defaults := make([]int, expectedCount)
	for i := range defaults {
		defaults[i] = expectedCount - i
	}

	start := strings.Index(raw, "[")
	end := strings.LastIndex(raw, "]")
	if start < 0 || end <= start {
		return defaults
	}

	scores := make([]int, expectedCount)
	found := 0
	jsonStr := raw[start : end+1]
	objects := strings.Split(jsonStr, "},{")
	for _, obj := range objects {
		obj = strings.Trim(obj, "[]{} ")
		id := extractInt(obj, `"id":`, ",")
		score := extractInt(obj, `"score":`, ",}")
		if id >= 0 && id < expectedCount && score >= 0 {
			scores[id] = score
			found++
		}
	}
	if found == 0 {
		return defaults
	}
	return scores
}

func extractInt(s, key, delim string) int {
	idx := strings.Index(s, key)
	if idx < 0 {
		return -1
	}
	rest := s[idx+len(key):]
	end := len(rest)
	for i, c := range rest {
		if strings.ContainsRune(delim, c) {
			end = i
			break
		}
	}
	val := strings.TrimSpace(rest[:end])
	var n int
	fmt.Sscanf(val, "%d", &n)
	return n
}
