// Package graph RAG 管线编排层
//
// 架构：
//
//	Pipeline (查询时)          Chunker (摄入时, chunker.go)
//	├─ Reranker 接口 ─ 可插拔    ├─ FixedSizeChunker
//	│  ├─ LLMReranker (默认)     ├─ SemanticChunker (未来)
//	│  ├─ CohereReranker (未来)  └─ MarkdownChunker (未来)
//	│  └─ FusionReranker (未来)
//	└─ Generate (ChatModel)
//
// 检索由调用方（Tools）负责，本层负责检索后的 Rerank → Generate
package graph

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cloudwego/eino/schema"
)

// ── 核心类型 ──

// Document 检索返回的文档片段
type Document struct {
	Content string  // 文档内容
	Score   float64 // 检索分数 (0~1)
	Source  string  // 来源: "fulltext" / "milvus" / "file"
}

// Input RAG 管线入参
type Input struct {
	Query      string
	Candidates []Document
}

// Output RAG 管线出参
type Output struct {
	Answer   string     // LLM 生成的回答
	UsedDocs []Document // 实际引用的文档（rerank 后的 top-N）
}

// ── 接口 ──

// ChatModel LLM 调用接口（解耦具体实现）
type ChatModel interface {
	Generate(ctx context.Context, messages []*schema.Message) (*schema.Message, error)
}

// Reranker 重排策略接口（可插拔）
// 实现：LLMReranker(默认) / CrossEncoderReranker / FusionReranker
type Reranker interface {
	Rerank(ctx context.Context, query string, docs []Document) []Document
}

// ── Pipeline ──

// Pipeline RAG 查询管线（Rerank → Generate）
type Pipeline struct {
	cm       ChatModel
	reranker Reranker
	topK     int
}

// PipelineOption 函数式配置
type PipelineOption func(*Pipeline)

// WithReranker 注入自定义重排策略（默认 LLMReranker）
func WithReranker(r Reranker) PipelineOption {
	return func(p *Pipeline) { p.reranker = r }
}

// WithTopK 设置生成阶段引用的文档数
func WithTopK(k int) PipelineOption {
	return func(p *Pipeline) { p.topK = k }
}

// NewPipeline 创建 RAG 管线
func NewPipeline(cm ChatModel, opts ...PipelineOption) *Pipeline {
	p := &Pipeline{cm: cm, topK: 3}
	for _, o := range opts {
		o(p)
	}
	if p.reranker == nil {
		p.reranker = NewLLMReranker(cm)
	}
	return p
}

// Rerank 仅执行重排，返回有序文档列表
// 适用场景：消息检索，Agent 的 LLM 做最终总结
func (p *Pipeline) Rerank(ctx context.Context, query string, docs []Document) []Document {
	return p.reranker.Rerank(ctx, query, docs)
}

// Execute 执行完整管线：Rerank → Generate
// 适用场景：文件检索 / 知识库，工具直接返回总结答案
func (p *Pipeline) Execute(ctx context.Context, input Input) (*Output, error) {
	if len(input.Candidates) == 0 {
		return &Output{Answer: "未找到相关信息"}, nil
	}

	ranked := p.reranker.Rerank(ctx, input.Query, input.Candidates)
	if len(ranked) > p.topK {
		ranked = ranked[:p.topK]
	}

	prompt := buildGeneratePrompt(input.Query, ranked)
	resp, err := p.cm.Generate(ctx, []*schema.Message{schema.UserMessage(prompt)})
	if err != nil {
		return nil, fmt.Errorf("RAG generate 失败: %w", err)
	}

	return &Output{Answer: resp.Content, UsedDocs: ranked}, nil
}

func buildGeneratePrompt(query string, docs []Document) string {
	var sb strings.Builder
	sb.WriteString("根据以下参考资料回答用户问题。如果资料不足以回答，请如实说明。\n\n")
	sb.WriteString(fmt.Sprintf("用户问题: %s\n\n参考资料:\n", query))
	for i, d := range docs {
		sb.WriteString(fmt.Sprintf("---\n[%d] %s\n", i+1, d.Content))
	}
	sb.WriteString("\n---\n请给出简洁准确的回答，引用具体信息时标注来源编号。")
	return sb.String()
}

// ── 单例 ──

var (
	orch     *Pipeline
	orchMu   sync.Mutex
	orchOnce sync.Once
)

// SetChatModel 注入 ChatModel（engine 包启动时调用一次）
func SetChatModel(model ChatModel, opts ...PipelineOption) {
	orchOnce.Do(func() {
		orch = NewPipeline(model, opts...)
	})
}

// GetOrchestrator 获取 RAG 管线单例
func GetOrchestrator() *Pipeline { return orch }

// ResetOrchestrator 清除单例，用于热重载
func ResetOrchestrator() {
	orchMu.Lock()
	orch = nil
	orchOnce = sync.Once{}
	orchMu.Unlock()
}
