package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/logger"
	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/schema"
)

var (
	cacheSummaryCM *deepseek.ChatModel
	summaryCMMu    sync.Mutex
)

const summaryPrompt = "你是对话摘要助手。请将以下对话压缩为 JSON 格式输出：\n" +
	"{\n" +
	"  \"summary\": \"摘要正文，不超过 300 字，第三人称\",\n" +
	"  \"type\": \"fact|preference|decision|knowledge|general\",\n" +
	"  \"tags\": \"关键词1,关键词2\",\n" +
	"  \"importance\": 0-5\n" +
	"}\n" +
	"type 说明：fact=用户提到的事实，preference=用户偏好习惯，decision=用户做出的决策，knowledge=技术知识点，general=普通对话\n" +
	"importance: 5=极其重要/关键决策，0=闲聊。只输出 JSON，不要其他内容。"

// SummaryResult LLM 摘要结构化输出
type SummaryResult struct {
	Summary    string `json:"summary"`
	Type       string `json:"type"`
	Tags       string `json:"tags"`
	Importance int    `json:"importance"`
}

func getSummaryChatModel(ctx context.Context) (*deepseek.ChatModel, error) {
	if cacheSummaryCM != nil {
		return cacheSummaryCM, nil
	}
	// 加锁
	summaryCMMu.Lock()
	// 解锁
	defer summaryCMMu.Unlock()
	// 再次确认
	if cacheSummaryCM != nil {
		return cacheSummaryCM, nil
	}
	// 没有则创建
	defaultAgent := config.GetDefaultAgent()
	apiKey := config.ResolveAgentAPIKey(defaultAgent)
	if apiKey == "" {
		return nil, fmt.Errorf("[摘要 ChatModel]：API Key 为空")
	}
	cm, err := deepseek.NewChatModel(ctx, &deepseek.ChatModelConfig{
		APIKey:  apiKey,
		Model:   defaultAgent.ModelName,
		BaseURL: defaultAgent.BaseURL,
	})
	if err != nil {
		return nil, fmt.Errorf("摘要 ChatModel 初始化失败: %w", err)
	}
	cacheSummaryCM = cm
	return cacheSummaryCM, nil
}

func ClearCachedSummaryCM() {
	summaryCMMu.Lock()
	cacheSummaryCM = nil
	summaryCMMu.Unlock()
}

func init() {
	config.OnReload(ClearCachedSummaryCM)
}

// SummarizeWithLLM 将一组历史消息压缩为结构化摘要。
func SummarizeWithLLM(ctx context.Context, msgs []*schema.Message) (*SummaryResult, error) {
	var sb strings.Builder
	for _, m := range msgs {
		sb.WriteString(string(m.Role))
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}
	dialogText := sb.String()

	userPrompt := fmt.Sprintf("请为以下对话生成摘要：\n\n%s", dialogText)

	cm, err := getSummaryChatModel(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := cm.Generate(ctx, []*schema.Message{
		schema.SystemMessage(summaryPrompt),
		schema.UserMessage(userPrompt),
	})
	if err != nil {
		return nil, fmt.Errorf("摘要 LLM 调用失败: %w", err)
	}

	raw := strings.TrimSpace(resp.Content)
	if raw == "" {
		return nil, fmt.Errorf("摘要 LLM 返回空内容")
	}

	// 解析 JSON，带兜底
	var result SummaryResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		// LLM 输出可能不是合法 JSON，降级为纯文本摘要
		logger.Log.Warnf("[Summarizer] JSON 解析失败，降级为纯文本: %v, raw=%s", err, raw[:min(100, len(raw))])
		result = SummaryResult{
			Summary:    raw,
			Type:       "general",
			Tags:       "",
			Importance: 1,
		}
	}
	if result.Summary == "" {
		result.Summary = raw
	}
	if result.Type == "" {
		result.Type = "general"
	}
	if result.Importance < 0 {
		result.Importance = 1
	}
	if result.Importance > 5 {
		result.Importance = 5
	}

	logger.Log.Debugf("[Summarizer] 摘要生成成功，type=%s importance=%d len=%d",
		result.Type, result.Importance, len(result.Summary))
	return &result, nil
}
