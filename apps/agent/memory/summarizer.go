package memory

import (
	"context"
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

const summaryPrompt = "你是一个对话摘要助手。你的任务是将用户与 AI 助手的历史对话压缩为一段简洁的摘要，要求：\n" +
	"1. 保留所有重要信息、用户意图、关键事实和用户偏好\n" +
	"2. 摘要不超过 300 字\n" +
	"3. 用第三人称叙述，例如「用户询问了...，AI回复了...」\n" +
	"4. 直接输出摘要内容，不要有任何前缀或解释"

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

// SummarizeWithLLM 将一组历史消息压缩为摘要字符串。
// 直接调用 ChatModel（非 Agent），不走 Tool、不走 Runner，极轻量。
func SummarizeWithLLM(ctx context.Context, msgs []*schema.Message) (string, error) {
	// 1. 将历史消息格式化为可读文本
	var sb strings.Builder
	for _, m := range msgs {
		role := string(m.Role)
		sb.WriteString(role)
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteString("\n")
	}
	dialogText := sb.String()

	// 2. 构造摘要 Prompt（System + User 两条消息）
	userPrompt := fmt.Sprintf("请为以下对话生成摘要：\n\n%s", dialogText)

	// 3. 初始化 ChatModel 包级缓存+双确认锁
	cm, err := getSummaryChatModel(ctx)

	if err != nil {
		return "", err
	}

	// 4. 调用 ChatModel
	resp, err := cm.Generate(ctx, []*schema.Message{
		schema.SystemMessage(summaryPrompt),
		schema.UserMessage(userPrompt),
	})
	if err != nil {
		return "", fmt.Errorf("摘要 LLM 调用失败: %w", err)
	}

	summary := strings.TrimSpace(resp.Content)
	if summary == "" {
		return "", fmt.Errorf("摘要 LLM 返回空内容")
	}

	logger.Log.Debugf("[Summarizer] 摘要生成成功，原始对话 %d 条，摘要长度 %d 字", len(msgs), len(summary))
	return summary, nil
}
