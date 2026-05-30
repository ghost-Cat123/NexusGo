package middleware

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/schema"
	"time"
)

const maxContinue = 3

type AutoContinueMiddleware struct {
	*adk.BaseChatModelAgentMiddleware
}

func (m AutoContinueMiddleware) BeforeAgent(
	ctx context.Context, runCtx *adk.ChatModelAgentContext,
) (context.Context, *adk.ChatModelAgentContext, error) {
	runCtx.Tools = append(runCtx.Tools, newContinueOutputTool())
	return ctx, runCtx, nil
}

// 构建续写工具，抛给LLM, 伪工具调用
func newContinueOutputTool() tool.BaseTool {
	t, _ := toolutils.InferTool(
		"continue_output",
		"continue truncated output",
		func(ctx context.Context, _ *struct{}) (*struct{ Result string }, error) {
			return &struct{ Result string }{
				Result: "continue from where you left off, do not repeat content already output",
			}, nil
		},
	)
	return t
}

// AfterModelRewriteState 重写方法 进行截断检测+续写
func (m AutoContinueMiddleware) AfterModelRewriteState(
	ctx context.Context, state *adk.ChatModelAgentState, _ *adk.ModelContext,
) (context.Context, *adk.ChatModelAgentState, error) {
	if len(state.Messages) == 0 {
		return ctx, state, nil
	}
	// 获取模型生成的最后一条消息
	lastMsg := state.Messages[len(state.Messages)-1]
	// 正常结束
	if lastMsg.ResponseMeta == nil {
		return ctx, state, nil
	}
	// 只处理文本截断 已经处理好JSON修复
	reason := lastMsg.ResponseMeta.FinishReason
	// 中断原因："stop", "length", "tool_calls", "content_filter", "null"
	if reason != "length" && reason != "max_tokens" {
		return ctx, state, nil
	}
	// 工具调用截断由 tool_fix 兜底，这里只处理纯文本
	if len(lastMsg.ToolCalls) > 0 {
		return ctx, state, nil
	}
	// 防止无限循环，最多只续写三次
	count := countRecentContinues(state.Messages)
	if count >= maxContinue {
		return ctx, state, nil
	}
	// 注入续写工具调用
	lastMsg.ToolCalls = []schema.ToolCall{
		{
			// 时间戳唯一ID
			ID:   fmt.Sprintf("continue_%d", time.Now().UnixNano()),
			Type: "function",
			Function: schema.FunctionCall{
				// 工具名称
				Name: "continue_output",
				// 参数为空
				Arguments: "{}",
			},
		},
	}
	// 把结束原因变为tool_calls
	lastMsg.ResponseMeta.FinishReason = "tool_calls"
	return ctx, state, nil
}

// 从消息历史末尾往前遍历，检查连续出现了多少次 continue_output 工具，表示模型使用续写工具的尝试次数
func countRecentContinues(msgs []adk.Message) int {
	count := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		if msg.ToolName == "continue_output" && msg.Role == schema.Tool {
			count++
		} else if msg.Role == schema.Assistant {
			// 最后一条消息都是Assistant的消息 所以应该跳过
			continue
		} else {
			break
		}
	}
	return count
}
