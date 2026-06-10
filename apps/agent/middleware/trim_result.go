package middleware

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
	"strings"
)

type TrimResultMiddleWare struct {
	*adk.BaseChatModelAgentMiddleware
}

// 收到裁剪影响的工具前缀
var noisyPrefixes = []string{"search_chat_history"}

func (m *TrimResultMiddleWare) BeforeModelRewriteState(
	ctx context.Context, state *adk.ChatModelAgentState, _ *adk.ModelContext,
) (context.Context, *adk.ChatModelAgentState, error) {
	lastTextIdx := -1
	for i := len(state.Messages) - 1; i >= 0; i-- {
		msg := state.Messages[i]
		// 是LLM生成并且不为空
		if msg.Role == schema.Assistant && msg.Content != "" {
			// 最后一条 assistant 文本回复的位置
			lastTextIdx = i
			break
		}
	}
	// 活跃工具链中，不裁剪
	if lastTextIdx < 0 {
		return ctx, state, nil
	}
	// 裁剪 lastTextIdx 之前的 noisy 工具结果
	for i := 0; i < lastTextIdx; i++ {
		msg := state.Messages[i]
		if msg.Role != schema.Tool {
			continue
		}
		// 找到带有影响工具前缀的工具
		for _, prefix := range noisyPrefixes {
			if strings.HasPrefix(msg.ToolName, prefix) {
				// 结果省略 Messages是切片，这边msg只拿到了消息的拷贝，真正需要改的是消息的内容
				state.Messages[i].Content = fmt.Sprintf("[result omitted] (~%d chars)", len(msg.Content))
				break
			}
		}
	}
	return ctx, state, nil
}
