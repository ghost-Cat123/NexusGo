package middleware

import (
	"NexusGo/apps/agent/tools"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

func init() {
	gob.Register(&ApprovalInfo{})
	gob.Register(&ApprovalResult{})
}

type ApprovalInfo struct {
	ToolName        string `json:"tool_name"`
	ArgumentsInJson string `json:"arguments_in_json"`
}

type ApprovalResult struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type ApprovalMiddleware struct {
	*adk.BaseChatModelAgentMiddleware
}

func (m *ApprovalMiddleware) WrapInvokableToolCall(
	_ context.Context,
	endpoint adk.InvokableToolCallEndpoint,
	tCtx *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {
	// 只拦截需要审批的工具
	if !tools.IsDestructiveTool(tCtx.Name) {
		return endpoint, nil
	}

	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		// 拿到工具中断状态
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)
		// 判断是否是第一次中断
		if !wasInterrupted {
			return "", tool.StatefulInterrupt(ctx, &ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJson: args,
			}, args)
		}
		// 恢复后第二次进入
		// 是否是目标工具 是否有数据 具体数据(也就是封装的返回值)
		isTarget, hasData, data := tool.GetResumeContext[*ApprovalResult](ctx)
		if isTarget && hasData {
			// 同意执行
			if data.Approved {
				return endpoint(ctx, storedArgs, opts...)
			}
			// 不同意且有原因
			if data.Reason != "" {
				return fmt.Sprintf("工具 '%s' 操作已取消 %s", tCtx.Name, data.Reason), nil
			}
			// 不同意没原因
			return "操作已取消", nil
		}
		// 如果不是当前工具的恢复或者没有数据 重新投递中断
		return "", tool.StatefulInterrupt(ctx, &ApprovalInfo{
			ToolName:        tCtx.Name,
			ArgumentsInJson: storedArgs,
		}, storedArgs)
	}, nil
}

func (m *ApprovalMiddleware) WrapStreamableToolCall(
	_ context.Context,
	endpoint adk.StreamableToolCallEndpoint,
	tCtx *adk.ToolContext,
) (adk.StreamableToolCallEndpoint, error) {
	// 只拦截需要审批的工具
	if !tools.IsDestructiveTool(tCtx.Name) {
		return endpoint, nil
	}
	return func(ctx context.Context, args string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)
		if !wasInterrupted {
			return nil, tool.StatefulInterrupt(ctx, &ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJson: args,
			}, args)
		}

		isTarget, hasData, data := tool.GetResumeContext[*ApprovalResult](ctx)
		if isTarget && hasData {
			if data.Approved {
				return endpoint(ctx, storedArgs, opts...)
			}
			if data.Reason != "" {
				return singleChunkReader(fmt.Sprintf("工具 '%s' 操作已取消 %s", tCtx.Name, data.Reason)), nil
			}
			return singleChunkReader(fmt.Sprintf("工具 '%s' 操作已取消", tCtx.Name)), nil
		}

		// 如果不是当前工具的恢复或者没有数据 重新投递中断
		return nil, tool.StatefulInterrupt(ctx, &ApprovalInfo{
			ToolName:        tCtx.Name,
			ArgumentsInJson: storedArgs,
		}, storedArgs)
	}, nil
}
