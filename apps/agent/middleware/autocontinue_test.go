package middleware_test

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"

	"go-im-system/apps/agent/middleware"
)

func makeState(msgs ...adk.Message) *adk.ChatModelAgentState {
	return &adk.ChatModelAgentState{Messages: msgs}
}

func TestAutoContinue_NoTruncation_NoChange(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	state := makeState(
		schema.AssistantMessage("完整回复", nil),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "stop"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "stop" {
		t.Errorf("非截断状态不应修改 FinishReason, got=%q", last.ResponseMeta.FinishReason)
	}
	if len(last.ToolCalls) > 0 {
		t.Error("非截断状态不应注入 tool call")
	}
}

func TestAutoContinue_TextTruncation_InjectsContinueOutput(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	state := makeState(
		schema.AssistantMessage("被截断的", nil),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "length"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "tool_calls" {
		t.Errorf("截断状态应改为 tool_calls, got=%q", last.ResponseMeta.FinishReason)
	}
	if len(last.ToolCalls) == 0 {
		t.Fatal("截断状态应注入 continue_output tool call")
	}
	if last.ToolCalls[0].Function.Name != "continue_output" {
		t.Errorf("注入的工具名称应为 continue_output, got=%q", last.ToolCalls[0].Function.Name)
	}
}

func TestAutoContinue_MaxTokens_AlsoHandles(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	state := makeState(
		schema.AssistantMessage("另一段截断文本", nil),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "max_tokens"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "tool_calls" {
		t.Errorf("max_tokens 应触发续写, FinishReason=%q", last.ResponseMeta.FinishReason)
	}
	if len(last.ToolCalls) == 0 || last.ToolCalls[0].Function.Name != "continue_output" {
		t.Error("max_tokens 截断应注入 continue_output")
	}
}

func TestAutoContinue_WithToolCalls_Skipped(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	state := makeState(
		schema.AssistantMessage("", []schema.ToolCall{{
			ID:   "call_1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "search_chat_history",
				Arguments: `{"keywords":"test"}`,
			},
		}}),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "length"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "length" {
		t.Error("有 tool_call 的截断不应被 autocontinue 处理")
	}
}

func TestAutoContinue_MaxContinueLimit(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}

	state := makeState(
		// 原始对话
		schema.UserMessage("提问"),
		// 第 1 轮续写: Tool -> Assistant
		&schema.Message{Role: schema.Tool, ToolName: "continue_output", Content: "continue"},
		schema.AssistantMessage("第一轮续写", nil),
		// 第 2 轮续写: Tool -> Assistant
		&schema.Message{Role: schema.Tool, ToolName: "continue_output", Content: "continue"},
		schema.AssistantMessage("第二轮续写", nil),
		// 第 3 轮续写: Tool -> Assistant
		&schema.Message{Role: schema.Tool, ToolName: "continue_output", Content: "continue"},
		// 最新的模型回复（仍然被截断——第 4 次尝试）
		schema.AssistantMessage("第三轮续写", nil),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "length"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "length" {
		t.Errorf("超过连续续写上限不应修改 FinishReason, got=%q", last.ResponseMeta.FinishReason)
	}
	if len(last.ToolCalls) > 0 {
		t.Error("超过连续续写上限不应注入 tool call")
	}
}

func TestAutoContinue_CountRecentContinues_ResetsOnNonContinue(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}

	state := makeState(
		// 2 轮 continue_output + 1 次别的工具 → 链条被打断
		&schema.Message{Role: schema.Tool, ToolName: "continue_output", Content: "continue"},
		schema.AssistantMessage("续写 1", nil),
		&schema.Message{Role: schema.Tool, ToolName: "continue_output", Content: "continue"},
		schema.AssistantMessage("续写 2", nil),
		// 其他工具打断
		&schema.Message{Role: schema.Tool, ToolName: "search_chat_history", Content: "result"},
		// 模型最新回复 截断了
		schema.AssistantMessage("又截断了", nil),
	)
	state.Messages[len(state.Messages)-1].ResponseMeta = &schema.ResponseMeta{FinishReason: "length"}

	_, out, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.ResponseMeta.FinishReason != "tool_calls" {
		t.Errorf("链条被打断后应重新计数, FinishReason=%q", last.ResponseMeta.FinishReason)
	}
	if len(last.ToolCalls) == 0 || last.ToolCalls[0].Function.Name != "continue_output" {
		t.Error("链条被打断后应能注入 continue_output")
	}
}

func TestAutoContinue_EmptyMessages_NoPanic(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	state := makeState()

	_, _, err := m.AfterModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoContinue_BeforeAgent_AddsContinueTool(t *testing.T) {
	m := &middleware.AutoContinueMiddleware{}
	runCtx := &adk.ChatModelAgentContext{}

	_, out, err := m.BeforeAgent(context.Background(), runCtx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, t := range out.Tools {
		info, _ := t.Info(context.Background())
		if info.Name == "continue_output" {
			found = true
			break
		}
	}
	if !found {
		t.Error("BeforeAgent 应注入 continue_output 工具")
	}
}
