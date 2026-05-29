package middleware_test

import (
	"context"
	"fmt"
	"go-im-system/apps/agent/memory"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"go-im-system/apps/agent/engine"
	"go-im-system/apps/agent/middleware"
	"go-im-system/apps/agent/tools"
)

// ── mock ToolCallingChatModel ──

type mockChatModel struct {
	generate func(ctx context.Context, input []*schema.Message) (*schema.Message, error)
}

func (m *mockChatModel) Generate(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.generate(ctx, input)
}

func (m *mockChatModel) Stream(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.generate(ctx, input)
	if err != nil {
		return nil, err
	}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(msg, nil)
	sw.Close()
	return sr, nil
}

func (m *mockChatModel) BindTools(_ []*schema.ToolInfo) error {
	return nil
}

// GetType GoString 等 eino 组件统一接口
func (m *mockChatModel) GetType() string { return "mock_chat_model" }

// ── 测试工具（不上 DB，纯内存） ──

type testSchReq struct {
	TargetUser     string `json:"target_user" jsonschema:"description=接收用户"`
	SendTime       string `json:"send_time" jsonschema:"description=发送时间"`
	MessageContent string `json:"message_content" jsonschema:"required,description=消息内容"`
}

type testSchResp struct {
	Result string `json:"result"`
}

func newTestDestructiveTool(t *testing.T) tool.BaseTool {
	t.Helper()
	inv, err := toolutils.InferTool(
		"schedule_message",
		"测试用定时消息工具",
		func(ctx context.Context, req *testSchReq) (*testSchResp, error) {
			return &testSchResp{Result: "测试工具执行成功"}, nil
		},
	)
	if err != nil {
		t.Fatalf("创建测试工具失败: %v", err)
	}
	return inv
}

// ── 纯单元测试 ──

func TestIsDestructiveTool(t *testing.T) {
	if !tools.IsDestructiveTool("schedule_message") {
		t.Error("schedule_message 应该被识别为 destructive 工具")
	}
	if tools.IsDestructiveTool("search_chat_history") {
		t.Error("search_chat_history 不应该被识别为 destructive 工具")
	}
	if tools.IsDestructiveTool("unknown_tool") {
		t.Error("未知工具不应该被识别为 destructive 工具")
	}
}

func TestSessionChannelRegisterAndPush(t *testing.T) {
	cpID := "test-checkpoint-123"

	ch := engine.RegisterSessionChan(cpID)
	if ch == nil {
		t.Fatal("RegisterSessionChan 返回 nil channel")
	}

	done := make(chan struct{})
	go func() {
		result := <-ch
		if !result.Approved {
			t.Error("期望 Approved=true")
		}
		close(done)
	}()

	ok := engine.PushSessionDecision(cpID, &middleware.ApprovalResult{Approved: true, Reason: ""})
	if !ok {
		t.Error("PushSessionDecision 应该成功")
	}
	<-done

	// 重复推送应该失败（channel 已删除）
	ok = engine.PushSessionDecision(cpID, &middleware.ApprovalResult{Approved: false})
	if ok {
		t.Error("重复 PushSessionDecision 应该失败")
	}
}

// ── 集成测试：完整中断 → 审批 → 恢复流程 ──

func TestApprovalMiddleware_FullInterruptResume_Approved(t *testing.T) {
	callCount := 0
	mockModel := &mockChatModel{
		generate: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
			callCount++
			if callCount == 1 {
				return schema.AssistantMessage("", []schema.ToolCall{{
					ID:   "call_test_001",
					Type: "function",
					Function: schema.FunctionCall{
						Name:      "schedule_message",
						Arguments: `{"target_user":"张三","send_time":"2025-06-01T09:00:00+08:00","message_content":"开会提醒"}`,
					},
				}}), nil
			}
			return schema.AssistantMessage("好的，已帮你设定定时消息。", nil), nil
		},
	}

	testTool := newTestDestructiveTool(t)

	ctx := context.Background()
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "test-agent",
		Description:   "测试 Agent",
		Model:         mockModel,
		Instruction:   "You are a test assistant.",
		MaxIterations: 2,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.ApprovalMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{testTool},
			},
		},
	})
	if err != nil {
		t.Fatalf("创建 Agent 失败: %v", err)
	}

	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: false,
		CheckPointStore: memory.NewCheckPointStore(),
	})

	checkpointID := "test-cp-approved"
	messages := []*schema.Message{
		schema.UserMessage("帮我明天9点提醒张三开会"),
	}

	events := runner.Run(ctx, messages, adk.WithCheckPointID(checkpointID))

	// 第一阶段：消费事件，应该遇到中断
	interrupted := false
	var found *adk.AgentEvent
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			t.Logf("事件错误: %v", event.Err)
			continue
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interrupted = true
			found = event
			break
		}
	}
	if !interrupted {
		t.Fatal("期望检测到中断事件，但没有")
	}

	// 验证中断信息包含正确的工具名
	ics := found.Action.Interrupted.InterruptContexts
	if len(ics) == 0 {
		t.Fatal("中断上下文为空")
	}
	info, ok := ics[0].Info.(*middleware.ApprovalInfo)
	if !ok {
		t.Fatalf("中断 Info 类型不匹配: %T", ics[0].Info)
	}
	if info.ToolName != "schedule_message" {
		t.Errorf("中断工具名 = %q, 期望 schedule_message", info.ToolName)
	}
	t.Logf("中断信息: tool=%s args=%s", info.ToolName, info.ArgumentsInJson)

	// 第二阶段：模拟用户审批通过，恢复执行
	targets := make(map[string]any, len(ics))
	for _, ic := range ics {
		if ic.IsRootCause {
			targets[ic.ID] = &middleware.ApprovalResult{Approved: true}
		}
	}

	events, err = runner.ResumeWithParams(ctx, checkpointID,
		&adk.ResumeParams{Targets: targets})
	if err != nil {
		t.Fatalf("ResumeWithParams 失败: %v", err)
	}

	// 第三阶段：消费恢复后的事件，应该得到工具结果和最终回复
	var finalText string
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			t.Logf("恢复后事件错误: %v", event.Err)
			continue
		}
		// 检查是否还有中断（不应该）
		if event.Action != nil && event.Action.Interrupted != nil {
			t.Fatal("审批通过后不应该再次中断")
		}
		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}
		mv := event.Output.MessageOutput
		if mv.Role != schema.Assistant {
			continue
		}
		if mv.IsStreaming {
			mv.MessageStream.SetAutomaticClose()
			for {
				frame, err := mv.MessageStream.Recv()
				if err != nil {
					break
				}
				if frame != nil && frame.Content != "" {
					finalText += frame.Content
				}
			}
		} else if mv.Message != nil && mv.Message.Content != "" {
			finalText += mv.Message.Content
		}
	}

	t.Logf("最终输出: %s", finalText)
	if finalText == "" {
		t.Error("审批通过后没有文本输出")
	}
}

func TestApprovalMiddleware_FullInterruptResume_Rejected(t *testing.T) {
	callCount := 0
	mockModel := &mockChatModel{
		generate: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
			callCount++
			if callCount == 1 {
				return schema.AssistantMessage("", []schema.ToolCall{{
					ID:   "call_test_002",
					Type: "function",
					Function: schema.FunctionCall{
						Name:      "schedule_message",
						Arguments: `{"target_user":"李四","send_time":"2025-06-02T10:00:00+08:00","message_content":"测试消息"}`,
					},
				}}), nil
			}
			return schema.AssistantMessage("操作已取消", nil), nil
		},
	}

	testTool := newTestDestructiveTool(t)

	ctx := context.Background()
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "test-agent-reject",
		Description:   "测试 Agent-拒绝场景",
		Model:         mockModel,
		Instruction:   "You are a test assistant.",
		MaxIterations: 2,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.ApprovalMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{testTool},
			},
		},
	})
	if err != nil {
		t.Fatalf("创建 Agent 失败: %v", err)
	}

	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: false,
		CheckPointStore: memory.NewCheckPointStore(),
	})

	checkpointID := "test-cp-rejected"
	events := runner.Run(ctx,
		[]*schema.Message{schema.UserMessage("取消那条定时消息")},
		adk.WithCheckPointID(checkpointID),
	)

	// 找到中断
	var interruptedEvent *adk.AgentEvent
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interruptedEvent = event
			break
		}
	}
	if interruptedEvent == nil {
		t.Fatal("期望检测到中断事件")
	}

	// 审批拒绝
	targets := make(map[string]any)
	for _, ic := range interruptedEvent.Action.Interrupted.InterruptContexts {
		if ic.IsRootCause {
			targets[ic.ID] = &middleware.ApprovalResult{Approved: false, Reason: "不想发"}
		}
	}

	events, err = runner.ResumeWithParams(ctx, checkpointID,
		&adk.ResumeParams{Targets: targets})
	if err != nil {
		t.Fatalf("ResumeWithParams 失败: %v", err)
	}

	var finalText string
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}
		mv := event.Output.MessageOutput
		if mv.Role != schema.Assistant {
			continue
		}
		if !mv.IsStreaming && mv.Message != nil {
			finalText += mv.Message.Content
		}
	}

	t.Logf("拒绝后输出: %s", finalText)
	// 拒绝后应该包含 "操作已取消"
	if finalText == "" {
		t.Error("拒绝后没有文本输出")
	}
}

// ── 测试：只读工具不被拦截 ──

func TestApprovalMiddleware_ReadOnlyTool_PassesThrough(t *testing.T) {
	mockModel := &mockChatModel{
		generate: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("搜索完成", nil), nil
		},
	}

	// 工具名称不在 destructiveToolNames 中
	searchTool, err := toolutils.InferTool(
		"search_chat_history",
		"搜索聊天记录",
		func(ctx context.Context, req *struct {
			Keywords string `json:"keywords" jsonschema:"required"`
		}) (*struct {
			Result string `json:"result"`
		}, error) {
			return &struct {
				Result string `json:"result"`
			}{Result: "找到 3 条记录"}, nil
		},
	)
	if err != nil {
		t.Fatalf("创建搜索工具失败: %v", err)
	}

	ctx := context.Background()
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "test-agent-readonly",
		Description:   "只读工具测试",
		Model:         mockModel,
		Instruction:   "You are a test assistant.",
		MaxIterations: 1,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.ApprovalMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{searchTool},
			},
		},
	})
	if err != nil {
		t.Fatalf("创建 Agent 失败: %v", err)
	}

	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: false,
		CheckPointStore: memory.NewCheckPointStore(),
	})

	events := runner.Run(ctx,
		[]*schema.Message{schema.UserMessage("搜索密码相关的聊天记录")},
		adk.WithCheckPointID("test-cp-readonly"),
	)

	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			t.Fatal("只读工具不应该触发中断")
		}
	}

	t.Log("只读工具通过测试——没有触发中断")
}

// Example 测试用法
func ExampleApprovalResult() {
	r := &middleware.ApprovalResult{Approved: true}
	fmt.Println(r.Approved)
	// Output: true
}
