package middleware_test

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"NexusGo/apps/agent/memory"
	"NexusGo/apps/agent/middleware"
)

func TestApprovalMiddleware_GroupSchedule_InterruptAndApprove(t *testing.T) {
	callCount := 0
	mockModel := &mockChatModel{
		generate: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
			callCount++
			if callCount == 1 {
				return schema.AssistantMessage("", []schema.ToolCall{{
					ID:   "call_group_001",
					Type: "function",
					Function: schema.FunctionCall{
						Name:      "schedule_message",
						Arguments: `{"group_name":"IM测试群","send_time":"2026-06-15T09:00:00+08:00","message_content":"明天开会"}`,
					},
				}}), nil
			}
			return schema.AssistantMessage("已为IM测试群设置明天9点的开会提醒。", nil), nil
		},
	}

	groupTool, err := toolutils.InferTool(
		"schedule_message",
		"用于设定定时发送消息，支持群聊和单聊",
		func(ctx context.Context, req *struct {
			TargetUser     string `json:"target_user" jsonschema:"description=单聊对象用户名"`
			GroupName      string `json:"group_name" jsonschema:"description=群名称"`
			SendTime       string `json:"send_time" jsonschema:"description=发送时间"`
			MessageContent string `json:"message_content" jsonschema:"required,description=消息内容"`
		}) (*struct {
			Result string `json:"result"`
		}, error) {
			return &struct {
				Result string `json:"result"`
			}{Result: "已为你设定好定时消息！\n任务ID: 99\n接收对象: IM测试群"}, nil
		},
	)
	if err != nil {
		t.Fatalf("创建群聊工具失败: %v", err)
	}

	ctx := context.Background()
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "test-agent-group-schedule",
		Description:   "群聊定时消息测试",
		Model:         mockModel,
		Instruction:   "You are a test assistant.",
		MaxIterations: 2,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.ApprovalMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{groupTool},
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

	checkpointID := "test-cp-group-schedule"
	events := runner.Run(ctx,
		[]*schema.Message{schema.UserMessage("帮我在IM测试群设定明天9点提醒开会")},
		adk.WithCheckPointID(checkpointID),
	)

	interrupted := false
	var found *adk.AgentEvent
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interrupted = true
			found = event
			break
		}
	}
	if !interrupted {
		t.Fatal("群聊定时消息应触发审批中断")
	}

	// 验证中断信息
	ics := found.Action.Interrupted.InterruptContexts
	if len(ics) == 0 {
		t.Fatal("中断上下文为空")
	}
	info, ok := ics[0].Info.(*middleware.ApprovalInfo)
	if !ok {
		t.Fatalf("Info 类型错误: %T", ics[0].Info)
	}
	if info.ToolName != "schedule_message" {
		t.Errorf("ToolName = %q, 期望 schedule_message", info.ToolName)
	}
	t.Logf("群聊中断信息: tool=%s args=%s", info.ToolName, info.ArgumentsInJson)

	// 审批通过
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

	var finalText string
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			t.Fatal("审批通过后不应再次中断")
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

	t.Logf("最终输出: %s", finalText)
	if finalText == "" {
		t.Error("审批通过后没有文本输出")
	}
}

func TestApprovalMiddleware_GroupSchedule_Rejected(t *testing.T) {
	callCount := 0
	mockModel := &mockChatModel{
		generate: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
			callCount++
			if callCount == 1 {
				return schema.AssistantMessage("", []schema.ToolCall{{
					ID:   "call_group_reject",
					Type: "function",
					Function: schema.FunctionCall{
						Name:      "schedule_message",
						Arguments: `{"group_name":"IM测试群","send_time":"2026-06-15T09:00:00+08:00","message_content":"测试"}`,
					},
				}}), nil
			}
			return schema.AssistantMessage("操作已取消", nil), nil
		},
	}

	groupTool, err := toolutils.InferTool(
		"schedule_message",
		"定时消息工具",
		func(ctx context.Context, req *struct {
			GroupName      string `json:"group_name"`
			SendTime       string `json:"send_time"`
			MessageContent string `json:"message_content"`
		}) (*struct {
			Result string `json:"result"`
		}, error) {
			return &struct {
				Result string `json:"result"`
			}{Result: "ok"}, nil
		},
	)
	if err != nil {
		t.Fatalf("创建工具失败: %v", err)
	}

	ctx := context.Background()
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "test-agent-group-reject",
		Description:   "群聊拒绝测试",
		Model:         mockModel,
		Instruction:   "You are a test assistant.",
		MaxIterations: 2,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.ApprovalMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{groupTool},
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

	checkpointID := "test-cp-group-reject"
	events := runner.Run(ctx,
		[]*schema.Message{schema.UserMessage("帮我在IM测试群发个消息")},
		adk.WithCheckPointID(checkpointID),
	)

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
		t.Fatal("期望检测到中断")
	}

	// 拒绝
	targets := make(map[string]any)
	for _, ic := range interruptedEvent.Action.Interrupted.InterruptContexts {
		if ic.IsRootCause {
			targets[ic.ID] = &middleware.ApprovalResult{Approved: false, Reason: "不需要"}
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
		if mv.Role == schema.Assistant && !mv.IsStreaming && mv.Message != nil {
			finalText += mv.Message.Content
		}
	}

	t.Logf("拒绝后输出: %s", finalText)
	if finalText == "" {
		t.Error("拒绝后应有文本输出")
	}
}
