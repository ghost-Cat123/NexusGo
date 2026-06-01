package middleware_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cloudwego/eino/schema"

	"NexusGo/apps/agent/middleware"
)

func TestTrimResult_TrimsNoisyBeforeLastAssistant(t *testing.T) {
	m := &middleware.TrimResultMiddleWare{}
	longContent := strings.Repeat("搜索结果", 500)

	state := makeState(
		// 旧的搜索工具结果（应在裁剪范围内）
		&schema.Message{Role: schema.Tool, ToolName: "search_chat_history", Content: longContent},
		// 模型基于搜索结果的文本回复
		schema.AssistantMessage("根据搜索结果，找到了以下内容...", nil),
		// 当前工具链中的新结果（不应裁剪）
		&schema.Message{Role: schema.Tool, ToolName: "schedule_message", Content: "任务创建成功"},
	)

	_, out, err := m.BeforeModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 旧搜索结果应被裁剪
	oldResult := out.Messages[0]
	if oldResult.Role != schema.Tool {
		t.Fatal("消息 0 应为工具结果")
	}
	if !strings.Contains(oldResult.Content, "[result omitted]") {
		t.Errorf("旧搜索结果应被裁剪, got=%q", oldResult.Content)
	}

	// 当前的 schedule_message 结果不应被裁剪
	currentResult := out.Messages[2]
	if currentResult.Role != schema.Tool {
		t.Fatal("消息 2 应为工具结果")
	}
	if strings.Contains(currentResult.Content, "[result omitted]") {
		t.Error("当前工具链中的结果不应被裁剪")
	}
}

func TestTrimResult_NoAssistantText_NoTrim(t *testing.T) {
	m := &middleware.TrimResultMiddleWare{}
	longContent := strings.Repeat("搜索数据", 300)

	state := makeState(
		&schema.Message{Role: schema.Tool, ToolName: "search_chat_history", Content: longContent},
		// 没有 assistant 文本回复——工具链仍活跃
	)

	_, out, err := m.BeforeModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(out.Messages[0].Content, "[result omitted]") {
		t.Error("活跃工具链中不应裁剪结果")
	}
}

func TestTrimResult_IgnoresNonNoisyTools(t *testing.T) {
	m := &middleware.TrimResultMiddleWare{}
	scheduleContent := "任务ID: 12345"

	state := makeState(
		&schema.Message{Role: schema.Tool, ToolName: "schedule_message", Content: scheduleContent},
		schema.AssistantMessage("定时消息已设置", nil),
	)

	_, out, err := m.BeforeModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(out.Messages[0].Content, "[result omitted]") {
		t.Error("非噪音工具结果不应被裁剪")
	}
}

func TestTrimResult_OnlyTrimsBeforeLastText(t *testing.T) {
	m := &middleware.TrimResultMiddleWare{}
	firstContent := strings.Repeat("第一轮搜索", 200)
	secondContent := strings.Repeat("第二轮搜索", 200)

	state := makeState(
		&schema.Message{Role: schema.Tool, ToolName: "search_chat_history", Content: firstContent},
		schema.AssistantMessage("第一轮结果摘要", nil),
		&schema.Message{Role: schema.Tool, ToolName: "search_chat_history", Content: secondContent},
		// 最后一轮搜索结果后还没有 assistant 文本回复——第三轮工具链活跃中
	)

	_, out, err := m.BeforeModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 第一轮结果——已在 assistant 回复前，应裁剪
	if !strings.Contains(out.Messages[0].Content, "[result omitted]") {
		t.Error("第一轮搜索结果应被裁剪")
	}
	// 第三轮结果——在最后一个 assistant 之后，工具链活跃，不应裁剪
	if strings.Contains(out.Messages[2].Content, "[result omitted]") {
		t.Error("活跃工具链中的第三轮结果不应被裁剪")
	}
}

func TestTrimResult_EmptyMessages_NoPanic(t *testing.T) {
	m := &middleware.TrimResultMiddleWare{}
	state := makeState()

	_, _, err := m.BeforeModelRewriteState(context.Background(), state, nil)
	if err != nil {
		t.Fatal(err)
	}
}
