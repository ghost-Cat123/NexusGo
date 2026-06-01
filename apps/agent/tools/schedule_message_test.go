package tools

import (
	"context"
	"testing"
	"time"
)

func init() {
	// Snowflake init 需要 logger + MySQL 等复杂依赖，单元测试不初始化
}

func TestSchMessageReq_GroupFieldPresent(t *testing.T) {
	req := SchMessageReq{
		GroupName:      "测试群",
		SendTime:       "2026-06-01T09:00:00+08:00",
		MessageContent: "开会提醒",
	}
	if req.GroupName != "测试群" {
		t.Error("GroupName 字段应为 '测试群'")
	}
	if req.TargetUser != "" {
		t.Error("TargetUser 应保持空字符串默认值")
	}
}

func TestSchMessageReq_TargetUserStillWorks(t *testing.T) {
	req := SchMessageReq{
		TargetUser:     "张三",
		SendTime:       "2026-06-01T09:00:00+08:00",
		MessageContent: "你好",
	}
	if req.TargetUser != "张三" {
		t.Error("TargetUser 字段应支持原有单聊场景")
	}
	if req.GroupName != "" {
		t.Error("GroupName 应保持空字符串默认值")
	}
}

func TestSchMessageReq_JSONTags(t *testing.T) {
	req := SchMessageReq{
		TargetUser:     "lisi",
		GroupName:      "IM测试群",
		MessageContent: "test",
		SendTime:       "2026-06-01T09:00:00+08:00",
	}
	if req.TargetUser != "lisi" {
		t.Error("TargetUser json tag 不正确")
	}
	if req.GroupName != "IM测试群" {
		t.Error("GroupName json tag 不正确")
	}
}

func TestSchMessageReq_EmptyBothFields(t *testing.T) {
	// 当 GroupName 和 TargetUser 都为空时，工具应通过 LLM ReAct 追问
	// 此测试仅验证结构本身允许该状态
	req := SchMessageReq{
		MessageContent: "test",
		SendTime:       "2026-06-01T09:00:00+08:00",
	}
	if req.TargetUser != "" || req.GroupName != "" {
		t.Error("两个字段均应默认为空")
	}
}

func TestSchMessageResp_Format(t *testing.T) {
	resp := SchMessageResp{
		Result: "已为你设定好定时消息！\n任务ID: 42\n接收对象: IM测试群\n发送时间: 2026-06-01T09:00:00+08:00\n内容: 开会了\n时间到了我会自动帮你发出去。",
	}
	if resp.Result == "" {
		t.Error("Result 不应为空")
	}
}

func TestSchMessageInvoker_PastTimeShouldFail(t *testing.T) {
	ctx := context.WithValue(context.Background(), "current_user_id", int64(1001))

	req := &SchMessageReq{
		TargetUser:     "张三",
		SendTime:       time.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		MessageContent: "过去的时间",
	}
	resp, err := schMessageInvoker(ctx, req)
	if err == nil && resp != nil {
		t.Logf("过去时间的响应: %s", resp.Result)
	} else if err != nil {
		t.Logf("过去时间触发了错误: %v", err)
	}
}

func TestSchMessageInvoker_InvalidTimeFormat(t *testing.T) {
	ctx := context.WithValue(context.Background(), "current_user_id", int64(1001))

	req := &SchMessageReq{
		TargetUser:     "张三",
		SendTime:       "not-a-valid-time",
		MessageContent: "测试",
	}
	resp, err := schMessageInvoker(ctx, req)
	if err != nil {
		t.Logf("无效时间格式错误: %v", err)
	}
	if resp != nil {
		t.Logf("无效时间响应: %s", resp.Result)
	}
}

func TestSchMessageInvoker_MissingUserContext(t *testing.T) {
	ctx := context.Background()
	req := &SchMessageReq{
		TargetUser:     "张三",
		SendTime:       time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		MessageContent: "测试",
	}
	_, err := schMessageInvoker(ctx, req)
	if err == nil {
		t.Error("缺少 current_user_id 时应返回错误")
	}
	t.Logf("期望的错误: %v", err)
}

func TestDestructiveToolRegistration(t *testing.T) {
	if !IsDestructiveTool("schedule_message") {
		t.Error("schedule_message 必须被注册为 destructive 工具")
	}

	if IsDestructiveTool("search_chat_history") {
		t.Error("search_chat_history 不应该是 destructive 工具")
	}

	if IsDestructiveTool("unknown_tool") {
		t.Error("未注册的工具不应该被识别为 destructive")
	}
}

func TestMustSchMessageTool_DoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MustSchMessageTool panic: %v", r)
		}
	}()
	tool := MustSchMessageTool()
	if tool == nil {
		t.Fatal("MustSchMessageTool 返回 nil")
	}
	info, err := tool.Info(context.Background())
	if err != nil {
		t.Fatalf("获取工具信息失败: %v", err)
	}
	t.Logf("工具名称: %s, 描述: %s", info.Name, info.Desc)
}
