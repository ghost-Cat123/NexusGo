package tools

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"go-im-system/apps/agent/dao"
	"go-im-system/apps/agent/models"
	"time"
)

// SchMessageReq 定义入参
type SchMessageReq struct {
	TargetUser     string `json:"target_user" jsonschema:"description=接收消息的用户名(username)"`
	GroupName      string `json:"group_name" jsonschema:"description=接收消息的群聊名称 与 target_user 互斥"`
	SendTime       string `json:"send_time" jsonschema:"description=发送时间, RFC3339格式,例如 2026-04-03T09:00:00+08:00"`
	MessageContent string `json:"message_content" jsonschema:"required,description=要发送的消息内容"`
}

// SchMessageResp 定义出参
type SchMessageResp struct {
	Result string `json:"result"`
}

func schMessageInvoker(ctx context.Context, req *SchMessageReq) (*SchMessageResp, error) {
	sendTime, err := time.Parse(time.RFC3339, req.SendTime)
	if err != nil {
		return &SchMessageResp{Result: fmt.Sprintf("时间格式解析失败: %v，请告诉我发送的具体日期和时间，例如'明天早上9点'。", err)}, nil
	}

	// 时间是否在过去
	if sendTime.Before(time.Now()) {
		return &SchMessageResp{Result: fmt.Sprintf("时间已经过去了，请设定一个未来的时间。")}, err
	}

	currentUserID, ok := ctx.Value("current_user_id").(int64)
	if !ok {
		return nil, fmt.Errorf("internal error: missing user context")
	}

	// 查询发送的目标Id
	targetUser, err := dao.FindUserByName(req.TargetUser)
	if err != nil {
		return &SchMessageResp{Result: fmt.Sprintf("未找到用户 '%s'，请检查用户名。", req.TargetUser)}, nil
	}

	taskId, err := dao.CreateScheduledMessage(&models.ScheduledMessages{
		CreatorId:         currentUserID,
		ReceiverId:        targetUser.UserId,
		Content:           req.MessageContent,
		ScheduledSendTime: sendTime,
		Status:            0,
	})

	if err != nil {
		return nil, fmt.Errorf("db error: %w", err)
	}

	// 返回成功消息给大模型
	return &SchMessageResp{
		Result: fmt.Sprintf("已为你设定好定时消息！\n任务ID: %d\n接收人: %s\n发送时间: %s\n内容: %s\n时间到了我会自动帮你发出去。",
			taskId, targetUser.Username, sendTime.Format(time.RFC3339), req.MessageContent),
	}, nil
}

func schMessageTool() (tool.InvokableTool, error) {
	return utils.InferTool(
		"schedule_message",
		"用于设定定时发送消息。当用户需要在未来的某个特定时间点发送消息给某人时使用此工具。",
		schMessageInvoker,
	)
}

func MustSchMessageTool() tool.InvokableTool {
	t, err := schMessageTool()
	if err != nil {
		panic(fmt.Sprintf("MustSchMessageTool failed: %v", err))
	}
	return t
}
