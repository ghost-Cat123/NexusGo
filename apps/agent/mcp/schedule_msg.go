package mcp

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	"context"
	"fmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"time"
)

func RegisterScheduleMessage(s *server.MCPServer) {
	tool := mcp.NewTool("schedule_message",
		mcp.WithDescription("定时发送消息。设置未来某个时间点自动向单聊或群聊发送消息，支持审批确认"),
		mcp.WithNumber("sender_id", mcp.Required(), mcp.Description("发送者用户ID")),
		mcp.WithNumber("receiver_id", mcp.Description("接收者用户ID（单聊时使用）")),
		mcp.WithNumber("group_id", mcp.Description("群ID（群聊时使用）")),
		mcp.WithString("content", mcp.Required(), mcp.Description("消息内容")),
		mcp.WithString("send_at", mcp.Required(), mcp.Description("发送时间，RFC3339格式")),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.Params.Arguments
		argsMap, ok := args.(map[string]any)
		if !ok {
			return mcp.NewToolResultError("参数格式错误，arguments 必须为 JSON 对象"), nil
		}
		senderID := int64(argsMap["sender_id"].(float64))
		content := argsMap["content"].(string)
		sendAtStr := argsMap["send_at"].(string)

		sendAt, err := time.Parse(time.RFC3339, sendAtStr)
		if err != nil {
			return mcp.NewToolResultError("时间格式错误，请使用 RFC3339 格式"), nil
		}
		if sendAt.Before(time.Now()) {
			return mcp.NewToolResultError("发送时间不能早于当前时间"), nil
		}

		var receiverID, groupID int64
		if v, ok := argsMap["receiver_id"].(float64); ok {
			receiverID = int64(v)
		}
		if v, ok := argsMap["group_id"].(float64); ok {
			groupID = int64(v)
		}
		if receiverID == 0 && groupID == 0 {
			return mcp.NewToolResultError("请指定接收方（用户或群）"), nil
		}
		scheduledMessage := &models.ScheduledMessages{
			CreatorId:         senderID,
			ReceiverId:        receiverID,
			GroupId:           groupID,
			Content:           content,
			ScheduledSendTime: sendAt,
		}
		_, err = dao.CreateScheduledMessage(scheduledMessage)
		if err != nil {
			return mcp.NewToolResultError("创建定时消息失败: " + err.Error()), nil
		}

		return mcp.NewToolResultText(
			fmt.Sprintf("定时消息已创建，将在 %s 发送", sendAt.Format("2006-01-02 15:04:05")),
		), nil
	})
}
