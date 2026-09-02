package chat_api

import (
	"context"
	"net/http"
	"strconv"

	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/proto/pb_msg"

	"github.com/gin-gonic/gin"
)

// GetConversationsHandler 获取当前用户的会话列表
func GetConversationsHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")

	reply, err := rpcclient.MsgServiceClient.GetConversations(context.Background(), &pb_msg.GetConversationsArgs{
		UserId: userID.(int64),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取列表失败", "error": err.Error()})
		return
	}

	var data []gin.H
	for _, conv := range reply.Conversations {
		data = append(data, gin.H{
			"target_id":    conv.TargetId,
			"session_type": conv.SessionType,
			"unread_count": conv.UnreadCount,
			"owner_id":     conv.OwnerId,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": data,
	})
}

// GetChatMessagesHandler 分页拉取单聊历史消息
func GetChatMessagesHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	targetIDStr := c.Query("target_id")
	cursorStr := c.Query("cursor") // 最后一条消息的 ID 或 Seq，用于游标分页
	limitStr := c.Query("limit")

	targetID, _ := strconv.ParseInt(targetIDStr, 10, 64)
	cursor, _ := strconv.ParseInt(cursorStr, 10, 64)
	limit, _ := strconv.ParseInt(limitStr, 10, 64)

	if limit <= 0 {
		limit = 20
	}

	reply, err := rpcclient.MsgServiceClient.GetChatHistory(context.Background(), &pb_msg.GetChatHistoryArgs{
		UserId:   userID.(int64),
		TargetId: targetID,
		Cursor:   cursor,
		Limit:    limit,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "拉取历史消息失败", "error": err.Error()})
		return
	}

	var messages []gin.H
	for _, m := range reply.Messages {
		messages = append(messages, gin.H{
			"msg_id":      m.MsgId,
			"sender_id":   m.SenderId,
			"content":     m.Content,
			"create_time": m.CreateTime,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": gin.H{
			"messages":    messages,
			"next_cursor": reply.NextCursor,
			"has_more":    reply.HasMore,
			"target_id":   reply.TargetId,
		},
	})
}

// MarkMessageReadHandler 标记消息已读
func MarkMessageReadHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		TargetID       int64 `json:"target_id"`
		LastReadMsgID  int64 `json:"last_read_msg_id"`
		SessionType    int   `json:"session_type"` // 1: 单聊, 2: 群聊
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.MsgServiceClient.MarkMessageRead(context.Background(), &pb_msg.MarkReadArgs{
		UserId:         userID.(int64),
		TargetId:       req.TargetID,
		LastReadMsgId:  req.LastReadMsgID,
		SessionType:    int32(req.SessionType),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "更新已读状态失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "已读状态更新成功"})
}
