package group_api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/mq"
	"NexusGo/apps/pkg/proto/pb_group"

	"github.com/gin-gonic/gin"
)

// CreateGroupHandler 创建群聊
func CreateGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupName string  `json:"group_name"`
		MemberIDs []int64 `json:"member_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	reply, err := rpcclient.GroupServiceClient.CreateGroup(context.Background(), &pb_group.CreateGroupArgs{
		UserId:    userID.(int64),
		GroupName: req.GroupName,
		MemberIds: req.MemberIDs,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "创建失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "群聊创建成功",
		"data": gin.H{
			"group_id":   reply.GroupId,
			"creator_id": userID,
		},
	})
}

// GetGroupListHandler 获取我的群组列表
func GetGroupListHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")

	reply, err := rpcclient.GroupServiceClient.GetGroupList(context.Background(), &pb_group.GetGroupListArgs{
		UserId: userID.(int64),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取群列表失败", "error": err.Error()})
		return
	}

	var groups []gin.H
	for _, g := range reply.Groups {
		groups = append(groups, gin.H{
			"group_id":   g.GroupId,
			"group_name": g.GroupName,
			"owner_id":   g.OwnerId,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": groups,
	})
}

// GetGroupInfoHandler 获取群信息
func GetGroupInfoHandler(c *gin.Context) {
	groupIDStr := c.Query("group_id")
	groupID, _ := strconv.ParseInt(groupIDStr, 10, 64)

	reply, err := rpcclient.GroupServiceClient.GetGroupInfo(context.Background(), &pb_group.GetGroupInfoArgs{
		GroupId: groupID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取群信息失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": gin.H{
			"group_id":   reply.GroupId,
			"group_name": reply.GroupName,
			"owner_id":   reply.OwnerId,
		},
	})
}

// GetGroupMembersHandler 获取群成员
func GetGroupMembersHandler(c *gin.Context) {
	groupIDStr := c.Query("group_id")
	groupID, _ := strconv.ParseInt(groupIDStr, 10, 64)

	reply, err := rpcclient.GroupServiceClient.GetGroupMembers(context.Background(), &pb_group.GetGroupMembersArgs{
		GroupId: groupID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取群成员失败", "error": err.Error()})
		return
	}

	var members []gin.H
	for _, m := range reply.Members {
		members = append(members, gin.H{
			"group_id": groupID,
			"user_id":  m.UserId,
			"role":     m.Role,
			"nickname": m.Nickname,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": members,
	})
}

// JoinGroupHandler 加入群聊
func JoinGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.GroupServiceClient.JoinGroup(context.Background(), &pb_group.JoinGroupArgs{
		UserId:  userID.(int64),
		GroupId: req.GroupID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "加群失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "加群成功"})
}

// LeaveGroupHandler 退出群聊
func LeaveGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.GroupServiceClient.LeaveGroup(context.Background(), &pb_group.LeaveGroupArgs{
		UserId:  userID.(int64),
		GroupId: req.GroupID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "退群失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "退群成功"})
}

// DissolveGroupHandler 解散群聊（仅群主可操作，前端需自行校验）
func DissolveGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.GroupServiceClient.DissolveGroup(context.Background(), &pb_group.DissolveGroupArgs{
		UserId:  userID.(int64),
		GroupId: req.GroupID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "解散群聊失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "群聊已解散"})
}

// GetGroupMessagesHandler 分页拉取群聊历史消息
func GetGroupMessagesHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	groupIDStr := c.Query("group_id")
	cursorStr := c.Query("cursor")
	limitStr := c.Query("limit")

	groupID, _ := strconv.ParseInt(groupIDStr, 10, 64)
	cursor, _ := strconv.ParseInt(cursorStr, 10, 64)
	limit, _ := strconv.ParseInt(limitStr, 10, 64)

	if limit <= 0 {
		limit = 20
	}

	reply, err := rpcclient.GroupServiceClient.GetGroupHistory(context.Background(), &pb_group.GetGroupHistoryArgs{
		GroupId: groupID,
		UserId:  userID.(int64),
		Cursor:  cursor,
		Limit:   limit,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "拉取群消息失败", "error": err.Error()})
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
			"group_id":    groupID,
		},
	})
}

// SearchGroupHandler 搜索群聊
func SearchGroupHandler(c *gin.Context) {
	keyword := c.Query("keyword")
	if keyword == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "请输入搜索关键词"})
		return
	}
	reply, err := rpcclient.GroupServiceClient.SearchGroup(context.Background(), &pb_group.SearchGroupArgs{Keyword: keyword})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "搜索失败", "error": err.Error()})
		return
	}
	var data []gin.H
	for _, g := range reply.Groups {
		data = append(data, gin.H{
			"group_id":   g.GroupId,
			"group_name": g.GroupName,
			"owner_id":   g.OwnerId,
		})
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "success", "data": data})
}

// RequestJoinGroupHandler 申请加群 → WS 通知群主
func RequestJoinGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupID int64 `json:"group_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	info, err := rpcclient.GroupServiceClient.GetGroupInfo(context.Background(), &pb_group.GetGroupInfoArgs{GroupId: req.GroupID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "查群信息失败", "error": err.Error()})
		return
	}
	ownerKey := fmt.Sprintf("route:user:%d", info.OwnerId)
	gatewayAddr, _ := cache.GetCache().Get(context.Background(), ownerKey).Result()
	if gatewayAddr != "" {
		dp := &mq.DownPayload{
			SenderID:   userID.(int64),
			ReceiverID: info.OwnerId,
			Content:    strconv.FormatInt(req.GroupID, 10),
			ChatType:   mq.ChatTypeGroupJoinRequest,
			GroupID:    req.GroupID,
		}
		body, _ := json.Marshal(dp)
		_ = mq.PublishDown(context.Background(), gatewayAddr, body)
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "加群申请已发送"})
}

// ApproveJoinGroupHandler 群主审批加群申请
func ApproveJoinGroupHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		GroupID  int64  `json:"group_id"`
		UserID   int64  `json:"user_id"`
		Action   string `json:"action"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	if req.Action == "accept" {
		_, err := rpcclient.GroupServiceClient.JoinGroup(context.Background(), &pb_group.JoinGroupArgs{
			UserId: req.UserID, GroupId: req.GroupID,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "加群失败", "error": err.Error()})
			return
		}
		// 通知申请者
		rk := fmt.Sprintf("route:user:%d", req.UserID)
		gw, _ := cache.GetCache().Get(context.Background(), rk).Result()
		if gw != "" {
			dp := &mq.DownPayload{
				SenderID:   userID.(int64),
				ReceiverID: req.UserID,
				Content:    strconv.FormatInt(req.GroupID, 10),
				ChatType:   mq.ChatTypeGroupJoinApproved,
				GroupID:    req.GroupID,
			}
			body, _ := json.Marshal(dp)
			_ = mq.PublishDown(context.Background(), gw, body)
		}
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "处理完成"})
}
