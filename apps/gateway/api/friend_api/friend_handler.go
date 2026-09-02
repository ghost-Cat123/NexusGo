package friend_api

import (
	"context"
	"net/http"

	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/proto/pb_friend"

	"github.com/gin-gonic/gin"
)

// ApplyFriendHandler 申请添加好友
func ApplyFriendHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		FriendID int64  `json:"friend_id"`
		ApplyMsg string `json:"apply_msg"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.FriendServiceClient.ApplyFriend(context.Background(), &pb_friend.ApplyFriendArgs{
		UserId:   userID.(int64),
		FriendId: req.FriendID,
		ApplyMsg: req.ApplyMsg,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "申请失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "好友申请已发送"})
}

// ResolveFriendHandler 处理好友申请（同意/拒绝）
func ResolveFriendHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		FriendID int64  `json:"friend_id"`
		Action   string `json:"action"` // "accept" or "reject"
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.FriendServiceClient.ResolveFriend(context.Background(), &pb_friend.ResolveFriendArgs{
		UserId:   userID.(int64),
		FriendId: req.FriendID,
		Action:   req.Action,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "处理失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "处理成功"})
}

// GetFriendListHandler 获取好友列表
func GetFriendListHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")

	reply, err := rpcclient.FriendServiceClient.GetFriendList(context.Background(), &pb_friend.GetFriendListArgs{
		UserId: userID.(int64),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取列表失败", "error": err.Error()})
		return
	}

	var data []gin.H
	for _, f := range reply.Friends {
		data = append(data, gin.H{
			"friend_id": f.FriendId,
			"nickname":  f.Nickname,
			"avatar":    f.Avatar,
			"status":    f.Status,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": data,
	})
}

// DeleteFriendHandler 删除好友
func DeleteFriendHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		FriendID int64 `json:"friend_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.FriendServiceClient.DeleteFriend(context.Background(), &pb_friend.DeleteFriendArgs{
		UserId:   userID.(int64),
		FriendId: req.FriendID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "删除失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "删除成功"})
}

// GetPendingRequestsHandler 获取待处理的好友申请
func GetPendingRequestsHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")

	reply, err := rpcclient.FriendServiceClient.GetPendingRequests(context.Background(), &pb_friend.GetPendingRequestsArgs{
		UserId: userID.(int64),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取申请列表失败", "error": err.Error()})
		return
	}

	var data []gin.H
	for _, r := range reply.Requests {
		data = append(data, gin.H{
			"user_id":    r.UserId,
			"username":   r.Username,
			"nickname":   r.Nickname,
			"avatar":     r.Avatar,
			"apply_msg":  r.ApplyMsg,
			"created_at": r.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": data,
	})
}
