package user_api

import (
	"GrowRPC/xclient"
	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/proto/pb_user"
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// GetUserInfoHandler 获取用户信息（默认查当前登录用户，传 ?user_id=X 可查指定用户）
func GetUserInfoHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	targetID := userID.(int64)
	if qid := c.Query("user_id"); qid != "" {
		if id, err := strconv.ParseInt(qid, 10, 64); err == nil {
			targetID = id
		}
	}

	ctx := xclient.WithRoutingKey(context.Background(), "") // 走一致性哈希，或者空让底层随机/哈希
	reply, err := rpcclient.UserServiceClient.GetUserInfo(ctx, &pb_user.GetUserInfoArgs{UserId: targetID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "获取用户信息失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": gin.H{
			"user_id":  reply.UserId,
			"username": reply.Username,
			"nickname": reply.Nickname,
			"avatar":   reply.Avatar,
		},
	})
}

// UpdateUserInfoHandler 更新用户资料（昵称、头像、密码）
func UpdateUserInfoHandler(c *gin.Context) {
	userID, _ := c.Get("user_id")
	var req struct {
		Nickname string `json:"nickname"`
		Avatar   string `json:"avatar"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误"})
		return
	}

	_, err := rpcclient.UserServiceClient.UpdateUserInfo(context.Background(), &pb_user.UpdateUserInfoArgs{
		UserId:   userID.(int64),
		Nickname: req.Nickname,
		Avatar:   req.Avatar,
		Password: req.Password,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "更新失败", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 200, "msg": "更新成功"})
}

// SearchUserHandler 根据关键字搜索用户（用于添加好友）
func SearchUserHandler(c *gin.Context) {
	keyword := c.Query("keyword")
	if keyword == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "请输入搜索关键字"})
		return
	}

	ctx := context.Background()
	reply, err := rpcclient.UserServiceClient.SearchUser(ctx, &pb_user.SearchUserArgs{Keyword: keyword})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "搜索失败", "error": err.Error()})
		return
	}

	var data []gin.H
	for _, u := range reply.Users {
		data = append(data, gin.H{
			"user_id":  u.UserId,
			"username": u.Username,
			"nickname": u.Nickname,
			"avatar":   u.Avatar,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "success",
		"data": data,
	})
}
