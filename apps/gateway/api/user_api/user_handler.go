package user_api

import (
	"GrowRPC/xclient"
	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/proto/pb_user"
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

func UserLoginHandler(c *gin.Context) {
	var req struct {
		UserID   int64  `json:"user_id"`
		UserName string `json:"user_name"`
		Password string `json:"password"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	rpcArgs := &pb_user.UserLoginArgs{
		UserId:   req.UserID,
		UserName: req.UserName,
		Password: req.Password,
	}

	ctx := xclient.WithRoutingKey(context.Background(), req.UserName)
	rpcReply, err := rpcclient.UserServiceClient.UserLogin(ctx, rpcArgs)

	if err != nil {
		logger.Log.Errorf("登录失败: %v", err)
		c.JSON(http.StatusUnauthorized, gin.H{
			"code":  401,
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "登录成功",
		"data": gin.H{
			"user_id": rpcReply.UserId,
			"token":   rpcReply.Token,
		},
	})
}

func UserRegisterHandler(c *gin.Context) {
	var req struct {
		UserID   int64  `json:"user_id"`
		UserName string `json:"user_name"`
		Password string `json:"password"`
		Nickname string `json:"nickname"`
		Avatar   string `json:"avatar"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	rpcArgs := &pb_user.UserRegisterArgs{
		UserId:   req.UserID,
		UserName: req.UserName,
		Password: req.Password,
		Nickname: req.Nickname,
		Avatar:   req.Avatar,
	}

	ctx := xclient.WithRoutingKey(context.Background(), req.UserName)
	rpcReply, err := rpcclient.UserServiceClient.UserRegister(ctx, rpcArgs)
	if err != nil {
		logger.Log.Errorf("内部 RPC 调用失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "系统繁忙"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "注册成功",
		"data": rpcReply.Success,
	})
}
