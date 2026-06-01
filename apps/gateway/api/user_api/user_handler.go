package user_api

import (
	"GeeRPC/xclient"
	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/proto/pb_user"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"
	"log"
	"net/http"
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
	rpcReply := &pb_user.UserLoginReply{}

	// 结合一致性 Hash 调用 RPC
	ctx := xclient.WithRoutingKey(context.Background(), req.UserName)

	// 网关在此刻化身为 RPC 客户端，调用后端的逻辑服务！
	err := rpcclient.LogicRpcClient.Call(ctx, "LogicService.UserLogin", rpcArgs, rpcReply)

	// 如果 RPC 返回了错误（比如上面写的 "密码错误" 或 "用户不存在"）
	if err != nil {
		log.Printf("登录失败: %v", err)
		c.JSON(http.StatusUnauthorized, gin.H{
			"code":  401,
			"error": err.Error(), // 直接把后端的错误原因告诉前端
		})
		return
	}

	// 成功！将 Token 真实地返回给前端！
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "登录成功",
		"data": gin.H{
			"user_id": rpcReply.UserId,
			"token":   rpcReply.Token, // 【关键修复】放在这里！前端才能拿到！
		},
	})
}

func UserRegisterHandler(c *gin.Context) {
	// 前端请求封装
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

	// RPC请求参数封装
	rpcArgs := &pb_user.UserRegisterArgs{
		UserId:   req.UserID,
		UserName: req.UserName,
		Password: req.Password,
		Nickname: req.Nickname,
		Avatar:   req.Avatar,
	}
	rpcReply := &pb_user.UserRegisterReply{}

	// 无状态请求 直接使用前端的userName作为一致性哈希的key
	ctx := xclient.WithRoutingKey(context.Background(), req.UserName)

	// 调用逻辑层后端服务
	err := rpcclient.LogicRpcClient.Call(ctx, "LogicService.UserRegister", rpcArgs, rpcReply)
	if err != nil {
		logger.Log.Errorf("内部 RPC 调用失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "系统繁忙"})
		return
	}

	// RPC响应封装成前端响应参数
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "注册成功",
		"data": rpcReply.Success,
	})
}
