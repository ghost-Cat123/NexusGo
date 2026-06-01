package handler

import (
	"NexusGo/apps/agent/engine"
	"NexusGo/apps/agent/middleware"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func ApproveSSE(c *gin.Context) {
	// 查询传入的session_id
	checkpointID := c.Query("checkpoint_id")
	// 解析传递的approved
	approved, _ := strconv.ParseBool(c.Query("approved"))
	// 构造审批result
	result := &middleware.ApprovalResult{Approved: approved}
	if !approved {
		result.Reason = c.Query("reason")
	}
	if !engine.PushSessionDecision(checkpointID, result) {
		c.JSON(http.StatusNotFound, gin.H{"error": "session not found or expired"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
