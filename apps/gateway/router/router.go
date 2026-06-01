package router

import (
	"NexusGo/apps/gateway/api/user_api"
	"NexusGo/apps/gateway/ws"
	"github.com/gin-gonic/gin"
	"net/http"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Requested-With")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Content-Type")
		c.Header("Access-Control-Allow-Credentials", "false")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func SetupRouter() *gin.Engine {
	r := gin.Default()

	_ = r.SetTrustedProxies(nil)
	r.Use(corsMiddleware())

	// 1. RESTful HTTP 路由组 处理短连接
	apiGroup := r.Group("/api")
	{
		apiGroup.POST("/login", user_api.UserLoginHandler)
		apiGroup.POST("/register", user_api.UserRegisterHandler)
	}

	// 2. 处理 websocket长连接路由
	r.GET("/ws", ws.Handler)

	return r
}
