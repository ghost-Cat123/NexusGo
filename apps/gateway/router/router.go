package router

import (
	"NexusGo/apps/gateway/api/chat_api"
	"NexusGo/apps/gateway/api/friend_api"
	"NexusGo/apps/gateway/api/group_api"
	"NexusGo/apps/gateway/api/upload_api"
	"NexusGo/apps/gateway/api/user_api"
	"NexusGo/apps/gateway/router/middleware"
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

	// 1. 公开 HTTP 路由组
	apiGroup := r.Group("/api")
	{
		apiGroup.POST("/login", user_api.UserLoginHandler)
		apiGroup.POST("/register", user_api.UserRegisterHandler)
	}

	// 2. 需要鉴权的 HTTP 路由组 (使用 JWT 中间件)
	authGroup := r.Group("/api")
	authGroup.Use(middleware.JWTAuthMiddleware())
	{
		// 2.1 用户模块
		authGroup.GET("/user/info", user_api.GetUserInfoHandler)
		authGroup.GET("/user/search", user_api.SearchUserHandler)
		authGroup.POST("/user/update", user_api.UpdateUserInfoHandler)

		// 2.2 好友模块
		authGroup.POST("/friend/apply", friend_api.ApplyFriendHandler)
		authGroup.POST("/friend/resolve", friend_api.ResolveFriendHandler)
		authGroup.GET("/friend/list", friend_api.GetFriendListHandler)
		authGroup.GET("/friend/pending", friend_api.GetPendingRequestsHandler)
		authGroup.POST("/friend/delete", friend_api.DeleteFriendHandler)

		// 2.3 单聊模块
		authGroup.GET("/chat/conversations", chat_api.GetConversationsHandler)
		authGroup.GET("/chat/messages", chat_api.GetChatMessagesHandler)
		authGroup.POST("/chat/read", chat_api.MarkMessageReadHandler)

		// 2.4 群聊模块
		authGroup.POST("/group/create", group_api.CreateGroupHandler)
		authGroup.GET("/group/list", group_api.GetGroupListHandler)
		authGroup.GET("/group/info", group_api.GetGroupInfoHandler)
		authGroup.GET("/group/members", group_api.GetGroupMembersHandler)
		authGroup.POST("/group/join", group_api.JoinGroupHandler)
		authGroup.POST("/group/leave", group_api.LeaveGroupHandler)
		authGroup.POST("/group/dissolve", group_api.DissolveGroupHandler)
		authGroup.GET("/group/search", group_api.SearchGroupHandler)
		authGroup.POST("/group/request_join", group_api.RequestJoinGroupHandler)
		authGroup.POST("/group/approve_join", group_api.ApproveJoinGroupHandler)
		authGroup.GET("/group/messages", group_api.GetGroupMessagesHandler)

		// 2.5 文件上传
		authGroup.POST("/upload", upload_api.UploadHandler)
		authGroup.POST("/group/:id/documents", group_api.UploadDocHandler) // 群文档上传
	}

	// 3. 处理 websocket长连接路由 (WS 可以在连接时验证 Token)
	r.GET("/ws", ws.Handler)

	// 4. 静态文件服务
	r.Static("/uploads", "./uploads")

	return r
}
