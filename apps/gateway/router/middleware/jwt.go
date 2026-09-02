package middleware

import (
	"NexusGo/apps/pkg/utils"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// JWTAuthMiddleware 验证 Token 并将 UserID 存入 Context
func JWTAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "请求头中缺少 Authorization"})
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if !(len(parts) == 2 && parts[0] == "Bearer") {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "Authorization 格式错误"})
			c.Abort()
			return
		}

		tokenString := parts[1]
		userID, err := utils.ParseToken(tokenString)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": err.Error()})
			c.Abort()
			return
		}

		// 将用户ID存入上下文
		c.Set("user_id", userID)
		c.Next()
	}
}
