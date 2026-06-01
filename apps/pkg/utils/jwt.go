package utils

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"time"
)

// JwtSecret 签名密钥 (生产环境中绝对不能硬编码，必须从配置文件/环境变量读取！)
var JwtSecret = []byte("NexusGo-super-secret-key-2026")

// CustomClaims 自定义 JWT 载荷
type CustomClaims struct {
	UserID int64 `json:"user_id"`
	// 你可以在这里加更多字段，比如 Role, Username
	jwt.RegisteredClaims // 内置标准字段 (包含过期时间等)
}

// GenerateToken 生成 JWT Token (登录成功后调用)
func GenerateToken(userID int64) (string, error) {
	// 设置 Token 7 天后过期
	expirationTime := time.Now().Add(7 * 24 * time.Hour)

	claims := CustomClaims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "go-im-gateway", // 签发者
		},
	}

	// 使用 HS256 算法生成 Token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 进行签名
	return token.SignedString(JwtSecret)
}

// ParseToken 解析并校验 JWT Token (网关拦截器调用)
func ParseToken(tokenString string) (int64, error) {
	// 解析 Token
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return JwtSecret, nil
	})

	if err != nil {
		// 捕捉具体的过期错误
		if errors.Is(err, jwt.ErrTokenExpired) {
			return 0, errors.New("token 已过期，请重新登录")
		}
		return 0, errors.New("无效的 token")
	}

	// 提取出我们自定义的 Claims
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims.UserID, nil
	}

	return 0, errors.New("无法解析 token")
}
