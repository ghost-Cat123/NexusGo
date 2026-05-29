package cache

import (
	"context"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"sync"

	"github.com/go-redis/redis/v8"
)

var (
	redisClient *redis.Client
	once        sync.Once
	initError   error
)

func InitRedis(config config.RedisConfig) error {
	once.Do(func() {
		ctx := context.Background()
		redisClient = redis.NewClient(&redis.Options{
			Addr:     config.Addr,     // Redis 服务器地址和端口
			Password: config.Password, // Redis 访问密码，如果没有可以为空字符串
			DB:       config.DB,       // 使用的 Redis 数据库编号，默认为 0
		})

		// 使用 Ping() 方法测试是否成功连接到 Redis 服务器
		var pong string
		pong, initError = redisClient.Ping(ctx).Result()
		if initError != nil {
			return
		}
		logger.Log.Info("Redis单例初始化成功！", pong)
	})
	return initError
}

func GetCache() *redis.Client {
	if redisClient == nil {
		panic("redis client not initialized")
	}
	return redisClient
}

func CloseRedis() {
	err := redisClient.Close()
	if err != nil {
		logger.Log.Errorf("Redis关闭失败%v", err)
	}
}
