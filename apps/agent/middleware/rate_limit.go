package middleware

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/go-redis/redis/v8"
	"go-im-system/apps/pkg/cache"
	"time"
)

// 基于redis的zset限流

type RateLimitMiddleware struct {
	*adk.BaseChatModelAgentMiddleware
}

func (m *RateLimitMiddleware) BeforeAgent(
	ctx context.Context, runCtx *adk.ChatModelAgentContext,
) (context.Context, *adk.ChatModelAgentContext, error) {
	// 取出当前userID
	userID, _ := ctx.Value("current_user_id").(int64)
	// 是否限流
	err := checkRateLimit(ctx, userID)
	if err != nil {
		return ctx, runCtx, err
	}
	return ctx, runCtx, nil
}

// 定义Lua脚本（限流）
/*
	1. 删除过期数据
	2. 统计当前请求数
	3. 请求是否超限
	4. 写入请求
	5. 设置过期时间
*/
var rateLimitLua = redis.NewScript(`
	redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[2])
	local count = redis.call('ZCARD', KEYS[1])
	if tonumber(count) >= tonumber(ARGV[3]) then
		return 0
	end
	redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1])
	redis.call('EXPIRE', KEYS[1], ARGV[4])
	return 1
`)

// 原子性限流操作
func checkRateLimit(ctx context.Context, userID int64) error {
	redisKey := fmt.Sprintf("rate_limit:ai:%d", userID)
	now := time.Now().UnixNano()
	oneMinuitAgo := now - time.Minute.Nanoseconds()
	limit := int64(5)
	expire := int64(60)

	// 返回值0限流 1放行
	result, err := rateLimitLua.Run(ctx, cache.GetCache(), []string{redisKey}, now, oneMinuitAgo, limit, expire).Result()
	if err != nil {
		return fmt.Errorf("[redis error] %v", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("聊天太频繁啦，请休息一分钟后再试~")
	}
	return nil
}
