package middleware

import (
	"NexusGo/apps/pkg/cache"
	"context"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/go-redis/redis/v8"
	"time"
)

// 基于redis的zset限流

type RateLimitMiddleware struct {
	*adk.BaseChatModelAgentMiddleware
}

func (m *RateLimitMiddleware) BeforeAgent(
	ctx context.Context, runCtx *adk.ChatModelAgentContext,
) (context.Context, *adk.ChatModelAgentContext, error) {
	// 取出当前userID 作为value
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
	1. 步骤1：删除【滑动窗口外】的所有过期请求
	2. 步骤2：统计当前窗口内的请求总数
	3. 步骤3：判断是否超限 → 超限直接返回0（拒绝请求）
	4. 步骤4：未超限 → 添加当前请求到ZSet
	5. 步骤5：刷新Key过期时间（防止闲置数据占用内存）
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
	// 1. 生成Redis Key：按用户ID隔离，实现【用户级限流】
	redisKey := fmt.Sprintf("rate_limit:ai:%d", userID)
	// 2. 当前时间（纳秒级，保证唯一）
	now := time.Now().UnixNano()
	// 3. 滑动窗口左边界：当前时间 - 1分钟（只保留最近1分钟的请求）
	oneMinuitAgo := now - time.Minute.Nanoseconds()
	// 4. 限流阈值：1分钟最多5次请求
	limit := int64(5)
	// 5. Key过期时间：60秒（无请求时自动删除，节约内存）
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
