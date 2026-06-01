package ws

import (
	"context"
	"fmt"
	"time"

	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/utils"
	"github.com/go-redis/redis/v8"
)

// ===================== 1. 通用接口 =====================
//
// 设计约定：
//   - MsgID：int64，全局唯一（雪花），与 MySQL messages.msg_id、客户端展示一致。
//   - SeqID：int64，仅在某会话（ConvID）内单调递增，用于同会话内排序；不同会话可重复。
//   - Redis ZSet member 存字符串形式的 MsgID，与 Lua unpack 返回值一致。

// ReliabilityManager 消息可靠性管理器接口
type ReliabilityManager interface {
	GetNextSeqID(ctx context.Context, conversationID string, participantID int64) (int64, error)
	GenerateMsgID(ctx context.Context, conversationID string, seqID int64) int64
	// IsDuplicate(ctx context.Context, msgID int64) (bool, error)
	// AddPending(ctx context.Context, msg *PendingMessage) error
	// Ack(ctx context.Context, msgID int64) error
	// StartRetryLoop(ctx context.Context)
	// SetRetryCallback(callback retryCallback)
}

// // PendingMessage 待确认投递（网关到对端 WebSocket 写成功前可重试）
// type PendingMessage struct {
// 	MsgID       int64         `json:"msg_id"`        // 全局唯一（雪花），非自增；由网关或 Logic 在插入前赋值
// 	ConvID      string        `json:"conv_id"`       // 会话 ID
// 	SenderID    int64         `json:"sender_id"`     // 发送方 ID
// 	ReceiverIDs []int64       `json:"receiver_ids"`  // 接收方 IDs 保证群聊
// 	Content     string        `json:"content"`       // 消息内容
// 	SeqID       int64         `json:"seq_id"`        // 会话内序号，不同会话可重复
// 	ChatType    string        `json:"chat_type"`     // 聊天类型
// 	RetryCount  int           `json:"retry_count"`   // 重试次数
// 	MaxRetry    int           `json:"max_retry"`     // 最大重试次数
// 	NextRetryAt time.Time     `json:"next_retry_at"` // 下次重试时间
// 	Backoff     time.Duration `json:"backoff"`       // 退避时间
// }

// type retryCallback func(ctx context.Context, msg *PendingMessage) bool

// RedisReliabilityManager 基于 Redis 的实现（无状态网关多实例安全：Lua 抢占任务）
type RedisReliabilityManager struct {
	rdb    *redis.Client
	config *ReliabilityConfig
	// retryCallback
}

// ReliabilityConfig 配置
type ReliabilityConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxRetry       int
	DedupWindow    time.Duration
}

// DefaultReliability 网关进程内默认管理器，由 InitGatewayReliability 赋值
var DefaultReliability *RedisReliabilityManager

// NewRedisReliabilityManager 创建管理器
func NewRedisReliabilityManager(config *ReliabilityConfig) *RedisReliabilityManager {
	if config == nil {
		config = &ReliabilityConfig{
			InitialBackoff: time.Second,
			MaxBackoff:     time.Minute * 2,
			MaxRetry:       8,
			DedupWindow:    time.Hour * 24,
		}
	}
	return &RedisReliabilityManager{
		rdb:    cache.GetCache(),
		config: config,
	}
}

var _ ReliabilityManager = (*RedisReliabilityManager)(nil)

// InitGatewayReliability 初始化默认管理器、回调并启动重试协程
func InitGatewayReliability(cfg *ReliabilityConfig) {
	DefaultReliability = NewRedisReliabilityManager(cfg)
	// DefaultReliability.SetRetryCallback(retryPushToReceivers)
	// go DefaultReliability.StartRetryLoop(ctx)
}

// GetNextSeqID 会话内单调序号（按会话 + 参与者维度 INCR）
func (m *RedisReliabilityManager) GetNextSeqID(ctx context.Context, conversationID string, participantID int64) (int64, error) {
	key := fmt.Sprintf("seq:%s:%d", conversationID, participantID)
	// 会话id
	return m.rdb.Incr(ctx, key).Result()
}

// GenerateMsgID 生成全局唯一 MsgID（雪花）seqID 仅由调用方写入
func (m *RedisReliabilityManager) GenerateMsgID(_ context.Context, _ string, _ int64) int64 {
	return utils.GetSnowflake().Generate()
}

// onTransportDelivered 网关已成功 WriteMessage 后回调，用于将 DB send_status 0→1（可选）
var onTransportDelivered func(ctx context.Context, senderID, msgID int64)

// SetOnTransportDelivered 由 main 注入，避免 reliability 包直接依赖 RPC 客户端
func SetOnTransportDelivered(f func(ctx context.Context, senderID, msgID int64)) {
	onTransportDelivered = f
}

// func (m *RedisReliabilityManager) moveToDLQ(ctx context.Context, msg *PendingMessage) {
// 	logger.Log.Infof("Message moved to DLQ: %d, retry count: %d", msg.MsgID, msg.RetryCount)
// 	dlqKey := "dlq:msgs"
// 	msgJson, _ := json.Marshal(msg)
// 	_ = m.rdb.LPush(ctx, dlqKey, msgJson).Err()
// 	_ = m.rdb.Del(ctx, pendingDetailKey(msg.MsgID)).Err()
// }

// Ack 删除 pending 详情并从 ZSet 移除（member 与 AddPending 一致）
// func (m *RedisReliabilityManager) Ack(ctx context.Context, msgID int64) error {
// 	key := "pending:msgs"
// 	detailKey := pendingDetailKey(msgID)
// 	if _, err := m.rdb.Get(ctx, detailKey).Result(); err != nil {
// 		if errors.Is(err, redis.Nil) {
// 			return nil
// 		}
// 		return fmt.Errorf("get message detail failed: %v", err)
// 	}
// 	member := strconv.FormatInt(msgID, 10)
// 	pipe := m.rdb.Pipeline()
// 	pipe.ZRem(ctx, key, member)
// 	pipe.Del(ctx, detailKey)
// 	_, err := pipe.Exec(ctx)
// 	return err
// }

// // IsDuplicate 对同一全局 MsgID 的去重占位（SetNX + TTL），用于客户端重试携带同一 msg_id 等场景
// func (m *RedisReliabilityManager) IsDuplicate(ctx context.Context, msgID int64) (bool, error) {
// 	ok, err := m.rdb.SetNX(ctx, dedupKey(msgID), "1", m.config.DedupWindow).Result()
// 	if err != nil {
// 		return false, err
// 	}
// 	// SetNX 成功表示首次见到该 msg_id → 非重复；已存在则重复
// 	return !ok, nil
// }

// // AddPending 写入详情 + ZSet（member 为十进制 MsgID 字符串）
// func (m *RedisReliabilityManager) AddPending(ctx context.Context, msg *PendingMessage) error {
// 	if msg.MaxRetry <= 0 {
// 		msg.MaxRetry = m.config.MaxRetry
// 	}
// 	msgJson, err := json.Marshal(msg)
// 	if err != nil {
// 		return fmt.Errorf("消息序列化错误: %v", err)
// 	}
// 	key := "pending:msgs"
// 	member := strconv.FormatInt(msg.MsgID, 10)
// 	z := &redis.Z{
// 		Score:  float64(msg.NextRetryAt.Unix()),
// 		Member: member,
// 	}
// 	detailKey := pendingDetailKey(msg.MsgID)
// 	pipe := m.rdb.Pipeline()
// 	pipe.Set(ctx, detailKey, msgJson, 24*time.Hour)
// 	pipe.ZAdd(ctx, key, z)
// 	_, err = pipe.Exec(ctx)
// 	return err
// }

// // StartRetryLoop 定时扫描（多实例下由 Lua 原子抢占，避免同一任务被多机同时投递）
// func (m *RedisReliabilityManager) StartRetryLoop(ctx context.Context) {
// 	ticker := time.NewTicker(1 * time.Second)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-ticker.C:
// 			m.processRetry(ctx)
// 		}
// 	}
// }

// func (m *RedisReliabilityManager) processRetry(ctx context.Context) {
// 	now := time.Now().Unix()
// 	batchSize := 50

// 	// 1. 从ZSet获取到期消息
// 	msgIDStrs, err := m.rdb.ZRangeByScore(ctx, "pending:msgs", &redis.ZRangeBy{
// 		Min:    "-inf",
// 		Max:    strconv.FormatInt(now, 10),
// 		Offset: 0,
// 		Count:  int64(batchSize),
// 	}).Result()

// 	if err != nil && !errors.Is(err, redis.Nil) {
// 		logger.Log.Errorf("获取需要重试的消息失败: %v", err)
// 		return
// 	}

// 	for _, msgIDStr := range msgIDStrs {
// 		msgID, err := strconv.ParseInt(msgIDStr, 10, 64)
// 		if err != nil {
// 			logger.Log.Errorf("解析重试消息 ID 失败: %v", err)
// 			continue
// 		}
// 		// 2. 生成分布式锁
// 		lockKey := fmt.Sprintf("lock:msg:%d", msgID)
// 		// 3. 尝试获取分布式锁
// 		locked, err := m.rdb.SetNX(ctx, lockKey, "1", 30*time.Second).Result()
// 		if err != nil {
// 			logger.Log.Errorf("获取分布式锁失败: %v", err)
// 			continue
// 		}
// 		if !locked {
// 			// 已被其他实例获取，跳过
// 			continue
// 		}

// 		// 4. 原子删除ZSet中的消息
// 		pipe := m.rdb.Pipeline()
// 		zremCmd := pipe.ZRem(ctx, "pending:msgs", msgIDStr)
// 		_, err = pipe.Exec(ctx)
// 		if err != nil {
// 			logger.Log.Errorf("删除 ZSet 消息失败: %v", err)
// 			m.rdb.Del(ctx, lockKey)
// 			continue
// 		}
// 		if zremCmd.Val() == 0 {
// 			m.rdb.Del(ctx, lockKey)
// 			continue
// 		}

// 		// 5. 获取消息详情
// 		detailKey := pendingDetailKey(msgID)
// 		msgJson, err := m.rdb.Get(ctx, detailKey).Result()
// 		if err != nil {
// 			if !errors.Is(err, redis.Nil) {
// 				logger.Log.Errorf("获取待重试消息详情失败: %v", err)
// 			}
// 			continue
// 		}
// 		var msg PendingMessage
// 		if err := json.Unmarshal([]byte(msgJson), &msg); err != nil {
// 			logger.Log.Errorf("消息解析失败: %v", err)
// 			continue
// 		}
// 		// 6. 处理消息
// 		var processed bool
// 		if m.retryCallback != nil {
// 			processed = m.retryCallback(ctx, &msg)
// 		} else {
// 			logger.Log.Errorf("retryCallback 未设置，消息 %d 处理失败", msg.MsgID)
// 			processed = false
// 		}

// 		// 7. 根据处理结果更新状态
// 		if processed {
// 			// 处理成功，删除详情
// 			if err := m.Ack(ctx, msg.MsgID); err != nil {
// 				logger.Log.Errorf("ACK 处理失败: %v", err)
// 			}
// 		} else {
// 			// 处理失败，更新重试信息
// 			m.updateRetryInfo(ctx, &msg)
// 		}

// 		// 8. 释放锁
// 		m.rdb.Del(ctx, lockKey)
// 	}
// }

// func (m *RedisReliabilityManager) updateRetryInfo(ctx context.Context, msg *PendingMessage) {
// 	msg.RetryCount++
// 	if msg.RetryCount > msg.MaxRetry {
// 		m.moveToDLQ(ctx, msg)
// 		return
// 	}
// 	backoff := m.config.InitialBackoff
// 	for i := 0; i < msg.RetryCount; i++ {
// 		backoff *= 2
// 		// 超出最大退避时间
// 		if backoff > m.config.MaxBackoff {
// 			backoff = m.config.MaxBackoff
// 			break
// 		}
// 	}
// 	msg.Backoff = backoff
// 	// 下次重试时间
// 	msg.NextRetryAt = time.Now().Add(backoff)

// 	if err := m.AddPending(ctx, msg); err != nil {
// 		logger.Log.Errorf("退避重新入队失败: %v", err)
// 	}
// }

// func (m *RedisReliabilityManager) SetRetryCallback(callback retryCallback) {
// 	m.retryCallback = callback
// }

// // retryPushToReceivers 默认重试：向在线接收方再次 WriteMessage（JSON）
// func retryPushToReceivers(ctx context.Context, msg *PendingMessage) bool {
// 	payload, err := json.Marshal(map[string]interface{}{
// 		"chat_type": "chat_push",
// 		"msg_id":    msg.MsgID,
// 		"seq_id":    msg.SeqID,
// 		"from":      msg.SenderID,
// 		"content":   msg.Content,
// 	})
// 	if err != nil {
// 		return false
// 	}
// 	ok := true
// 	for _, rid := range msg.ReceiverIDs {
// 		client, exists := GlobalCliMap.Get(strconv.FormatInt(rid, 10))
// 		if !exists {
// 			ok = false
// 			continue
// 		}
// 		client.SendMessage(payload)
// 	}
// 	if ok && onTransportDelivered != nil {
// 		onTransportDelivered(ctx, msg.SenderID, msg.MsgID)
// 	}
// 	return ok
// }
