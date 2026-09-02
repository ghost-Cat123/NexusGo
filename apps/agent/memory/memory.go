package memory

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/pkg/config"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"time"

	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/logger"
	"github.com/cloudwego/eino/schema"
)

const (
	// WindowSize 触发摘要压缩的阈值（Redis List 长度达到此值时触发）
	WindowSize = 20
	// KeepAfterCompress 压缩后保留的最新消息条数
	KeepAfterCompress = 15
)

// RedisStore 单例 store，负责创建 session
type RedisStore struct {
	TTL time.Duration
}

func NewRedisStore(ttl time.Duration) *RedisStore {
	return &RedisStore{TTL: ttl}
}

// GetOrCreate 通过 sessionID 创建或恢复 Session
// sessionID 格式示例："agent_session:12345"（外部已含前缀）
func (s *RedisStore) GetOrCreate(sessionID string, userID int64) *Session {
	return &Session{
		ID:     sessionID,
		Key:    "memory:" + sessionID, // Redis key，与 sessionID 解耦，避免 key 双重前缀
		UserID: userID,
		Store:  s,
	}
}

// Session 存储某一用户的上下文
type Session struct {
	ID     string
	Key    string // Redis List key
	UserID int64
	Store  *RedisStore
}

var appendScript = redis.NewScript(`
	redis.call('RPush', KEYS[1], ARGV[1])
	redis.call('EXPIRE', KEYS[1], ARGV[2])
	return 1
`)

// Append 将一条消息追加到 Redis List，并在达到阈值时异步触发摘要压缩
func (s *Session) Append(ctx context.Context, msg *schema.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("[Memory] 消息序列化失败: %w", err)
	}
	_, err = appendScript.Run(ctx, cache.GetCache(), []string{s.Key}, data, int64(s.Store.TTL.Seconds())).Result()
	if err != nil {
		return fmt.Errorf("[Memory] 消息追加失败: %w", err)
	}
	// 检查是否需要触发摘要压缩（异步，不阻塞当前请求）
	count, _ := cache.GetCache().LLen(ctx, s.Key).Result()
	if count >= WindowSize {
		go s.triggerSummaryCompress()
	}
	return nil
}

// GetMessages 读取 Redis List 中全部消息并反序列化
func (s *Session) GetMessages(ctx context.Context) ([]*schema.Message, error) {
	strs, err := cache.GetCache().LRange(ctx, s.Key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis LRange 失败: %w", err)
	}

	var history []*schema.Message
	for _, str := range strs {
		var msg schema.Message
		if err := json.Unmarshal([]byte(str), &msg); err == nil {
			history = append(history, &msg)
		}
	}
	return history, nil
}

var summaryScript = redis.NewScript(`
	redis.call('LTRIM', KEYS[1], ARGV[2], -1)
	redis.call('LPUSH', KEYS[1], ARGV[1])
	redis.call('EXPIRE', KEYS[1], ARGV[3])
	return 1
`)

// triggerSummaryCompress 从 Redis 取出最旧的一批消息，调用 LLM 摘要，写入 MySQL，并替换 Redis 中的旧消息
// 此函数在独立 goroutine 中运行，使用 Background context 与请求生命周期解耦
func (s *Session) triggerSummaryCompress() {
	ctx := context.Background()

	toCompress := WindowSize - KeepAfterCompress // 需要被压缩的条数

	// 1. 取出最旧的 toCompress 条消息
	strs, err := cache.GetCache().LRange(ctx, s.Key, 0, int64(toCompress-1)).Result()
	if err != nil || len(strs) == 0 {
		logger.Log.Warnf("[Memory] 摘要压缩：读取旧消息失败或为空, key=%s", s.Key)
		return
	}

	// 2. 反序列化需要压缩的旧消息
	var msgs []*schema.Message
	for _, str := range strs {
		var m schema.Message
		if json.Unmarshal([]byte(str), &m) == nil {
			msgs = append(msgs, &m)
		}
	}
	if len(msgs) == 0 {
		return
	}

	// 3. 调用 LLM 生成摘要
	summary, err := SummarizeWithLLM(ctx, msgs)
	if err != nil {
		logger.Log.Errorf("[Memory] LLM 摘要生成失败: %v", err)
		return
	}
	logger.Log.Infof("[Memory] 用户 [%d] 摘要生成成功，类型=%s 重要性=%d 长度=%d",
		s.UserID, summary.Type, summary.Importance, len(summary.Summary))

	// 4. 落库 MySQL（持久化摘要）
	if err := dao.SaveSummary(s.UserID, s.ID, summary.Summary, summary.Type, summary.Tags, int8(summary.Importance)); err != nil {
		logger.Log.Errorf("[Memory] 摘要落库失败: %v", err)
		// 落库失败不影响继续压缩 Redis（避免 Redis 无限增长）
	}

	// 同时将摘要放入向量数据库
	if err := embeddingMemoryToMilvus(ctx, s.UserID, s.ID, summary.Summary, summary.Type, int8(summary.Importance)); err != nil {
		logger.Log.Warnf("[Memory] Milvus 记忆索引失败已降级为普通摘要: %v", err)
	}

	// 5. 原子性替换 Redis：
	//    - 删去已压缩的旧消息（LTrim 保留 toCompress 之后的部分）
	//    - 在头部插入一条 SystemMessage 作为摘要占位
	summaryMsg := schema.SystemMessage(fmt.Sprintf("[历史摘要] %s", summary))
	summaryData, _ := json.Marshal(summaryMsg)

	_, err = summaryScript.Run(ctx, cache.GetCache(), []string{s.Key}, summaryData, toCompress, int64(s.Store.TTL.Seconds())).Result()
	if err != nil {
		logger.Log.Errorf("[Memory] 压缩后 Redis 更新失败: %v", err)
		return
	}
	logger.Log.Infof("[Memory] 用户 [%d] 摘要压缩完成，压缩了 %d 条消息", s.UserID, toCompress)
}

func embeddingMemoryToMilvus(ctx context.Context, userID int64, sessionID string, summary string, memoryType string, importance int8) error {
	// 1. embedder
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		logger.Log.Warnf("[Memory] Embedder获取失败: %v", err)
		return err
	}

	// 2. indexer
	indexer, err := vdb.GetIndxer(ctx, vdb.MemoryCollection, embedder, vdb.MemoryDocumentConverter)
	if err != nil {
		logger.Log.Warnf("[Memory] Indexer获取失败: %v", err)
		return err
	}

	// 3. doc
	doc := &schema.Document{
		ID:      uuid.NewString(),
		Content: summary,
		MetaData: map[string]any{
			"user_id":     userID,
			"session_id":  sessionID,
			"memory_type": memoryType,
			"importance":  int64(importance),
			"create_time": time.Now().Unix(),
		},
	}

	// 4. 写入
	_, err = indexer.Store(ctx, []*schema.Document{doc})
	if err != nil {
		logger.Log.Warnf("[Memory] 向量数据库写入失败: %v", err)
		return err
	}
	return nil
}
