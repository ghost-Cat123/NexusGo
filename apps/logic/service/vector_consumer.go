package service

import (
	"context"
	"encoding/json"
	"fmt"
	milvusidx "github.com/cloudwego/eino-ext/components/indexer/milvus2"
	"github.com/cloudwego/eino/schema"
	amqp "github.com/rabbitmq/amqp091-go"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/mq"
	vdb "go-im-system/apps/pkg/vector_db"
	"time"
)

const vectorWorkers = 2
const vectorBatchSize = 20
const vectorFlushInterval = 5 * time.Second // 兜底 flush 间隔，防止小批量消息永远卡在内存

func StartVectorConsumer() {
	msgs, err := mq.ConsumeVectorQueue("logic.vector.queue", "vector.all")
	if err != nil {
		logger.Log.Fatalf("向量消费者启动失败: %v", err)
	}
	logger.Log.Infof("✅ 向量消费者已启动")
	// 批量异步消费
	for i := 0; i < vectorWorkers; i++ {
		go consumeVectorLoop(msgs)
	}
}

func consumeVectorLoop(msgs <-chan amqp.Delivery) {
	ctx := context.Background()
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		logger.Log.Fatalf("[Vector] Embedder 初始化失败: %v", err)
		return
	}
	indexer, err := vdb.GetIndxer(ctx, vdb.CollectionName, embedder)
	if err != nil {
		logger.Log.Fatalf("[Vector] Indexer 初始化失败: %v", err)
		return
	}

	batch := make([]amqp.Delivery, 0, vectorBatchSize)
	// 定时兜底 ticker：防止不足一批的消息永远停留在内存
	ticker := time.NewTicker(vectorFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				// 通道关闭，flush 剩余消息后退出
				if len(batch) > 0 {
					flushBatch(ctx, indexer, batch)
				}
				return
			}
			batch = append(batch, d)
			if len(batch) >= vectorBatchSize {
				flushBatch(ctx, indexer, batch)
				batch = batch[:0]
				ticker.Reset(vectorFlushInterval) // 重置计时器避免重复触发
			}
		case <-ticker.C:
			// 定时兜底：不管积了几条，时间到了就 flush
			if len(batch) > 0 {
				flushBatch(ctx, indexer, batch)
				batch = batch[:0]
			}
		}
	}
}

func flushBatch(ctx context.Context, indexer *milvusidx.Indexer, deliveries []amqp.Delivery) {
	docs := make([]*schema.Document, 0, len(deliveries))
	valid := make([]amqp.Delivery, 0, len(deliveries))

	for _, d := range deliveries {
		var p mq.VectorPayload
		if err := json.Unmarshal(d.Body, &p); err != nil {
			// 格式错误，进死信，不重试（避免消息无限循环）
			_ = d.Nack(false, false)
			continue
		}
		docs = append(docs, &schema.Document{
			ID:      fmt.Sprintf("%d", p.MsgID),
			Content: p.Content,
			MetaData: map[string]any{
				"msg_id":    fmt.Sprintf("%d", p.MsgID), // ← 补全 msg_id，documentConverter 读 MetaData 时需要
				"conv_id":   p.ConvID,
				"sender_id": p.SenderID,
				"send_time": p.SendTime,
			},
		})
		valid = append(valid, d)
	}

	if len(docs) == 0 {
		return
	}

	_, err := indexer.Store(ctx, docs)
	if err != nil {
		logger.Log.Errorf("[Vector] Indexer.Store 失败: %v，%d 条消息将重回队列", err, len(valid))
		// BUG FIX: 去掉 return，确保所有消息都被 Nack
		for _, d := range valid {
			_ = d.Nack(false, true)
		}
		return
	}

	// 批量确认
	for _, d := range valid {
		_ = d.Ack(false)
	}
	logger.Log.Debugf("[Vector] 批量写入成功: %d 条", len(valid))
}
