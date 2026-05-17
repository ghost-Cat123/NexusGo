package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-im-system/apps/logic/dao"
	"go-im-system/apps/logic/models"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/mq"
	"go-im-system/apps/pkg/utils"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	uploadWorkers   = 50
	batchSize       = 50 // 积满 50 条触发批量插入
	batchFlushMs    = 30 // 最多等 30ms，兼顾低流量防卡和高流量低延迟
	numBatchWorkers = 4  // 并行 batchInsertWorker 数，消耗能力 = 单个的 4 倍
)

// batchItem 批量写入的最小单元
type batchItem struct {
	msg     *models.Messages
	payload *mq.UploadPayload
	d       amqp.Delivery
}

// StartUploadConsumer 启动上行消息消费者（Gateway → MQ → Logic）
func StartUploadConsumer() {
	msgs, err := mq.ConsumeUploadQueue("logic.upload.queue", "upload.all")
	if err != nil {
		logger.Log.Fatalf("上行消费者启动失败: %v", err)
	}
	logger.Log.Infof("✅ 上行消费者已启动，Queue: logic.upload.queue, Workers: %d", uploadWorkers)

	// batchCh 容量翻倍，应对突发流量
	batchCh := make(chan batchItem, batchSize*uploadWorkers*2)
	// 启动多个并行 batchInsertWorker，消耗能力 = numBatchWorkers 倍
	for i := 0; i < numBatchWorkers; i++ {
		go batchInsertWorker(batchCh)
	}

	for i := 0; i < uploadWorkers; i++ {
		go consumeUploadLoop(msgs, batchCh)
	}
}

func consumeUploadLoop(msgs <-chan amqp.Delivery, batchCh chan<- batchItem) {
	for d := range msgs {
		handleUploadDelivery(d, batchCh)
	}
	logger.Log.Warn("上行消费通道已关闭")
}

func handleUploadDelivery(d amqp.Delivery, batchCh chan<- batchItem) {
	var payload mq.UploadPayload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		logger.Log.Errorf("[Upload] Payload 解析失败，丢弃消息: %v", err)
		_ = d.Nack(false, false)
		return
	}

	switch payload.ChatType {
	case mq.ChatTypeSingle:
		// 单聊消息进批量写入管道
		msg := models.NewMessages(payload.SenderID, payload.ReceiverID, payload.GroupID, payload.Content, false)
		msg.MsgId = payload.MsgID
		msg.SeqId = payload.SeqID
		batchCh <- batchItem{msg: msg, payload: &payload, d: d}

	case mq.ChatTypeGroup:
		// 群聊消息写扩散，继续走单条逻辑（群消息量少，不是瓶颈）
		if err := handleGroupChat(&payload); err != nil {
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)

	case mq.ChatTypeAI:
		if err := handleAIChat(&payload); err != nil {
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)

	default:
		// 未知类型，按单聊兜底
		msg := models.NewMessages(payload.SenderID, payload.ReceiverID, payload.GroupID, payload.Content, false)
		msg.MsgId = payload.MsgID
		msg.SeqId = payload.SeqID
		batchCh <- batchItem{msg: msg, payload: &payload, d: d}
	}
}

// batchInsertWorker 积攒消息批量写库，写完后异步推下行
// 策略：满 batchSize 条 OR 超过 batchFlushMs 毫秒，触发一次批量 INSERT
func batchInsertWorker(batchCh <-chan batchItem) {
	ticker := time.NewTicker(time.Duration(batchFlushMs) * time.Millisecond)
	defer ticker.Stop()

	buf := make([]batchItem, 0, batchSize)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		items := buf
		buf = make([]batchItem, 0, batchSize)

		// 批量插入 MySQL（一次 INSERT ... VALUES (...),(...),...）
		msgs := make([]*models.Messages, len(items))
		for i, it := range items {
			msgs[i] = it.msg
		}
		// 批量 MGet 获取所有接收者的路由地址（极大降低 Redis 并发请求）
		ctx := context.Background()
		receiverMap := make(map[int64]string)
		var redisKeys []string
		var receiverIDs []int64
		for _, it := range items {
			if _, exists := receiverMap[it.payload.ReceiverID]; !exists {
				receiverMap[it.payload.ReceiverID] = ""
				redisKeys = append(redisKeys, "route:user:"+strconv.FormatInt(it.payload.ReceiverID, 10))
				receiverIDs = append(receiverIDs, it.payload.ReceiverID)
			}
		}

		if len(redisKeys) > 0 {
			vals, err := cache.GetCache().MGet(ctx, redisKeys...).Result()
			if err == nil {
				for i, val := range vals {
					if strVal, ok := val.(string); ok {
						receiverMap[receiverIDs[i]] = strVal
					}
				}
			} else {
				logger.Log.Errorf("[Batch] Redis MGet 失败: %v", err)
			}
		}

		if err := dao.BatchInsertMessages(msgs); err != nil {
			// 批量插入失败：降级为逐条单插（隔离脏数据，不能全部 Nack）
			logger.Log.Warnf("[Batch] 批量插入失败，降级单条: %v", err)
			for _, it := range items {
				addr := receiverMap[it.payload.ReceiverID]
				if sErr := dao.InsertMessage(it.msg); sErr != nil {
					if isDuplicateKey(sErr) {
						// 重复键：消息在之前已落库（幂等重试场景），视为成功，推下行 + ACK
						logger.Log.Infof("[Batch] 消息 %d 已存在，幂等跳过", it.msg.MsgId)
						go pushDownAndAckWithAddr(it, addr)
					} else {
						// 真实错误（磁盘满、连接断等），送死信队列
						logger.Log.Errorf("[Batch] 消息 %d 单条插入失败，送死信: %v", it.msg.MsgId, sErr)
						_ = it.d.Nack(false, false)
					}
					continue
				}
				go pushDownAndAckWithAddr(it, addr)
			}
			return
		}

		// 批量插入成功：并发推下行 + 单条独立 ACK
		// 不用 multiple=true，避免多 worker 并行时 delivery tag 乱序导致漏 ACK
		for _, it := range items {
			addr := receiverMap[it.payload.ReceiverID]
			go pushDownAndAckWithAddr(it, addr)
		}
		logger.Log.Debugf("[Batch] 批量写入 %d 条成功", len(items))
	}

	for {
		select {
		case item, ok := <-batchCh:
			if !ok {
				flush()
				return
			}
			buf = append(buf, item)
			if len(buf) >= batchSize {
				flush() // 满了立刻刷
			}
		case <-ticker.C:
			flush() // 定时兜底刷
		}
	}
}

// pushDownWithAddr 用已查出的网关地址推送下行
func pushDownWithAddr(payload *mq.UploadPayload, gatewayAddr string) {
	if gatewayAddr == "" {
		return
	}
	ctx := context.Background()
	downPayload := &mq.DownPayload{
		MsgID:      payload.MsgID,
		SeqID:      payload.SeqID,
		SenderID:   payload.SenderID,
		ReceiverID: payload.ReceiverID,
		Content:    payload.Content,
		ChatType:   payload.ChatType,
	}
	body, _ := json.Marshal(downPayload)
	if err := mq.PublishDown(ctx, gatewayAddr, body); err != nil {
		logger.Log.Warnf("[Upload] 下行 Publish 失败（已落库）: %v", err)
	}
}

// pushDownAndAckWithAddr 使用现成地址推送+确认
func pushDownAndAckWithAddr(it batchItem, gatewayAddr string) {
	pushDownWithAddr(it.payload, gatewayAddr)
	_ = it.d.Ack(false)
	// 下行前异步写入向量数据消费者队列
	vecPayload := mq.VectorPayload{
		MsgID:    it.msg.MsgId,
		ConvID:   it.payload.ConvID,
		SenderID: it.payload.SenderID,
		SendTime: time.Now().Unix(),
		Content:  it.payload.Content,
	}
	if body, _ := json.Marshal(vecPayload); len(body) > 0 {
		_ = mq.PublishVector(context.Background(), "vector.all", body)
	}
}

// isDuplicateKey 判断是否是 MySQL 主键/唯一键冲突错误（Error 1062）
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "1062") ||
		strings.Contains(err.Error(), "Duplicate entry")
}

// handleGroupChat 群聊消息写扩散（保持不变）
func handleGroupChat(payload *mq.UploadPayload) error {
	ctx := context.Background()
	members, err := dao.GetGroupMembers(payload.GroupID)
	if err != nil {
		return fmt.Errorf("[Upload] 查询群成员失败: %v", err)
	}
	for _, memberID := range members {
		// 准备要插入的消息
		msg := models.NewMessages(payload.SenderID, memberID, payload.GroupID, payload.Content, false)
		msg.MsgId = utils.GetSnowflake().Generate()
		msg.SeqId = payload.SeqID
		// 接收者为自己
		if memberID == payload.SenderID {
			// sender自身：落库，不下行，用作向量数据库锚点
			msg.IsRead = true
			msg.SendStatus = models.SendStatusSentConfirmed
			err := dao.InsertMessage(msg)
			if err != nil {
				logger.Log.Errorf("[Upload][Group] 向量锚点消息落库失败: %v", err)
				continue
			}
			// 使用该MsgID存入向量数据库
			vecID := msg.MsgId
			vecPayload := mq.VectorPayload{
				MsgID:    vecID,
				ConvID:   payload.ConvID,
				SenderID: payload.SenderID,
				SendTime: time.Now().Unix(),
				Content:  payload.Content,
			}
			if body, _ := json.Marshal(vecPayload); len(body) > 0 {
				_ = mq.PublishVector(ctx, "vector.all", body)
			}
		}
		// 其他用户正常走数据库并下行
		if err := dao.InsertMessage(msg); err != nil {
			logger.Log.Errorf("[Upload][Group] 群成员 %d 落库失败: %v", memberID, err)
			continue
		}
		redisKey := "route:user:" + strconv.FormatInt(memberID, 10)
		gatewayAddr, err := cache.GetCache().Get(ctx, redisKey).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			logger.Log.Errorf("[Upload][Group] 查询 Redis 出错: %v", err)
			continue
		}
		downPayload := mq.DownPayload{
			MsgID:      msg.MsgId,
			SeqID:      payload.SeqID,
			GroupID:    payload.GroupID,
			SenderID:   payload.SenderID,
			ReceiverID: memberID,
			Content:    payload.Content,
			ChatType:   payload.ChatType,
		}
		body, _ := json.Marshal(downPayload)
		if pubErr := mq.PublishDown(ctx, gatewayAddr, body); pubErr != nil {
			logger.Log.Warnf("[Upload][Group] 群成员 %d 下行 Publish 失败: %v", memberID, pubErr)
		}
	}
	return nil
}

// handleAIChat AI 消息处理
func handleAIChat(payload *mq.UploadPayload) error {
	message := models.NewMessages(payload.SenderID, payload.ReceiverID, payload.GroupID, payload.Content, true)
	message.MsgId = payload.MsgID
	message.SeqId = payload.SeqID
	if err := dao.InsertMessage(message); err != nil {
		return fmt.Errorf("[Upload] AI消息落库失败: %v", err)
	}
	return nil
}
