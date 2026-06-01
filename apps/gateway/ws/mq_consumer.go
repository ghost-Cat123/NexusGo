package ws

import (
	"context"
	"encoding/json"
	"strconv"

	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

const downWorkers = 50

// StartMQConsumer 声明该网关实例的专属 Queue，绑定到 Exchange，启动多个消费协程。
// gatewayAddr 示例: "127.0.0.1:8080"（与 Redis route:user:<id> 的值保持一致）
func StartMQConsumer(gatewayAddr string) {
	queueName := "gateway.queue." + gatewayAddr
	msgs, err := mq.ConsumeDownQueue(queueName, gatewayAddr)
	if err != nil {
		logger.Log.Fatalf("MQ 消费者启动失败: %v", err)
	}
	logger.Log.Infof("✅ MQ 消费者已启动，Queue: %s, Workers: %d", queueName, downWorkers)

	for i := 0; i < downWorkers; i++ {
		go consumeLoop(msgs)
	}
}

// consumeLoop 阻塞消费，每条消息独立处理，不会因单条失败而崩溃。
func consumeLoop(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		handleMQDelivery(d)
	}
	logger.Log.Warn("MQ 消费通道已关闭")
}

// handleMQDelivery 处理单条 MQ 消息：
//  1. 解析 PushPayload
//  2. 在本地 CliMap 找到接收方 WebSocket 连接
//  3. 写入消息并手动 ACK
//     若接收方已离线（如用户在消息发出和 MQ 到达之间断开），NACK 且不重入队；
//     接收方上线时将通过 SyncUnread 拉取离线消息，无需 MQ 重试。
func handleMQDelivery(d amqp.Delivery) {
	var payload mq.DownPayload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		logger.Log.Errorf("[MQ] Payload 解析失败，丢弃消息: %v", err)
		_ = d.Nack(false, false) // 解析失败，不重入队
		return
	}

	receiverStr := strconv.FormatInt(payload.ReceiverID, 10)
	client, ok := GlobalCliMap.Get(receiverStr)
	if !ok {
		// 接收方已不在本节点（可能已离线），NACK 且不重入队
		// 离线消息已落库，用户上线时 SyncUnread 会推送
		logger.Log.Warnf("[MQ] 用户 [%d] 已不在本节点，已落库 直接ACK", payload.ReceiverID)
		_ = d.Ack(false)
		return
	}

	// 按前端协议组装 chat_push JSON
	pushMsg, err := marshalChatPush(payload.MsgID, payload.SeqID, payload.GroupID, payload.SenderID, payload.Content)
	if err != nil {
		logger.Log.Errorf("[MQ] 组装推送 JSON 失败: %v", err)
		_ = d.Nack(false, false)
		return
	}

	client.SendMessage(pushMsg)

	// 手动 ACK：消息已成功写入 WebSocket 缓冲区
	_ = d.Ack(false)
	logger.Log.Infof("[MQ] 消息推送成功: receiver=%d msgID=%d seqID=%d",
		payload.ReceiverID, payload.MsgID, payload.SeqID)

	// 投递确认 send_status 从 0（未发送）→ 1（已投递至网关 WS）
	if onTransportDelivered != nil {
		go onTransportDelivered(context.Background(), payload.SenderID, payload.MsgID)
	}
}
