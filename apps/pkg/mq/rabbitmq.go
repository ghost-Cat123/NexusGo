package mq

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 下行交换机
const downExchangeName = "gateway.exchange"

// 上行交换机
const uploadExchangeName = "message.upload"

// 死信队列交换机
const deadExchangeName = "dead.letter.exchange"

// 向量数据交换机
const vectorExchangeName = "im.vector.index"

// 消息类型
const (
	ChatTypeSingle        = "single_chat"
	ChatTypeGroup         = "group_chat"
	ChatTypeAI            = "ai_chat"
	ChatTypeFriendRequest = "friend_request"
)

// DownPayload 下行消息体，Logic 生产、Gateway 消费。
// 字段与前端 chat_push 协议对齐，不涉及数据库字段变更。
type DownPayload struct {
	MsgID      int64  `json:"msg_id"`
	SeqID      int64  `json:"seq_id"`
	GroupID    int64  `json:"group_id"`
	SenderID   int64  `json:"sender_id"`
	ReceiverID int64  `json:"receiver_id"`
	Content    string `json:"content"`
	ChatType   string `json:"chat_type"`
}

// UploadPayload 上行消息体，Gateway 生产、Logic 消费。
type UploadPayload struct {
	MsgID      int64  `json:"msg_id"`
	SeqID      int64  `json:"seq_id"`
	ChatType   string `json:"chat_type"`
	GroupID    int64  `json:"group_id"`
	ConvID     string `json:"conv_id"`
	SenderID   int64  `json:"sender_id"`
	ReceiverID int64  `json:"receiver_id"`
	Content    string `json:"content"`
}

type VectorPayload struct {
	MsgID    int64  `json:"msg_id"`
	ConvID   string `json:"conv_id"`
	SenderID int64  `json:"sender_id"`
	SendTime int64  `json:"send_time"`
	Content  string `json:"content"`
}

const publishPoolSize = 20

var (
	conn       *amqp.Connection
	channel    *amqp.Channel   // 消费专用（Consume + Ack），不与 Publish 争抢
	publishChs []*amqp.Channel // Publish 通道池，轮询分发
	publishIdx uint64          // 轮询计数器
)

// InitRabbitMQ 建立连接并声明 Direct Exchange（持久化，重启不消失）。
// Logic 和 Gateway 启动时各自调用一次。
func InitRabbitMQ(url string) error {
	var err error
	conn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("RabbitMQ 连接失败: %w", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("创建 Channel 失败: %w", err)
	}

	// 声明上行 Exchange（Gateway → MQ → Logic）
	// Direct Exchange：所有 Logic 实例竞争消费同一个 Queue（负载均衡）
	if err = channel.ExchangeDeclare(
		uploadExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明上行 Exchange [%s] 失败: %w", uploadExchangeName, err)
	}

	// 声明下行 Exchange（Logic → MQ → 目标 Gateway）
	// Direct Exchange：RoutingKey = 目标 gatewayAddr，精准路由到对应网关 Queue
	if err = channel.ExchangeDeclare(
		downExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明下行 Exchange [%s] 失败: %w", downExchangeName, err)
	}

	if err = channel.ExchangeDeclare(
		vectorExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明向量 Exchange [%s] 失败: %w", vectorExchangeName, err)
	}

	logger.Log.Infof("✅ RabbitMQ 已连接，上行Exchange [%s] + 下行Exchange [%s] + 向量 Exchange [%s] 声明成功", uploadExchangeName, downExchangeName, vectorExchangeName)

	// 初始化死信队列
	err = channel.ExchangeDeclare(deadExchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("声明死信 Exchange 失败: %w", err)
	}
	_, err = channel.QueueDeclare("dead.letter.queue", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("声明 死信 Queue 失败: %w", err)
	}
	err = channel.QueueBind("dead.letter.queue", "dead.letter", deadExchangeName, false, nil)
	if err != nil {
		return fmt.Errorf("绑定 Queue 到 Exchange 失败: %w", err)
	}

	// 创建 Publish 通道池：消费和发布使用独立 Channel，互不阻塞
	publishChs = make([]*amqp.Channel, publishPoolSize)
	for i := 0; i < publishPoolSize; i++ {
		ch, e := conn.Channel()
		if e != nil {
			return fmt.Errorf("创建 Publish 通道 [%d] 失败: %w", i, e)
		}
		publishChs[i] = ch
	}
	logger.Log.Infof("✅ Publish 通道池已创建，Size: %d", publishPoolSize)

	return nil
}

// getPublishCh 轮询选取 Publish 通道，消除单 Channel 争抢瓶颈。
func getPublishCh() *amqp.Channel {
	i := atomic.AddUint64(&publishIdx, 1)
	return publishChs[i%uint64(len(publishChs))]
}

// PublishUpload 上行发布（Gateway → MQ → Logic）。
// routingKey 固定 "upload.all"，所有 Logic 实例竞争消费。
func PublishUpload(ctx context.Context, routingKey string, body []byte) error {
	return getPublishCh().PublishWithContext(
		ctx,
		uploadExchangeName, // exchange
		routingKey,         // routing key：目标网关地址
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // 持久化消息，Broker 重启不丢
			Body:         body,
		},
	)
}

// ConsumeUploadQueue Logic 调用，声明共享上行队列并返回消费通道。
func ConsumeUploadQueue(queueName, routingKey string) (<-chan amqp.Delivery, error) {
	// 声明队列
	q, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		// 死信队列参数
		amqp.Table{
			// 死信交换机和key
			"x-dead-letter-exchange":    deadExchangeName,
			"x-dead-letter-routing-key": "dead.letter",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("声明 Queue [%s] 失败: %w", queueName, err)
	}
	// 绑定队列
	if err = channel.QueueBind(q.Name, routingKey, uploadExchangeName, false, nil); err != nil {
		return nil, fmt.Errorf("绑定 Queue 到 Exchange 失败: %w", err)
	}

	// QoS：预取数量从 50 提高到 2000，避免高 QPS + 批量处理下的通道饥饿和积压
	if err = channel.Qos(2000, 0, false); err != nil {
		return nil, fmt.Errorf("设置 QoS 失败: %w", err)
	}

	// auto-ack=false：手动确认，确保消息被真正处理后再从 Queue 移除
	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("注册消费者失败: %w", err)
	}

	logger.Log.Infof("✅ Queue [%s] 已声明并绑定，RoutingKey: %s", queueName, routingKey)
	return msgs, nil
}

// PublishDown 下行发布（Logic → MQ → 目标 Gateway）。
// routingKey 传目标网关地址（如 "127.0.0.1:8080"），消息持久化防重启丢失。
func PublishDown(ctx context.Context, routingKey string, body []byte) error {
	return getPublishCh().PublishWithContext(
		ctx,
		downExchangeName, // exchange
		routingKey,       // routing key：目标网关地址
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // 持久化消息，Broker 重启不丢
			Body:         body,
		},
	)
}

// ConsumeDownQueue 声明该网关专属的持久化下行 Queue，绑定到下行 Exchange，返回消息通道。
// queueName 示例: "gateway.queue.127.0.0.1:8080"
// routingKey 与 PublishDown 时一致，即 gatewayAddr
func ConsumeDownQueue(queueName, routingKey string) (<-chan amqp.Delivery, error) {
	q, err := channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, fmt.Errorf("声明 Queue [%s] 失败: %w", queueName, err)
	}

	if err = channel.QueueBind(q.Name, routingKey, downExchangeName, false, nil); err != nil {
		return nil, fmt.Errorf("绑定 Queue 到 Exchange 失败: %w", err)
	}

	// QoS：预取数量从 50 提高到 2000，避免高 QPS 限制网关下发吞吐
	if err = channel.Qos(2000, 0, false); err != nil {
		return nil, fmt.Errorf("设置 QoS 失败: %w", err)
	}

	// auto-ack=false：手动确认，确保消息被真正处理后再从 Queue 移除
	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("注册消费者失败: %w", err)
	}

	logger.Log.Infof("✅ Queue [%s] 已声明并绑定，RoutingKey: %s", queueName, routingKey)
	return msgs, nil
}

// PublishVector 向量写入（logic->vector Consumer）异步向向量数据库投递
func PublishVector(ctx context.Context, vectorRoutingKey string, body []byte) error {
	return getPublishCh().PublishWithContext(
		ctx,
		vectorExchangeName,
		vectorRoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

func ConsumeVectorQueue(queueName string, vectorRoutingKey string) (<-chan amqp.Delivery, error) {
	q, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			// 死信交换机和key
			"x-dead-letter-exchange":    deadExchangeName,
			"x-dead-letter-routing-key": "dead.letter",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("声明 Queue [%s] 失败: %w", queueName, err)
	}
	if err = channel.QueueBind(q.Name, vectorRoutingKey, vectorExchangeName, false, nil); err != nil {
		return nil, fmt.Errorf("绑定 Queue 到 vectorExchange 失败: %w", err)
	}
	if err = channel.Qos(50, 0, false); err != nil {
		return nil, fmt.Errorf("设置 QoS 失败: %w", err)
	}

	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("注册消费者失败: %w", err)
	}

	logger.Log.Infof("✅ Queue [%s] 已声明并绑定，RoutingKey: %s", queueName, vectorRoutingKey)
	return msgs, nil
}

// Close 优雅关闭（供 main 注册 defer）
func Close() {
	for _, ch := range publishChs {
		if ch != nil {
			_ = ch.Close()
		}
	}
	if channel != nil {
		_ = channel.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
}
