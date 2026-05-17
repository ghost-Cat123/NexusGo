package ws

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/logger"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	Uid  int64
	Conn *websocket.Conn
	// 带缓冲发送通道
	SendChan chan []byte
	// Close只执行一次 channel防止重复关闭
	closeOnce sync.Once
	// 关闭信号量
	closed chan struct{}
}

func NewClient(uid int64, conn *websocket.Conn) *Client {
	return &Client{
		Uid:  uid,
		Conn: conn,
		// 这里可作为配置项
		SendChan: make(chan []byte, 1024),
		closed:   make(chan struct{}),
	}
}

// 生命周期管理

// SendMessage 1. 发送消息
func (c *Client) SendMessage(msg []byte) {
	// 1. 获取当前 channel 的长度和容量
	currentLen := len(c.SendChan)
	capacity := cap(c.SendChan)

	// 2. 【监控预警水位线】: 当堆积超过 80% 时，打 Warn 日志，但在 Prometheus 监控面板上这里会画出一条飙升的红线
	if currentLen > capacity*8/10 {
		logger.Log.Warnw("WebSocket 消息堆积预警 (背压产生)",
			"uid", c.Uid,
			"chan_len", currentLen,
			"chan_cap", capacity,
		)
	}

	// 3. 尝试发送
	select {
	case c.SendChan <- msg:
		// 发送成功，如果是为了极其严谨的压测，你甚至可以在这里记录一个 Metrics:
		// metrics.SendChanLengthSummary.Observe(float64(currentLen))

	case <-c.closed:
		logger.Log.Infof("用户 [%d] 连接已关闭，终止发送", c.Uid)

	default:
		// 4. 【熔断丢弃线】: 当 100% 满的时候，绝对不能阻塞业务协程！
		// 走到这里说明水池彻底溢出，直接丢弃新消息，保护网关不被卡死！
		logger.Log.Errorw("WebSocket 消息通道已满，触发防卡死丢弃！",
			"uid", c.Uid,
		)
		// 【可选架构优化】: 针对极度恶劣的慢客户端，如果它的 Channel 满了，说明他网络卡死了，
		// 大厂的做法是直接主动把这个客户端踢下线 (Kick-out)，释放服务器资源。
		c.Close()
	}
}

// Close 2. 关闭连接
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		// 广播关闭信号
		close(c.closed)
		// 关闭底层websocket连接
		err := c.Conn.Close()
		if err != nil {
			logger.Log.Error("连接关闭失败")
			return
		}
		// 不要去 close(c.SendChan)，让 Go 垃圾回收它，防止引发向 closed chan 写入的 panic
	})
}

func (c *Client) WritePump() {
	// 写报错 清除
	defer c.Close()
	for {
		select {
		// 读channel中的消息
		case msg, ok := <-c.SendChan:
			if !ok {
				// 防止通道关闭读0值
				logger.Log.Errorf("消息通道已关闭")
				return
			}
			// 写超时
			err := c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				logger.Log.Errorf("写超时时间失败")
				return
			}
			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				logger.Log.Errorf("用户 [%d] 写入失败: %v", c.Uid, err)
				return
			}
		case <-c.closed:
			// 关闭信号
			return
		}
	}
}

func (c *Client) ReadPump() {
	userIDStr := strconv.FormatInt(c.Uid, 10)
	defer func() {
		logger.Log.Infof("用户 [%d] 断开连接", c.Uid)
		// 1. 本地连接池移除该用户的连接
		GlobalCliMap.Remove(userIDStr, c)
		c.Close()
	}()

	c.Conn.SetReadLimit(8192)
	for {
		_, msgData, err := c.Conn.ReadMessage()
		if err != nil {
			logger.Log.Errorf("读取消息失败或用户掉线: %v", err)
			break
		}
		// 解析基础包，只看 action 是什么
		var baseReq struct {
			ChatType string `json:"chat_type"`
		}
		if err := json.Unmarshal(msgData, &baseReq); err != nil {
			return
		}

		switch baseReq.ChatType {
		case "ping":
			// 收到前端是心跳信息
			ctx := context.Background()
			redisKey := "route:user:" + userIDStr
			// 重新设置30s过期时间
			cache.GetCache().Expire(ctx, redisKey, time.Second*30)
			// 给前端发送响应
			c.SendMessage([]byte(`{"action":"pong"}`))
		case "single_chat":
			go handleSingleChat(c.Uid, msgData)
		case "ai_chat":
			go handlerAIChat(c.Uid, msgData)
		case "group_chat":
			go handleGroupChat(c.Uid, msgData) // 调群聊函数
		case "ack":
			// 读取确认 已读 + send_status 置为「发送已确认」(2)
			handleAck(c.Uid, msgData)
		default:
			logger.Log.Info("未知动作", baseReq.ChatType)
		}
		logger.Log.Infof("收到用户 [%d] 的消息: %s", c.Uid, string(msgData))
	}
}
