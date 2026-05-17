package ws

import (
	"GeeRPC/xclient"
	"context"
	"encoding/json"
	"go-im-system/apps/gateway/rpcclient"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/proto/pb_msg"
	"go-im-system/apps/pkg/utils"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// 全局WebSocket升级器
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// GlobalCliMap CliMap 全局连接管理器
var GlobalCliMap = NewCliMap()

// ClientRequest 定义前端发来的 JSON 格式结构体
type ClientRequest struct {
	ChatType string `json:"chat_type"` // 新增
	Receiver int64  `json:"receiver"`
	GroupID  int64  `json:"group_id"` // 新增
	Message  string `json:"message"`
}

// Handler 处理websocket请求
func Handler(c *gin.Context) {
	// 1. 根据token获取用户信息
	token := c.Query("token")
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "未携带 Token"})
		return
	}
	userID, err := utils.ParseToken(token)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "鉴权失败，请重新登录"})
		return
	}
	userIDStr := strconv.FormatInt(userID, 10)

	// 2. 获取该用户的websocket长连接
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Log.Fatalf("WebSocket 升级失败: %v", err)
		return
	}
	logger.Log.Infof("用户 [%s] 成功建立websocket连接", userIDStr)

	// 3. 将conn连接放入全局ConnectionManager Map中维护不同用户的连接
	client := NewClient(userID, conn)

	// 4. 注册到全局路由 挤掉旧连接
	GlobalCliMap.Register(userID, client)

	// 5. 更新redis路由表
	ctx := context.Background()
	redisKey := "route:user:" + userIDStr
	gatewayAddr := config.GlobalConfig.Server.GatewayAddr
	err = cache.GetCache().Set(ctx, redisKey, gatewayAddr, time.Hour*24).Err()
	if err != nil {
		logger.Log.Errorf("Redis 路由写入失败: %v", err)
	}

	// 启动写通道
	go client.WritePump()

	// 6. 拉取所有发送给该用户的未读信息
	err = syncMessage(userID, client)
	if err != nil {
		logger.Log.Errorf("消息同步失败: %v", err)
		return
	}

	// 7. 开启独立线程 处理该连接客户端消息
	go client.ReadPump()
}

func syncMessage(userID int64, client *Client) error {
	syncArgs := &pb_msg.SyncUnreadArgs{ReceiverId: userID}
	syncReply := &pb_msg.SyncUnreadReply{}
	routingKey := strconv.FormatInt(userID, 10)
	ctx := xclient.WithRoutingKey(context.Background(), routingKey)
	err := rpcclient.LogicRpcClient.Call(ctx, "LogicService.SyncUnread", syncArgs, syncReply)
	if err == nil && len(syncReply.Messages) > 0 {
		// 如果有未读消息，通过 conn 循环 WriteMessage 发给该用户！
		for _, msg := range syncReply.Messages {
			unReadMsg, err := json.Marshal(map[string]interface{}{
				"chat_type":   "sync_unread",
				"sender_id":   msg.SenderId,
				"content":     msg.Content,
				"group_id":    msg.GroupId,
				"msg_id":      msg.MsgId,
				"seq_id":      msg.SeqId,
				"send_status": msg.SendStatus,
			})
			if err != nil {
				logger.Log.Errorf("未读消息 JSON 序列化失败: %v", err)
				continue
			}
			client.SendMessage(unReadMsg)
		}
		// 发送完后，最好再调一个 RPC 告诉 Logic 服：这些消息已读了 (Update is_read = 1)
		readArgs := &pb_msg.ReadMessagesArgs{ReceiverId: userID}
		readReply := &pb_msg.ReadMessagesReply{}
		err = rpcclient.LogicRpcClient.Call(ctx, "LogicService.ReadMessages", readArgs, readReply)
		if err != nil || !readReply.Success {
			logger.Log.Errorf("消息 [%d] 连接关闭失败", userID)
		} else {
			logger.Log.Infof("未读消息拉取成功")
		}
	}
	return err
}
