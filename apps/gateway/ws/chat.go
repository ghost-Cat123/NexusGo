package ws

import (
	"GrowRPC/xclient"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/mq"
	"NexusGo/apps/pkg/utils"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"NexusGo/apps/gateway/rpcclient"
	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/proto/pb_msg"
)

// agentHTTPClient 是全局共享的 HTTP 客户端。
// 使用连接池+长连接，避免每次 SSE 请求都新建 TCP 连接，显著降低 TTFT。
var agentHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false, // 必须保持长连接
	},
	// SSE 是流式响应，不设整体超时，由调用方负责 ctx 取消
	Timeout: 0,
}

// NotifyDeliveredRPC 通知 Logic：对端 WS 已写入，send_status 0→1
func NotifyDeliveredRPC(ctx context.Context, senderID, msgID int64) {
	args := &pb_msg.NotifyDeliveredArgs{MsgId: msgID}
	rk := xclient.WithRoutingKey(ctx, strconv.FormatInt(senderID, 10))
	if _, err := rpcclient.MsgServiceClient.NotifyDelivered(rk, args); err != nil {
		logger.Log.Errorf("NotifyDelivered RPC 失败: %v", err)
	}
}

func marshalChatPush(msgID, seqID, groupID, from int64, content string) ([]byte, error) {
	sessionType := 1
	if groupID > 0 {
		sessionType = 2
	}
	return json.Marshal(map[string]interface{}{
		"chat_type":    "chat_push",
		"msg_id":       msgID,
		"seq_id":       seqID,
		"from":         from,
		"group_id":     groupID,
		"content":      content,
		"session_type": sessionType,
	})
}

func marshalFriendRequestPush(from int64, content string) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "friend_request",
		"from":      from,
		"apply_msg": content,
	})
}

func marshalFriendResolvedPush(from int64, action string) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "friend_request_resolved",
		"from":      from,
		"action":    action,
	})
}

func marshalFriendDeletedPush(from int64) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "friend_deleted",
		"from":      from,
	})
}

func marshalGroupCreatedPush(from, groupID int64, groupName string) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type":  "group_created",
		"from":       from,
		"group_id":   groupID,
		"group_name": groupName,
	})
}

func marshalGroupDissolvedPush(groupID int64) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "group_dissolved",
		"group_id":  groupID,
	})
}

func marshalGroupJoinRequestPush(from, groupID int64) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "group_join_request",
		"from":      from,
		"group_id":  groupID,
	})
}

func marshalGroupJoinApprovedPush(groupID int64) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "group_join_approved",
		"group_id":  groupID,
	})
}

func marshalGroupMemberChangedPush(groupID int64) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"chat_type": "group_member_changed",
		"group_id":  groupID,
	})
}

func handleSingleChat(senderId int64, msgData []byte) {
	var clientReq ClientRequest
	if err := json.Unmarshal(msgData, &clientReq); err != nil {
		logger.Log.Errorf("JSON解析失败: %v", err)
		return
	}

	bgCtx := context.Background()
	var msgID, seqID int64
	var convID string

	// 可靠性管理器：生成会话ID、消息ID、会话内序号
	if DefaultReliability != nil {
		convID = utils.SingleChatConvID(senderId, clientReq.Receiver)
		seq, err := DefaultReliability.GetNextSeqID(bgCtx, convID, senderId)
		if err != nil {
			logger.Log.Errorf("单聊分配会话序号失败: %v", err)
			return
		}
		seqID = seq
		msgID = DefaultReliability.GenerateMsgID(bgCtx, convID, seqID)
	}

	// 上行：网关不调RPC不查DB，生成MsgID后直推MQ
	payload := mq.UploadPayload{
		MsgID:      msgID,
		SeqID:      seqID,
		ChatType:   mq.ChatTypeSingle,
		GroupID:    0,
		ConvID:     convID,
		SenderID:   senderId,
		ReceiverID: clientReq.Receiver,
		Content:    clientReq.Message,
	}
	body, _ := json.Marshal(payload)

	// 上行失败直接打日志 等待客户端重试
	if pubErr := mq.PublishUpload(bgCtx, "upload.all", body); pubErr != nil {
		logger.Log.Errorf("[Gateway] 上行Publish失败，客户端将触发重试: %v", pubErr)
		return
	}

	// 给自己的ws发送server_ack
	if senderClient, ok := GlobalCliMap.Get(strconv.FormatInt(senderId, 10)); ok {
		ackMsg := fmt.Sprintf(`{"chat_type":"server_ack","msg_id":%d,"seq_id":%d}`, msgID, seqID)
		senderClient.SendMessage([]byte(ackMsg))
	}
	logger.Log.Infof("[Gateway] 上行Publish成功: sender=%d receiver=%d msgID=%d",
		senderId, clientReq.Receiver, msgID)
}

func handleGroupChat(senderId int64, msgData []byte) {
	var clientReq ClientRequest
	if err := json.Unmarshal(msgData, &clientReq); err != nil {
		logger.Log.Errorf("JSON解析失败: %v", err)
		return
	}
	bgCtx := context.Background()
	var msgID, seqID int64
	// 群聊 ConvID 格式：group_{group_id}
	convID := fmt.Sprintf("group_%d", clientReq.GroupID)
	// 可靠性管理器：生成消息ID、会话内序号
	if DefaultReliability != nil {
		seq, err := DefaultReliability.GetNextSeqID(bgCtx, convID, senderId)
		if err != nil {
			logger.Log.Errorf("群聊分配会话序号失败: %v", err)
			return
		}
		seqID = seq
		msgID = DefaultReliability.GenerateMsgID(bgCtx, convID, seqID)
	}
	// 组装群聊上行payload
	payload := mq.UploadPayload{
		MsgID:      msgID,
		SeqID:      seqID,
		ChatType:   mq.ChatTypeGroup,
		GroupID:    clientReq.GroupID,
		ConvID:     convID,
		SenderID:   senderId,
		ReceiverID: clientReq.Receiver,
		Content:    clientReq.Message,
	}
	body, _ := json.Marshal(payload)

	if pubErr := mq.PublishUpload(bgCtx, "upload.all", body); pubErr != nil {
		logger.Log.Errorf("[Gateway] 群聊上行 Publish 失败: %v", pubErr)
		return
	}

	// 立即给发送者返回 server_ack
	if senderClient, ok := GlobalCliMap.Get(strconv.FormatInt(senderId, 10)); ok {
		ackMsg := fmt.Sprintf(`{"chat_type":"server_ack","msg_id":%d,"seq_id":%d}`, msgID, seqID)
		senderClient.SendMessage([]byte(ackMsg))
	}
}

func handlerAIChat(senderId int64, msgData []byte) {
	var clientReq ClientRequest
	if err := json.Unmarshal(msgData, &clientReq); err != nil {
		logger.Log.Errorf("JSON解析失败: %v", err)
		return
	}

	// ── 第一步：立即发起 SSE 请求（不等 Redis/MQ，最小化 TTFT）──
	agentURL := fmt.Sprintf("%s/agent/chat/sse?user_id=%d&message=%s",
		config.GlobalConfig.Server.AgentAddr, senderId, url.QueryEscape(clientReq.Message))
	resp, err := agentHTTPClient.Get(agentURL)
	if err != nil {
		logger.Log.Errorf("[Gateway] 连接 Agent SSE 失败: %v", err)
		return
	}
	defer resp.Body.Close()

	// ── 第二步：MQ 上行异步落库，不阻塞流式推送 ──
	bgCtx := context.Background()
	convID := fmt.Sprintf("ai_%d", senderId)
	var msgID, seqID int64

	if DefaultReliability != nil {
		seq, err := DefaultReliability.GetNextSeqID(bgCtx, convID, senderId)
		if err != nil {
			logger.Log.Errorf("分配会话序号失败: %v", err)
			// 序号分配失败不影响流式推送，继续
		} else {
			seqID = seq
			msgID = DefaultReliability.GenerateMsgID(bgCtx, convID, seqID)
		}
	}

	// 异步上行 MQ + 发送 server_ack，不阻塞 SSE 扫描
	go func(mid, sid int64) {
		payload := mq.UploadPayload{
			MsgID:      mid,
			SeqID:      sid,
			ChatType:   mq.ChatTypeAI,
			GroupID:    0,
			ConvID:     convID,
			SenderID:   senderId,
			ReceiverID: -1,
			Content:    clientReq.Message,
		}
		body, _ := json.Marshal(payload)
		if pubErr := mq.PublishUpload(bgCtx, "upload.all", body); pubErr != nil {
			logger.Log.Errorf("[Gateway] AI上行Publish失败: %v", pubErr)
		}
		// 服务端 ACK（落库成功后通知客户端消息已持久化）
		if senderClient, ok := GlobalCliMap.Get(strconv.FormatInt(senderId, 10)); ok {
			ackMsg := fmt.Sprintf(`{"chat_type":"server_ack","msg_id":%d,"seq_id":%d}`, mid, sid)
			senderClient.SendMessage([]byte(ackMsg))
		}
	}(msgID, seqID)

	// ── 第三步：扫描 SSE 流，实时 push chunk 给客户端 ──
	senderClient, ok := GlobalCliMap.Get(strconv.FormatInt(senderId, 10))
	if !ok {
		// 用户已离线，AI消息落库兜底，无需推送
		return
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "data: [DONE]" {
			senderClient.SendMessage([]byte(`{"chat_type":"ai_end","from":"-1"}`))
			return
		}
		if strings.HasPrefix(line, "data: ") {
			chunk := strings.TrimPrefix(line, "data: ")
			msg := fmt.Sprintf(`{"chat_type":"ai_chunk","from":"-1","content":"%s"}`, chunk)
			senderClient.SendMessage([]byte(msg))
		}
	}
}

func handleAck(userId int64, msgData []byte) {
	var ackReq struct {
		MsgId    int64 `json:"msg_id"`
		SenderId int64 `json:"sender_id"`
	}
	if err := json.Unmarshal(msgData, &ackReq); err != nil {
		logger.Log.Errorf("ACK 参数解析失败: %v", err)
		return
	}
	ackMessageArgs := &pb_msg.AckMessageArgs{
		MsgId: ackReq.MsgId,
	}

	routingKey := strconv.FormatInt(userId, 10)
	ctx := xclient.WithRoutingKey(context.Background(), routingKey)
	_, err := rpcclient.MsgServiceClient.AckMessage(ctx, ackMessageArgs)
	if err != nil {
		logger.Log.Errorf("更新已读状态失败: %v", err)
		return
	}

	if senderClient, ok := GlobalCliMap.Get(strconv.FormatInt(ackReq.SenderId, 10)); ok {
		ackMsg := fmt.Sprintf(`{"chat_type": "ack", "read_receipt": %d, "msg_id": %d}`, userId, ackReq.MsgId)
		senderClient.SendMessage([]byte(ackMsg))
		logger.Log.Infof("ACK成功发送给 [%d]", ackReq.SenderId)
	}
}
