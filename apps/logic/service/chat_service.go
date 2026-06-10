package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"NexusGo/apps/logic/dao"
	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/proto/pb_msg"
)

// LogicService RPC 服务（上行消息已迁移至 MQ 消费，此处仅保留查询/ACK 类轻量 RPC）

type LogicService struct{}

func (s *LogicService) SyncUnread(ctx context.Context, req *pb_msg.SyncUnreadArgs, resp *pb_msg.SyncUnreadReply) error {
	// 去 MySQL 查所有 ReceiverId == args.UserId 且 IsRead == false 的消息
	messages, err := dao.GetUnreadMessages(req.ReceiverId)
	if err != nil {
		return err
	}

	// 组装到 reply 中返回给 Gateway
	for _, msg := range messages {
		resp.Messages = append(resp.Messages, &pb_msg.MessageItem{
			MsgId:      msg.MsgId,
			SenderId:   msg.SenderId,
			GroupId:    msg.GroupId,
			Content:    msg.Content,
			SeqId:      msg.SeqId,
			SendStatus: int32(msg.SendStatus),
		})
	}
	return nil
}

// NotifyDelivered 网关 WS 已成功写入接收方连接，将 send_status 从 0 置为 1
func (s *LogicService) NotifyDelivered(ctx context.Context, req *pb_msg.NotifyDeliveredArgs, resp *pb_msg.NotifyDeliveredReply) error {
	if err := dao.MarkSendTransportDelivered(req.MsgId); err != nil {
		logger.Log.Errorf("NotifyDelivered 失败: %v", err)
		resp.Success = false
		return err
	}
	resp.Success = true
	return nil
}

func (s *LogicService) AckMessage(ctx context.Context, req *pb_msg.AckMessageArgs, resp *pb_msg.AckMessageReply) error {
	// 去 MySQL 将msgId == args.msg_id 的消息 变为已读
	err := dao.MarkMessageAsRead(req.MsgId)
	if err != nil {
		logger.Log.Errorf("修改据库失败: %v", err)
		resp.Success = false
		return err
	}
	resp.Success = true
	return nil
}

func (s *LogicService) ReadMessages(ctx context.Context, req *pb_msg.ReadMessagesArgs, resp *pb_msg.ReadMessagesReply) error {
	err := dao.MarkMessagesAsRead(req.ReceiverId)
	if err != nil {
		logger.Log.Errorf("修改据库失败: %v", err)
		resp.Success = false
		return err
	} else {
		resp.Success = true
		return nil
	}
}

func (s *LogicService) GetConversations(ctx context.Context, req *pb_msg.GetConversationsArgs, resp *pb_msg.GetConversationsReply) error {
	convs, err := dao.GetConversations(req.UserId)
	if err != nil {
		return err
	}
	for _, c := range convs {
		resp.Conversations = append(resp.Conversations, &pb_msg.ConversationItem{
			TargetId:    c.TargetID,
			SessionType: int32(c.SessionType),
			UnreadCount: int64(c.UnreadCount),
			OwnerId:     c.OwnerID,
		})
	}
	return nil
}

func (s *LogicService) GetChatHistory(ctx context.Context, req *pb_msg.GetChatHistoryArgs, resp *pb_msg.GetChatHistoryReply) error {
	// 仅对首屏（cursor=0）做 Redis 缓存，翻历史页直接走 DB
	if req.Cursor == 0 {
		cacheKey := fmt.Sprintf("chat:history:%d:%d:%d:%d", req.UserId, req.TargetId, req.Cursor, req.Limit)
		if cached, err := cache.GetCache().Get(ctx, cacheKey).Result(); err == nil && cached != "" {
			return json.Unmarshal([]byte(cached), resp)
		}
	}

	msgs, err := dao.GetChatHistory(req.UserId, req.TargetId, req.Cursor, req.Limit)
	if err != nil {
		return err
	}
	resp.TargetId = req.TargetId
	for _, msg := range msgs {
		resp.Messages = append(resp.Messages, &pb_msg.MessageItem{
			MsgId:      msg.MsgId,
			SenderId:   msg.SenderId,
			GroupId:    msg.GroupId,
			Content:    msg.Content,
			SeqId:      msg.SeqId,
			SendStatus: int32(msg.SendStatus),
			CreateTime: msg.CreateTime.Format("2006-01-02 15:04:05"),
		})
	}
	if len(msgs) == int(req.Limit) {
		resp.HasMore = true
		resp.NextCursor = msgs[len(msgs)-1].MsgId
	}

	// 首屏结果写入 Redis，TTL 5 分钟
	if req.Cursor == 0 {
		if data, err := json.Marshal(resp); err == nil {
			cacheKey := fmt.Sprintf("chat:history:%d:%d:%d:%d", req.UserId, req.TargetId, 0, req.Limit)
			cache.GetCache().Set(ctx, cacheKey, data, 5*time.Minute)
		}
	}
	return nil
}

func (s *LogicService) MarkMessageRead(ctx context.Context, req *pb_msg.MarkReadArgs, resp *pb_msg.MarkReadReply) error {
	err := dao.ClearUnread(req.UserId, req.TargetId)
	if err != nil {
		return err
	}
	resp.Success = true
	return nil
}
