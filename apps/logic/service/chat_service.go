package service

import (
	"go-im-system/apps/logic/dao"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/proto/pb_msg"
)

// LogicService RPC 服务（上行消息已迁移至 MQ 消费，此处仅保留查询/ACK 类轻量 RPC）

type LogicService struct{}

func (s *LogicService) SyncUnread(args *pb_msg.SyncUnreadArgs, reply *pb_msg.SyncUnreadReply) error {
	// 去 MySQL 查所有 ReceiverId == args.UserId 且 IsRead == false 的消息
	messages, err := dao.GetUnreadMessages(args.ReceiverId)
	if err != nil {
		return err
	}

	// 组装到 reply 中返回给 Gateway
	for _, msg := range messages {
		reply.Messages = append(reply.Messages, &pb_msg.MessageItem{
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
func (s *LogicService) NotifyDelivered(args *pb_msg.NotifyDeliveredArgs, reply *pb_msg.NotifyDeliveredReply) error {
	if err := dao.MarkSendTransportDelivered(args.MsgId); err != nil {
		logger.Log.Errorf("NotifyDelivered 失败: %v", err)
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

func (s *LogicService) AckMessage(args *pb_msg.AckMessageArgs, reply *pb_msg.AckMessageReply) error {
	// 去 MySQL 将msgId == args.msg_id 的消息 变为已读
	err := dao.MarkMessageAsRead(args.MsgId)
	if err != nil {
		logger.Log.Errorf("修改据库失败: %v", err)
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

func (s *LogicService) ReadMessages(args *pb_msg.ReadMessagesArgs, reply *pb_msg.ReadMessagesReply) error {
	err := dao.MarkMessagesAsRead(args.ReceiverId)
	if err != nil {
		logger.Log.Errorf("修改据库失败: %v", err)
		reply.Success = false
		return err
	} else {
		reply.Success = true
		return nil
	}
}
