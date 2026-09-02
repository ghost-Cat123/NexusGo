package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
)

func InsertMessage(message *models.Messages) error {
	err := db.GetDB().Create(message).Error
	return err
}

// BatchInsertMessages 批量插入，底层生成单条 INSERT ... VALUES (...),(...),...
// 遇到重复主键时整批失败，调用方应降级为逐条 InsertMessage 处理。
func BatchInsertMessages(messages []*models.Messages) error {
	if len(messages) == 0 {
		return nil
	}
	// CreateInBatches 会把 messages 按 batchSize 分批，每批一条 SQL
	// 这里直接传 len(messages)，让整批变成一条 INSERT（调用方已经控制了批大小）
	return db.GetDB().CreateInBatches(messages, len(messages)).Error
}

func GetUnreadMessages(receiverId int64) ([]models.Messages, error) {
	var messages []models.Messages
	result := db.GetDB().Where("receiver_id = ? AND is_read = ?", receiverId, false).Find(&messages)
	return messages, result.Error
}
 
func MarkMessagesAsRead(receiverId int64) error {
	result := db.GetDB().Model(&models.Messages{}).
		Where("receiver_id = ? AND is_read = ?", receiverId, false).
		Updates(map[string]interface{}{
			"is_read":     true,
			"send_status": models.SendStatusSentConfirmed,
		})
	return result.Error
}

func MarkMessageAsRead(msgId int64) error {
	// 已读即视为「发送已确认」（单级业务确认，不单独做投递 ACK）
	result := db.GetDB().Model(&models.Messages{}).
		Where("msg_id = ? AND is_read = ?", msgId, false).
		Updates(map[string]interface{}{
			"is_read":     true,
			"send_status": models.SendStatusSentConfirmed,
		})
	return result.Error
}

// MarkSendTransportDelivered 网关推送成功：仅允许 0 -> 1
func MarkSendTransportDelivered(msgId int64) error {
	result := db.GetDB().Model(&models.Messages{}).
		Where("msg_id = ? AND send_status = ?", msgId, models.SendStatusUnsent).
		Update("send_status", models.SendStatusSentUnconfirmed)
	return result.Error
}

const maxChatLimit = 200

func GetChatHistory(userID, targetID, cursor, limit int64) ([]models.Messages, error) {
	if limit <= 0 || limit > maxChatLimit {
		limit = 20
	}
	var messages []models.Messages
	query := db.GetDB().Where("group_id = 0").Where("((sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?))", userID, targetID, targetID, userID)
	if cursor > 0 {
		query = query.Where("msg_id < ?", cursor)
	}
	err := query.Order("msg_id DESC").Limit(int(limit)).Find(&messages).Error
	return messages, err
}

func GetGroupHistory(groupID, userID, cursor, limit int64) ([]models.Messages, error) {
	if limit <= 0 || limit > maxChatLimit {
		limit = 20
	}
	var messages []models.Messages
	query := db.GetDB().Where("group_id = ? AND receiver_id = ?", groupID, userID)
	if cursor > 0 {
		query = query.Where("msg_id < ?", cursor)
	}
	err := query.Order("msg_id DESC").Limit(int(limit)).Find(&messages).Error
	return messages, err
}

// DeleteMessagesBetween 删除两个用户之间的所有单聊消息
func DeleteMessagesBetween(userA, userB int64) error {
	return db.GetDB().Where(
		"(sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?)",
		userA, userB, userB, userA,
	).Where("group_id = 0").Delete(&models.Messages{}).Error
}

func DeleteGroupMessages(groupID int64) error {
	return db.GetDB().Where("group_id = ?", groupID).Delete(&models.Messages{}).Error
}
