package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
)

func GetConversations(ownerID int64) ([]models.Conversation, error) {
	var convs []models.Conversation
	err := db.GetDB().Where("owner_id = ?", ownerID).Order("update_time DESC").Find(&convs).Error
	return convs, err
}

// UpsertConversation 原子 upsert：新会话创建，已有会话累加未读数并更新最后消息
// 使用 INSERT ... ON DUPLICATE KEY UPDATE 避免 First+Create/Update 的竞态窗口
func UpsertConversation(ownerID, targetID int64, sessionType int8, unreadDelta int, lastMsgID int64) error {
	return db.GetDB().Exec(
		`INSERT INTO conversations (owner_id, target_id, session_type, unread_count, last_message_id, update_time)
		 VALUES (?, ?, ?, ?, ?, NOW())
		 ON DUPLICATE KEY UPDATE
		     unread_count = unread_count + VALUES(unread_count),
		     last_message_id = IF(VALUES(last_message_id) > 0, VALUES(last_message_id), last_message_id),
		     update_time = NOW()`,
		ownerID, targetID, sessionType, unreadDelta, lastMsgID,
	).Error
}

func ClearUnread(ownerID, targetID int64, sessionType int8) error {
	return db.GetDB().Model(&models.Conversation{}).
		Where("owner_id = ? AND target_id = ? AND session_type = ?", ownerID, targetID, sessionType).
		Update("unread_count", 0).Error
}

func DeleteConversation(userID, friendID int64, sessionType int8) error {
	return db.GetDB().Where(
		"(owner_id = ? AND target_id = ? AND session_type = ?) OR (owner_id = ? AND target_id = ? AND session_type = ?)",
		userID, friendID, sessionType, friendID, userID, sessionType,
	).Delete(&models.Conversation{}).Error
}

func DeleteGroupConversations(groupID int64) error {
	return db.GetDB().Where("target_id = ? AND session_type = 2", groupID).
		Delete(&models.Conversation{}).Error
}
