package dao

import (
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/db"
	"strings"
	"time"
)

func InsertMessage(message *models.Messages) error {
	err := db.GetDB().Create(message).Error
	return err
}

func GetHistoryBetweenUsers(uid1, uid2 int64, startTime, endTime time.Time, limit int) ([]models.Messages, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var messages []models.Messages
	// 构建 SQL
	sql := `
		SELECT *
		FROM messages 
		WHERE 
		    ((sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?))
		    AND create_time >= ? 
		    AND create_time <= ?
		ORDER BY create_time DESC 
		LIMIT ?
	`
	result := db.GetDB().Raw(sql, uid1, uid2, uid2, uid1, startTime, endTime, limit).Scan(&messages)
	return messages, result.Error
}

// SearchHistoryMessages 根据关键词查询所有和当前用户相关的聊天记录
func SearchHistoryMessages(uid1, uid2, groupId int64, keyWords []string, startTime, endTime time.Time, limit int) ([]models.Messages, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var conditions []string
	var args []interface{}

	// 1. 时间范围
	conditions = append(conditions, "create_time BETWEEN ? AND ?")
	args = append(args, startTime, endTime)

	// 2. 群聊 vs 单聊（决定 WHERE 结构）
	if groupId != 0 {
		// ── 群聊分支 ──
		// receiver_id 去重：写扩散每人，只取当前用户的副本
		conditions = append(conditions, "group_id = ?")
		args = append(args, groupId)
		conditions = append(conditions, "receiver_id = ?")
		args = append(args, uid1)

		if uid2 != 0 {
			// 场景 C: 群内搜某人
			conditions = append(conditions, "sender_id = ?")
			args = append(args, uid2)
		}

	} else {
		// ── 单聊分支 ──
		conditions = append(conditions, "group_id = 0")
		conditions = append(conditions,
			"((sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?))")
		args = append(args,
			uid1, uid2,
			uid2, uid1)
		// 场景 A: 两人之间搜索
	}

	// 3. 关键词 FULLTEXT Boolean 模式
	if len(keyWords) > 0 {
		terms := "+" + strings.Join(keyWords, " +")
		conditions = append(conditions, "MATCH(content) AGAINST(? IN BOOLEAN MODE)")
		args = append(args, terms)
	}

	// 4. 排序 + 分页
	sql := "SELECT * FROM messages WHERE " + strings.Join(conditions, " AND ")
	sql += " ORDER BY create_time DESC LIMIT ?"
	args = append(args, limit)

	var messages []models.Messages
	err := db.GetDB().Raw(sql, args...).Scan(&messages).Error
	return messages, err
}

// GetMessagesByIDs 根据消息id批量查找消息，同时做权限校验（只返回当前用户相关消息）
func GetMessagesByIDs(msgIDs []int64) ([]models.Messages, error) {
	if len(msgIDs) == 0 {
		return nil, nil
	}
	var messages []models.Messages
	err := db.GetDB().Where("msg_id IN ?", msgIDs).
		Order("create_time DESC").Find(&messages).Error
	return messages, err
}
