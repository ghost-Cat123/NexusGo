package dao

import (
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/logger"
	"gorm.io/gorm"
	"time"
)

func CreateScheduledMessage(schMessage *models.ScheduledMessages) (int64, error) {
	result := db.GetDB().Create(schMessage)
	return schMessage.CreatorId, result.Error
}

func GetPendingScheduledTasks(batch int) ([]models.ScheduledMessages, error) {
	var schMessages []models.ScheduledMessages
	// 构建 SQL
	result := db.GetDB().Where("status = ? AND scheduled_send_time <= ?", 0, time.Now()).Limit(batch).Find(&schMessages)
	return schMessages, result.Error
}

func ClaimTask(schMsgId int64) (int64, error) {
	result := db.GetDB().Model(&models.ScheduledMessages{}).
		Where("sch_msg_id = ? AND status = ?", schMsgId, 0).
		Updates(map[string]interface{}{
			"status":       1,
			"updated_time": time.Now(),
		})
	return result.RowsAffected, result.Error
}

// ExecuteSchSend 使用事务确保状态一致
func ExecuteSchSend(msg *models.Messages, schMsgId int64) error {
	// 使用事务完成插入消息+状态更新
	return db.GetDB().Transaction(func(tx *gorm.DB) error {
		// 1. 插入发送的定时消息到msg表中
		if err := InsertMessage(msg); err != nil {
			// 回滚
			return err
		}
		// 2. 修改定时消息表中消息的状态信息
		if err := tx.Model(&models.ScheduledMessages{}).
			Where("sch_msg_id = ? AND status = ?", schMsgId, 1).
			Updates(map[string]interface{}{
				"status":       2,
				"updated_time": time.Now(),
			}).Error; err != nil {
			return err
		}
		return nil
	})
}

func FailedTask(schMsgId int64, failReason string) {
	err := db.GetDB().Model(&models.ScheduledMessages{}).
		Where("sch_msg_id = ? AND status = ?", schMsgId, 1).
		Updates(map[string]interface{}{
			"status":       3,
			"fail_reason":  failReason,
			"updated_time": time.Now(),
		}).Error
	if err != nil {
		logger.Log.Fatalf("[FATAL] 任务 ID: %d 状态回写失败！原错误: %s, 数据库写入错误: %v", schMsgId, failReason, err)
	}
}

func MarkSchTaskDone(schMsgId int64) error {
	return db.GetDB().Model(&models.ScheduledMessages{}).
		Where("sch_msg_id = ? AND status = ?", schMsgId, 1).
		Updates(map[string]interface{}{
			"status":       2,
			"updated_time": time.Now(),
		}).Error
}
