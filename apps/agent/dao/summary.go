package dao

import (
	"fmt"
	"go-im-system/apps/agent/models"
	"go-im-system/apps/pkg/db"
)

// SaveSummary 将 LLM 生成的摘要落库
func SaveSummary(userID int64, sessionID, summary string) error {
	record := &models.AgentSummary{
		UserID:    userID,
		SessionID: sessionID,
		Summary:   summary,
	}
	if err := db.GetDB().Create(record).Error; err != nil {
		return fmt.Errorf("摘要落库失败: %w", err)
	}
	return nil
}

// GetLatestSummary 拉取该 Session 最近一条摘要，供注入 Prompt 使用
// 若不存在则返回空字符串（非 error）
func GetLatestSummary(userID int64, sessionID string) (string, error) {
	var record models.AgentSummary
	err := db.GetDB().
		Where("user_id = ? AND session_id = ?", userID, sessionID).
		Order("create_time DESC").
		First(&record).Error
	if err != nil {
		// record not found 视为无摘要，正常情况
		return "", nil
	}
	return record.Summary, nil
}
