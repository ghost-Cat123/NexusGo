package dao

import (
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/db"
	"fmt"
	"strings"
)

// SaveSummary 将 LLM 生成的结构化摘要落库
func SaveSummary(userID int64, sessionID string, summary, memoryType, tags string, importance int8) error {
	record := &models.AgentSummary{
		UserID:     userID,
		SessionID:  sessionID,
		Summary:    summary,
		MemoryType: memoryType,
		Tags:       tags,
		Importance: importance,
	}
	if err := db.GetDB().Create(record).Error; err != nil {
		return fmt.Errorf("摘要落库失败: %w", err)
	}
	return nil
}

// GetLatestSummary 按重要性+时间排序取 Top-3 摘要，去重拼接
func GetLatestSummary(userID int64, sessionID string) (string, error) {
	var records []models.AgentSummary
	err := db.GetDB().
		Where("user_id = ? AND session_id = ?", userID, sessionID).
		Order("importance DESC, create_time DESC").
		Limit(3).
		Find(&records).Error
	if err != nil {
		return "", nil
	}
	if len(records) == 0 {
		return "", nil
	}

	// 去重：相似摘要只保留 importance 更高的
	var summaries []string
	for _, r := range records {
		dup := false
		for _, s := range summaries {
			if r.Summary == s {
				dup = true
				break
			}
		}
		if !dup {
			summaries = append(summaries, r.Summary)
		}
	}
	return strings.Join(summaries, "\n\n"), nil
}
