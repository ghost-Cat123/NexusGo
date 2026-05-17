package models

import "time"

// AgentSummary 对话摘要持久化模型
// 对应 MySQL 表：agent_summaries
type AgentSummary struct {
	ID         int64     `gorm:"primaryKey;autoIncrement;column:id"`
	UserID     int64     `gorm:"index:idx_session,priority:1;column:user_id"`
	SessionID  string    `gorm:"index:idx_session,priority:2;column:session_id"` // 对应 Redis Session Key
	Summary    string    `gorm:"type:text;column:summary"`
	CreateTime time.Time `gorm:"autoCreateTime;column:create_time"`
}
