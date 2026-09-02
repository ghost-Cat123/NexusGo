package models

import "time"

// AgentSummary 对话摘要持久化模型
// 对应 MySQL 表：agent_summaries
type AgentSummary struct {
	ID         int64     `gorm:"primaryKey;autoIncrement;column:id"`
	UserID     int64     `gorm:"index:idx_session,priority:1;column:user_id"`
	SessionID  string    `gorm:"index:idx_session,priority:2;column:session_id"`
	Summary    string    `gorm:"type:text;column:summary"`
	MemoryType string    `gorm:"index;default:general;column:memory_type"`
	Tags       string    `gorm:"column:tags;default:''"`
	Importance int8      `gorm:"index;default:0;column:importance"`
	CreateTime time.Time `gorm:"autoCreateTime;column:create_time"`
}
