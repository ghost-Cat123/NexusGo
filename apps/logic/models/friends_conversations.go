package models

import "time"

// Friend 好友关系表
type Friend struct {
	ID         int64     `gorm:"primaryKey;autoIncrement;column:id"`
	UserID     int64     `gorm:"uniqueIndex:uk_user_friend,priority:1;column:user_id"`     // 发起方
	FriendID   int64     `gorm:"uniqueIndex:uk_user_friend,priority:2;column:friend_id"`   // 接收方
	Status     int8      `gorm:"column:status;default:0;index"`                            // 0: 申请中, 1: 已通过, 2: 已拒绝
	Remark     string    `gorm:"type:varchar(64);column:remark"`                           // 备注名
	CreateTime time.Time `gorm:"autoCreateTime;column:create_time"`                        // 创建时间
	UpdateTime time.Time `gorm:"autoUpdateTime;column:update_time"`                        // 更新时间
}

// Conversation 最近会话列表
type Conversation struct {
	ID            int64     `gorm:"primaryKey;autoIncrement;column:id"`
	OwnerID       int64     `gorm:"uniqueIndex:uk_owner_target,priority:1;column:owner_id"`   // 会话归属人
	TargetID      int64     `gorm:"uniqueIndex:uk_owner_target,priority:2;column:target_id"`  // 聊天对象ID (User/Group)
	SessionType   int8      `gorm:"uniqueIndex:uk_owner_target,priority:3;column:session_type"` // 1: 单聊, 2: 群聊
	UnreadCount   int       `gorm:"column:unread_count;default:0"`                             // 未读消息数
	LastMessageID int64     `gorm:"column:last_message_id"`                                    // 最后一条消息ID (用于排序和展示摘要)
	IsTop         bool      `gorm:"column:is_top;default:false"`                               // 是否置顶
	UpdateTime    time.Time `gorm:"autoUpdateTime;index;column:update_time"`                   // 最后活跃时间
}
