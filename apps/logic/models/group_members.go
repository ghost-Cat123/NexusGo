package models

import "time"

type GroupMember struct {
	MemberID int64     `gorm:"primaryKey;autoIncrement;column:id"`
	GroupID  int64     `gorm:"uniqueIndex:uk_group_user;column:group_id"`
	UserID   int64     `gorm:"column:user_id"`
	role     int8      `gorm:"column:role;default:0"`
	JoinTime time.Time `gorm:"autoCreateTime;column:join_time"`
}
