package models

import "time"

type Group struct {
	GroupID    int64     `gorm:"primaryKey;column:group_id"`
	GroupName  string    `gorm:"column:group_name"`
	OwnerID    int64     `gorm:"column:owner_id"`
	CreatorID  int64     `gorm:"column:creator_id"`
	CreateTime time.Time `gorm:"autoCreateTime;column:create_time"`
}
