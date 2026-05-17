package dao

import "go-im-system/apps/pkg/db"

// GetGroupMembers 获取群内所有成员id 包含发送者
func GetGroupMembers(groupID int64) ([]int64, error) {
	var members []int64
	err := db.GetDB().Table("group_members").
		Where("group_id = ?", groupID).
		Pluck("user_id", &members).Error
	return members, err
}
