package dao

import "NexusGo/apps/pkg/db"

// GetGroupMembers 获取群内所有成员id 包含发送者
func GetGroupMembers(groupID int64) ([]int64, error) {
	var members []int64
	err := db.GetDB().Table("group_members").
		Where("group_id = ?", groupID).
		Pluck("user_id", &members).Error
	return members, err
}

func IsGroupMember(groupID, userID int64) (bool, error) {
	var result int
	err := db.GetDB().Raw(
		"SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ? LIMIT 1",
		groupID, userID,
	).Scan(&result).Error
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
