package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
)

// GetGroupMembers 获取群内所有成员id 包含发送者
func GetGroupMembers(groupID int64) ([]int64, error) {
	var members []int64
	err := db.GetDB().Table("group_members").
		Where("group_id = ?", groupID).
		Pluck("user_id", &members).Error
	return members, err
}

func GetGroupMemberModels(groupID int64) ([]models.GroupMember, error) {
	var members []models.GroupMember
	err := db.GetDB().Where("group_id = ?", groupID).Find(&members).Error
	return members, err
}

func GetUserGroups(userID int64) ([]models.GroupMember, error) {
	var members []models.GroupMember
	err := db.GetDB().Where("user_id = ?", userID).Find(&members).Error
	return members, err
}

func InsertGroupMember(member *models.GroupMember) error {
	return db.GetDB().Create(member).Error
}

func DeleteGroupMember(groupID, userID int64) error {
	return db.GetDB().Where("group_id = ? AND user_id = ?", groupID, userID).Delete(&models.GroupMember{}).Error
}
