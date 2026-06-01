package dao

import (
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/db"
	"errors"
	"gorm.io/gorm"
)

// CreateGroup 建群+拉取群成员，建群时群主和群成员是建群者，返回新群group_id
func CreateGroup(group *models.Group, memberIDs []int64) error {
	return db.GetDB().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(group).Error; err != nil {
			return err
		}
		var members []models.GroupMember
		for _, uid := range memberIDs {
			members = append(members, models.GroupMember{
				GroupID: group.GroupID,
				UserID:  uid,
			})
		}
		return tx.Create(&members).Error
	})
}

func FindGroupByName(groupName string) (models.Group, error) {
	var group models.Group
	result := db.GetDB().Where("group_name = ? ", groupName).Find(&group)
	if result.Error != nil {
		return group, result.Error
	}
	// 检查是否有记录被找到
	if result.RowsAffected == 0 {
		return group, errors.New("群聊不存在")
	}
	return group, nil
}
