package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
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

func GetGroupByID(groupID int64) (models.Group, error) {
	var group models.Group
	err := db.GetDB().Where("group_id = ?", groupID).First(&group).Error
	return group, err
}

func GetGroupsByIDs(groupIDs []int64) ([]models.Group, error) {
	var groups []models.Group
	err := db.GetDB().Where("group_id IN ?", groupIDs).Find(&groups).Error
	return groups, err
}

func DeleteGroup(groupID int64) error {
	return db.GetDB().Where("group_id = ?", groupID).Delete(&models.Group{}).Error
}

func SearchGroups(keyword string) ([]models.Group, error) {
	var groups []models.Group
	err := db.GetDB().Where("group_name LIKE ?", "%"+keyword+"%").Find(&groups).Error
	return groups, err
}
