package dao

import (
	"go-im-system/apps/logic/models"
	"go-im-system/apps/pkg/db"
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
