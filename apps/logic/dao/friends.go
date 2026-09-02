package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
	"time"
)

type PendingFriendRequest struct {
	UserID    int64     `gorm:"column:user_id"`
	Username  string    `gorm:"column:username"`
	Nickname  string    `gorm:"column:nickname"`
	Avatar    string    `gorm:"column:avatar"`
	Remark    string    `gorm:"column:remark"`
	CreatedAt time.Time `gorm:"column:create_time"`
}

func ApplyFriend(userID, friendID int64, remark string) error {
	friend := &models.Friend{
		UserID:   userID,
		FriendID: friendID,
		Status:   0,
		Remark:   remark,
	}
	return db.GetDB().Create(friend).Error
}

func ResolveFriend(userID, friendID int64, status int8) error {
	return db.GetDB().Model(&models.Friend{}).
		Where("((user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)) AND status = 0", userID, friendID, friendID, userID).
		Updates(map[string]interface{}{"status": status, "update_time": time.Now()}).Error
}

func GetFriendList(userID int64) ([]models.Friend, error) {
	var friends []models.Friend
	err := db.GetDB().Where("user_id = ? OR friend_id = ?", userID, userID).Find(&friends).Error
	return friends, err
}

func DeleteFriend(userID, friendID int64) error {
	return db.GetDB().Where("(user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)", userID, friendID, friendID, userID).
		Delete(&models.Friend{}).Error
}

func GetPendingFriendRequests(userID int64) ([]PendingFriendRequest, error) {
	var results []PendingFriendRequest
	err := db.GetDB().Table("friends f").
		Select("f.user_id, u.username, u.nickname, u.avatar, f.remark, f.create_time").
		Joins("JOIN users u ON u.user_id = f.user_id").
		Where("f.friend_id = ? AND f.status = 0", userID).
		Order("f.create_time DESC").
		Find(&results).Error
	return results, err
}
