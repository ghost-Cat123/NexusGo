package dao

import (
	"NexusGo/apps/logic/models"
	"NexusGo/apps/pkg/db"
	"errors"
)

func FindUserByName(userName string) (models.User, error) {
	var user models.User
	result := db.GetDB().Where("username = ? ", userName).Find(&user)
	if result.Error != nil {
		return user, result.Error
	}
	if result.RowsAffected == 0 {
		return user, errors.New("用户不存在")
	}
	return user, nil
}

func GetUserByID(userID int64) (models.User, error) {
	var user models.User
	err := db.GetDB().Where("user_id = ?", userID).First(&user).Error
	return user, err
}

func SearchUsers(keyword string) ([]models.User, error) {
	var users []models.User
	// 优先尝试全文索引（需先执行 sql/ft_users_search.sql）：
	// MySQL 的 MATCH AGAINST IN BOOLEAN MODE 支持通配符 + 中文分词
	err := db.GetDB().
		Where("MATCH(username, nickname) AGAINST (? IN BOOLEAN MODE)", keyword+"*").
		Find(&users).Error
	if err != nil || len(users) == 0 {
		// 降级：全文索引未创建或无匹配时回退到 LIKE
		err = db.GetDB().
			Where("username LIKE ? OR nickname LIKE ?", "%"+keyword+"%", "%"+keyword+"%").
			Find(&users).Error
	}
	return users, err
}

func GetUsersByIDs(userIDs []int64) (map[int64]models.User, error) {
	if len(userIDs) == 0 {
		return map[int64]models.User{}, nil
	}
	var users []models.User
	err := db.GetDB().Where("user_id IN ?", userIDs).Find(&users).Error
	if err != nil {
		return nil, err
	}
	result := make(map[int64]models.User, len(users))
	for _, u := range users {
		result[u.UserId] = u
	}
	return result, nil
}

func InsertUser(user *models.User) error {
	result := db.GetDB().Create(user)
	return result.Error
}

func UpdateUser(userID int64, updates map[string]interface{}) error {
	return db.GetDB().Model(&models.User{}).Where("user_id = ?", userID).Updates(updates).Error
}
