package db

import (
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
	"time"
)

// DB 全局变量私有化
var (
	db *gorm.DB
	// 保证只注册一次
	once    sync.Once
	initErr error
)

func InitMySQL(sqlConfig config.MySQLConfig) error {
	once.Do(func() {
		db, initErr = gorm.Open(mysql.Open(sqlConfig.DSN), &gorm.Config{})
		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(sqlConfig.MaxIdleConns)
		sqlDB.SetMaxOpenConns(sqlConfig.MaxOpenConns)
		sqlDB.SetConnMaxLifetime(time.Hour)
		logger.Log.Infof("数据库单例初始化成功！max_idle=%d max_open=%d",
			sqlConfig.MaxIdleConns, sqlConfig.MaxOpenConns)
	})
	return initErr
}

func GetDB() *gorm.DB {
	if db == nil {
		panic("db not initialized")
	}
	return db
}
