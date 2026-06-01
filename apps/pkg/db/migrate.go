package db

import (
	"NexusGo/apps/pkg/logger"
	"gorm.io/gorm"
)

// AutoMigrateTables 执行 GORM AutoMigrate，幂等：表不存在则建，字段不存在则加列，不会删除字段。
// models 传入需要迁移的结构体指针列表，由各服务的 main.go 在 InitMySQL 后调用。
func AutoMigrateTables(models ...interface{}) error {
	if db == nil {
		panic("db not initialized, call InitMySQL first")
	}
	if err := db.AutoMigrate(models...); err != nil {
		return err
	}
	logger.Log.Infof("[DB] AutoMigrate 完成，共迁移 %d 张表", len(models))
	return nil
}

// MustAutoMigrate 同 AutoMigrateTables，但失败时直接 Fatal，适合 main 函数启动阶段调用。
func MustAutoMigrate(models ...interface{}) {
	if err := AutoMigrateTables(models...); err != nil {
		logger.Log.Fatalf("[DB] AutoMigrate 失败，服务终止: %v", err)
	}
}

// tableExists 工具函数：检查表是否存在（可选用于条件日志，AutoMigrate 本身已幂等）
func tableExists(db *gorm.DB, tableName string) bool {
	return db.Migrator().HasTable(tableName)
}
