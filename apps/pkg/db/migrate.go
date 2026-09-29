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
	if db.Migrator().HasTable("messages") {
		if err := EnsureMessagesFullTextIndex(); err != nil {
			return err
		}
	}
	logger.Log.Infof("[DB] AutoMigrate 完成，共迁移 %d 张表", len(models))
	return nil
}

// EnsureMessagesFullTextIndex keeps the MySQL FULLTEXT prerequisite alongside
// the schema migration. GORM's AutoMigrate does not create FULLTEXT indexes.
func EnsureMessagesFullTextIndex() error {
	const indexName = "ft_messages_content"
	var count int64
	if err := db.Raw(`
		SELECT COUNT(*)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'messages'
		  AND INDEX_NAME = ?`, indexName).Scan(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	if err := db.Exec(`ALTER TABLE messages ADD FULLTEXT INDEX ft_messages_content (content) WITH PARSER ngram`).Error; err != nil {
		return err
	}
	logger.Log.Info("[DB] 已创建 messages.content 的 ngram FULLTEXT 索引")
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
