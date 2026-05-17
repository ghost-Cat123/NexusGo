package main

import (
	"github.com/gin-gonic/gin"
	"go-im-system/apps/agent/handler"
	"go-im-system/apps/agent/task"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/db"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/utils"
	"go-im-system/apps/pkg/vector_db"
	"log"
	"strconv"
)

func main() {
	if err := config.InitConfig(""); err != nil {
		log.Fatalf("配置初始化失败: %v", err)
	}
	// 初始化生产级日志
	logger.InitLogger()

	// 确保程序退出时把缓冲区里的日志落盘
	defer logger.Log.Sync()

	dbInitErr := db.InitMySQL(config.GlobalConfig.MySQL)
	if dbInitErr != nil {
		logger.Log.Fatalf("连接数据库失败: %v", dbInitErr)
	}

	cacheInitErr := cache.InitRedis(config.GlobalConfig.Redis)
	if cacheInitErr != nil {
		logger.Log.Fatalf("连接缓存失败: %v", cacheInitErr)
	}

	vectorDbInitErr := vector_db.InitClient(config.GlobalConfig.Milvus)
	if vectorDbInitErr != nil {
		logger.Log.Fatalf("连接向量数据库失败: %v", vectorDbInitErr)
	}

	gwSnow := config.GlobalConfig.Server.AgentSnowflakeNode

	if err := utils.InitSnowflake(int64(gwSnow)); err != nil {
		logger.Log.Fatalf("网关 Snowflake 初始化失败: %v", err)
	}

	task.StartCronJobs()

	r := gin.Default()
	// 改成SEE接口和网关层对接
	r.GET("/agent/chat/sse", handler.ChatSSE)

	logger.Log.Infof("Agent 服务启动，端口 :%s", strconv.Itoa(config.GlobalConfig.Server.AgentPort))
	err := r.Run(":" + strconv.Itoa(config.GlobalConfig.Server.AgentPort))
	if err != nil {
		return
	}
}
