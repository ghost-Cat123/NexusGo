package main

import (
	"time"

	"github.com/gin-gonic/gin"
	"go-im-system/apps/gateway/router"
	"go-im-system/apps/gateway/rpcclient"
	"go-im-system/apps/gateway/ws"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/db"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/mq"
	"go-im-system/apps/pkg/utils"
	"log"
	"strconv"
)

// 入口函数
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

	gwSnow := config.GlobalConfig.Server.GatewaySnowflakeNode

	if err := utils.InitSnowflake(int64(gwSnow)); err != nil {
		logger.Log.Fatalf("网关 Snowflake 初始化失败: %v", err)
	}

	rpcclient.InitRPCClient()

	// 初始化 RabbitMQ：Gateway 作为消息消费者，订阅自身专属 Queue
	gatewayAddr := config.GlobalConfig.Server.GatewayAddr
	if mqErr := mq.InitRabbitMQ(config.GlobalConfig.RabbitMQ.URL); mqErr != nil {
		logger.Log.Fatalf("初始化 RabbitMQ 失败: %v", mqErr)
	}
	defer mq.Close()
	ws.StartMQConsumer(gatewayAddr) // 开启异步消费协程

	ws.InitGatewayReliability(&ws.ReliabilityConfig{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Minute * 2,
		MaxRetry:       12,
		DedupWindow:    24 * time.Hour,
	})
	ws.SetOnTransportDelivered(ws.NotifyDeliveredRPC)

	// 启动Gin服务器端
	r := gin.Default()

	// 绑定路由
	r = router.SetupRouter()

	// 启动 HTTP服务器 监听端口
	err := r.Run(":" + strconv.Itoa(config.GlobalConfig.Server.GatewayPort))
	if err != nil {
		return
	}
}
