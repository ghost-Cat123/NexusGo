package main

import (
	"GeeRPC"
	"GeeRPC/midware"
	"go-im-system/apps/logic/service"
	"go-im-system/apps/pkg/cache"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/db"
	"go-im-system/apps/pkg/logger"
	"go-im-system/apps/pkg/mq"
	"go-im-system/apps/pkg/utils"
	"go-im-system/apps/pkg/vector_db"
	"log"
	"net"
	"strconv"
)

func main() {
	// 实际开发中，这个路径可以用命令行 flag 传进来，比如 -c ./config.yaml
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

	logicSnow := config.GlobalConfig.Server.LogicSnowflakeNode

	if err := utils.InitSnowflake(int64(logicSnow)); err != nil {
		logger.Log.Fatalf("Logic Snowflake 初始化失败: %v", err)
	}

	// 初始化 RabbitMQ：Logic 作为消息生产者，落库后将消息发布到目标网关
	if mqErr := mq.InitRabbitMQ(config.GlobalConfig.RabbitMQ.URL); mqErr != nil {
		logger.Log.Fatalf("初始化 RabbitMQ 失败: %v", mqErr)
	}
	defer mq.Close()

	// 启动上行 MQ 消费者（Gateway → MQ → Logic，替代原 SendMessage RPC）
	service.StartUploadConsumer()

	vectorDbInitErr := vector_db.InitClient(config.GlobalConfig.Milvus)
	if cacheInitErr != nil {
		logger.Log.Fatalf("连接向量数据库失败: %v", vectorDbInitErr)
	}

	// 启动向量数据库消费者 消息异步落入向量数据库
	service.StartVectorConsumer()

	GeeRPC.Use(midware.LoggerInterceptor, midware.RecoveryInterceptor)

	_ = GeeRPC.Register(new(service.LogicService))

	l, err := net.Listen("tcp", ":"+strconv.Itoa(config.GlobalConfig.Server.LogicPort))
	if err != nil {
		log.Fatal("network error:", err)
	}
	logger.Log.Info("Logic RPC 服务端启动成功，端口 :", strconv.Itoa(config.GlobalConfig.Server.LogicPort))

	GeeRPC.Accept(l)
}
