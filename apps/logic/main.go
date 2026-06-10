package main

import (
	"GrowRPC"
	"GrowRPC/midware"
	"GrowRPC/registry"
	"NexusGo/apps/logic/models"
	"NexusGo/apps/logic/service"
	"NexusGo/apps/pkg/cache"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/logger"
	"NexusGo/apps/pkg/mq"
	"NexusGo/apps/pkg/proto/pb_friend"
	"NexusGo/apps/pkg/proto/pb_group"
	"NexusGo/apps/pkg/proto/pb_msg"
	"NexusGo/apps/pkg/proto/pb_user"
	"NexusGo/apps/pkg/utils"
	"context"
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
	// 自动建表（幂等，生产环境安全）
	db.MustAutoMigrate(
		&models.User{},
		&models.Messages{},
		&models.Group{},
		&models.GroupMember{},
		&models.Friend{},
		&models.Conversation{},
	)

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

	// ─── RPC 框架初始化 ───
	GrowRPC.Use(midware.LoggerInterceptor, midware.RecoveryInterceptor)

	// 使用代码生成的注册函数，泛型零反射注册
	logicService := new(service.LogicService)
	pb_user.RegisterUserServiceServer(GrowRPC.DefaultServer, logicService)
	pb_msg.RegisterMsgServiceServer(GrowRPC.DefaultServer, logicService)
	pb_friend.RegisterFriendServiceServer(GrowRPC.DefaultServer, logicService)
	pb_group.RegisterGroupServiceServer(GrowRPC.DefaultServer, logicService)

	// etcd 服务注册
	etcdEndpoints := config.GlobalConfig.Server.EtcdEndpoints
	if len(etcdEndpoints) > 0 {
		etcdReg, regErr := registry.NewEtcdRegistry(etcdEndpoints, 10)
		if regErr != nil {
			logger.Log.Fatalf("etcd 注册中心连接失败: %v", regErr)
		}
		addr := "tcp@localhost:" + strconv.Itoa(config.GlobalConfig.Server.LogicPort)
		if regErr = etcdReg.Register("LogicService", addr, nil); regErr != nil {
			logger.Log.Fatalf("etcd 服务注册失败: %v", regErr)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			if err := etcdReg.KeepAlive(ctx); err != nil {
				logger.Log.Errorf("etcd KeepAlive 退出: %v", err)
			}
		}()
		logger.Log.Info("etcd 服务注册成功，服务名: LogicService，地址: " + addr)
	}

	l, err := net.Listen("tcp", ":"+strconv.Itoa(config.GlobalConfig.Server.LogicPort))
	if err != nil {
		log.Fatal("network error:", err)
	}
	logger.Log.Info("Logic RPC 服务端启动成功，端口 :", strconv.Itoa(config.GlobalConfig.Server.LogicPort))

	GrowRPC.Accept(l)
}
