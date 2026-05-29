package main

import (
	"context"
	"errors"
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
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if err := config.InitConfig(""); err != nil {
		log.Fatalf("配置初始化失败: %v", err)
	}
	// 初始化生产级日志
	logger.InitLogger()

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

	cronScheduler := task.StartCronJobs()

	r := gin.Default()
	// SSE路由
	r.GET("/agent/chat/sse", handler.ChatSSE)
	// 审批路由
	r.POST("/agent/chat/approve", handler.ApproveSSE)
	port := strconv.Itoa(config.GlobalConfig.Server.AgentPort)
	// 创建http服务
	serve := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// 监听os信号
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 在协程中启动http服务
	go func() {
		logger.Log.Infof("Agent 服务启动，端口 :%s", port)
		if err := serve.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()
	// 阻塞直到收到http关闭信号
	<-ctx.Done()
	logger.Log.Infof("收到退出信号，开始关闭")
	// 停止HTTP服务（设置5s超时，等待所有请求处理完毕）
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := serve.Shutdown(shutdownCtx); err != nil {
		logger.Log.Errorf("HTTP 关闭异常: %v", err)
	}

	// 按序停止服务，逆序
	cronScheduler.Stop()
	vector_db.CloseMilvus()
	cache.CloseRedis()
	db.CloseMySQL()

	logger.Log.Infof("服务已安全退出")
	if err := logger.Log.Sync(); err != nil {
		logger.Log.Errorf("日志落盘异常: %v", err)
	}
}
