package vector_db

import (
	"context"
	"sync"
	"time"

	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/logger"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

var (
	milvusCli *milvusclient.Client
	once      sync.Once
	initErr   error
)

func InitClient(milvusConfig config.MilvusConfig) error {
	once.Do(func() {
		var err error
		for i := 0; i < 15; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			milvusCli, err = milvusclient.New(ctx, &milvusclient.ClientConfig{Address: milvusConfig.Addr})
			cancel()
			if err == nil {
				break
			}
			logger.Log.Warnf("Milvus 连接建立失败，正在重试 (%d/15)... 错误: %v", i+1, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			initErr = err
			logger.Log.Errorf("Milvus 连接建立最终失败: %v", initErr) // 打印具体原因
			return
		}

		logger.Log.Infof("向量数据库单例初始化成功！")

		initErr = InitMessageCollection()
		if initErr != nil {
			logger.Log.Errorf("消息向量表初始化失败: %v", initErr)
		} else {
			logger.Log.Infof("消息向量表初始化成功！")
		}
		initErr = InitMemoryCollection()
		if initErr != nil {
			logger.Log.Errorf("记忆向量表初始化失败: %v", initErr)
		} else {
			logger.Log.Infof("记忆向量表初始化成功！")
		}
	})
	return initErr
}

func GetMilvus() *milvusclient.Client {
	if milvusCli == nil {
		panic("milvus not initialized")
	}
	return milvusCli
}

func CloseMilvus() {
	err := milvusCli.Close(context.Background())
	if err != nil {
		logger.Log.Errorf("Milvus关闭失败%v", err)
	}
}
