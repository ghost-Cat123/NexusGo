package vector_db

import (
	"context"
	"sync"
	"time"

	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

var (
	milvusCli *milvusclient.Client
	once      sync.Once
	initErr   error
)

func InitClient(milvusConfig config.MilvusConfig) error {
	once.Do(func() {
		// 【关键修改 1】：加上 3 秒超时限制，不要让它无限卡死
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		milvusCli, initErr = milvusclient.New(ctx, &milvusclient.ClientConfig{Address: milvusConfig.Addr})
		if initErr != nil {
			logger.Log.Errorf("Milvus 连接建立失败: %v", initErr) // 打印具体原因
			return
		}

		logger.Log.Infof("向量数据库单例初始化成功！")

		initErr = InitCollection()
		if initErr != nil {
			logger.Log.Errorf("向量数据库表初始化失败: %v", initErr)
		} else {
			logger.Log.Infof("向量数据库表初始化成功！")
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
