package vector_db

import (
	"context"
	"sync"
	"time"

	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
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

const CollectionName = "message_vectors"
const VectorDim int64 = 1024 // text-embedding-v3 的实际输出维度

// MessageFields 向量数据库表字段
var MessageFields = []*entity.Field{
	entity.NewField().WithName("msg_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true),
	entity.NewField().WithName("conv_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64),
	entity.NewField().WithName("sender_id").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("send_time").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(VectorDim),
}

func InitCollection() error {
	// 【关键修改 2】：建表同样加上超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(CollectionName))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Infof("Collection %s already exists", CollectionName)
		return nil
	}

	schema := entity.NewSchema()
	for _, f := range MessageFields {
		schema.WithField(f)
	}

	if err := milvusCli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(CollectionName, schema)); err != nil {
		return err
	}

	idx := index.NewHNSWIndex(entity.COSINE, 8, 200)
	if _, err := milvusCli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(CollectionName, "embedding", idx)); err != nil {
		return err
	}

	loadTask, err := milvusCli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(CollectionName))
	if err != nil {
		return err
	}
	if err := loadTask.Await(ctx); err != nil {
		return err
	}

	logger.Log.Infof("Milvus Collection [%s] 初始化并加载成功！", CollectionName)
	return nil
}
