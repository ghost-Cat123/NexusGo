package vector_db

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"time"
)

const MessageCollection = "message_vectors"

// MessageFields 向量数据库表字段
var MessageFields = []*entity.Field{
	entity.NewField().WithName("msg_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true),
	entity.NewField().WithName("conv_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64),
	entity.NewField().WithName("sender_id").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("send_time").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(VectorDim),
}

func InitMessageCollection() error {
	// 【关键修改 2】：建表同样加上超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(MessageCollection))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Infof("Collection %s already exists", MessageCollection)
		return nil
	}

	schema := entity.NewSchema()
	for _, f := range MessageFields {
		schema.WithField(f)
	}

	if err := milvusCli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(MessageCollection, schema)); err != nil {
		return err
	}

	idx := index.NewHNSWIndex(entity.COSINE, 8, 200)
	if _, err := milvusCli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(MessageCollection, "embedding", idx)); err != nil {
		return err
	}

	loadTask, err := milvusCli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(MessageCollection))
	if err != nil {
		return err
	}
	if err := loadTask.Await(ctx); err != nil {
		return err
	}

	logger.Log.Infof("Milvus Collection [%s] 初始化并加载成功！", MessageCollection)
	return nil
}
