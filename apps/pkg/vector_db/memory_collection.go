package vector_db

import (
	"context"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"go-im-system/apps/pkg/logger"
	"time"
)

const MemoryCollection = "memory_vectors"

// MemoryFields 向量数据库表字段
var MemoryFields = []*entity.Field{
	entity.NewField().WithName("memory_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true),
	entity.NewField().WithName("user_id").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("session_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64),
	entity.NewField().WithName("summary").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512),
	entity.NewField().WithName("create_time").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(VectorDim),
}

func InitMemoryCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 数据库表是否存在
	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(MemoryCollection))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Infof("Collection %s already exists", MemoryCollection)
		return nil
	}

	// 建表
	schema := entity.NewSchema()
	for _, f := range MemoryFields {
		schema.WithField(f)
	}
	if err := milvusCli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(MemoryCollection, schema)); err != nil {
		return err
	}

	// 创建索引
	idx := index.NewHNSWIndex(entity.COSINE, 8, 200)
	if _, err := milvusCli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(MemoryCollection, "embedding", idx)); err != nil {
		return err
	}

	// 加载数据库
	loadTask, err := milvusCli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(MemoryCollection))
	if err != nil {
		return err
	}
	if err := loadTask.Await(ctx); err != nil {
		return err
	}

	logger.Log.Infof("Milvus Collection [%s] 初始化并加载成功！", MemoryCollection)
	return nil
}
