package vector_db

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const MemoryCollection = "memory_vectors"

// MemoryFields 向量数据库表字段（v2: 新增 importance / memory_type 标量字段，用于混合排序召回）
var MemoryFields = []*entity.Field{
	entity.NewField().WithName("memory_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true),
	entity.NewField().WithName("user_id").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("session_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64),
	entity.NewField().WithName("summary").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512),
	entity.NewField().WithName("importance").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("memory_type").WithDataType(entity.FieldTypeVarChar).WithMaxLength(32),
	entity.NewField().WithName("create_time").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(VectorDim),
}

func InitMemoryCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 已有 Collection 必须保留：启动过程不能隐式删除长期记忆。
	// Schema 变更应通过显式迁移完成，而不是在服务重启时重建。
	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(MemoryCollection))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Infof("Collection %s already exists; preserving existing memories", MemoryCollection)
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

	logger.Log.Infof("Milvus Collection [%s] 初始化并加载成功！(含 importance/memory_type 标量字段)", MemoryCollection)
	return nil
}
