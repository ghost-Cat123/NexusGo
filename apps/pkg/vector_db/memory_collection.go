package vector_db

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"fmt"
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

	// 数据库表是否存在 → 若存在则删除重建（开发环境，保证 schema 最新）
	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(MemoryCollection))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Warnf("Collection %s 已存在，删除重建以更新 schema...", MemoryCollection)
		if err := milvusCli.DropCollection(ctx, milvusclient.NewDropCollectionOption(MemoryCollection)); err != nil {
			return fmt.Errorf("删除旧 Collection 失败: %w", err)
		}
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
