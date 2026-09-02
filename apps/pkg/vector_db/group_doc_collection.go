package vector_db

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"time"

	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const GroupDocCollection = "group_docs"

// GroupDocFields 群文档向量表字段
var GroupDocFields = []*entity.Field{
	entity.NewField().WithName("chunk_id").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true),
	entity.NewField().WithName("group_id").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("file_name").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128),
	entity.NewField().WithName("chunk_idx").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("content").WithDataType(entity.FieldTypeVarChar).WithMaxLength(1024),
	entity.NewField().WithName("upload_time").WithDataType(entity.FieldTypeInt64),
	entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(VectorDim),
}

func InitGroupDocCollection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := milvusCli.HasCollection(ctx, milvusclient.NewHasCollectionOption(GroupDocCollection))
	if err != nil {
		return err
	}
	if exists {
		logger.Log.Infof("Collection %s already exists", GroupDocCollection)
		return nil
	}

	schema := entity.NewSchema()
	for _, f := range GroupDocFields {
		schema.WithField(f)
	}
	if err := milvusCli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(GroupDocCollection, schema)); err != nil {
		return err
	}

	idx := index.NewHNSWIndex(entity.COSINE, 8, 200)
	if _, err := milvusCli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(GroupDocCollection, "embedding", idx)); err != nil {
		return err
	}

	loadTask, err := milvusCli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(GroupDocCollection))
	if err != nil {
		return err
	}
	if err := loadTask.Await(ctx); err != nil {
		return err
	}

	logger.Log.Infof("Milvus Collection [%s] 初始化成功！", GroupDocCollection)
	return nil
}

// GroupDocConverter 群文档 → Milvus 列
func GroupDocConverter(_ context.Context, docs []*schema.Document, vectors [][]float64) ([]column.Column, error) {
	chunkIDs := make([]string, len(docs))
	groupIDs := make([]int64, len(docs))
	fileNames := make([]string, len(docs))
	chunkIdxs := make([]int64, len(docs))
	contents := make([]string, len(docs))
	uploadTimes := make([]int64, len(docs))
	embeddings := make([][]float32, len(docs))

	for i, doc := range docs {
		chunkIDs[i] = doc.ID
		contents[i] = doc.Content
		groupIDs[i] = doc.MetaData["group_id"].(int64)
		fileNames[i] = doc.MetaData["file_name"].(string)
		chunkIdxs[i] = doc.MetaData["chunk_idx"].(int64)
		uploadTimes[i] = doc.MetaData["upload_time"].(int64)

		emb := make([]float32, len(vectors[i]))
		for j, v := range vectors[i] {
			emb[j] = float32(v)
		}
		embeddings[i] = emb
	}

	return []column.Column{
		column.NewColumnVarChar("chunk_id", chunkIDs),
		column.NewColumnInt64("group_id", groupIDs),
		column.NewColumnVarChar("file_name", fileNames),
		column.NewColumnInt64("chunk_idx", chunkIdxs),
		column.NewColumnVarChar("content", contents),
		column.NewColumnInt64("upload_time", uploadTimes),
		column.NewColumnFloatVector("embedding", int(VectorDim), embeddings),
	}, nil
}
