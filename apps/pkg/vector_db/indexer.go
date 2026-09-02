package vector_db

import (
	"context"
	milvusidex "github.com/cloudwego/eino-ext/components/indexer/milvus2"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/column"
)

func GetIndxer(ctx context.Context, collectionName string, embedder embedding.Embedder,
	documentConverter func(context.Context, []*schema.Document, [][]float64) ([]column.Column, error)) (*milvusidex.Indexer, error) {
	return milvusidex.NewIndexer(ctx, &milvusidex.IndexerConfig{
		Client:     milvusCli,
		Collection: collectionName,
		Vector: &milvusidex.VectorConfig{
			VectorField:  "embedding",
			Dimension:    VectorDim,
			MetricType:   milvusidex.COSINE,
			IndexBuilder: milvusidex.NewHNSWIndexBuilder().WithM(8).WithEfConstruction(200),
		},
		Embedding:         embedder,
		DocumentConverter: documentConverter,
	})
}

func MessageDocumentConverter(_ context.Context, docs []*schema.Document, vectors [][]float64) ([]column.Column, error) {
	msgIDs := make([]string, len(docs)) // 【改动 1】
	convIDs := make([]string, len(docs))
	senderIDs := make([]int64, len(docs))
	senderTimes := make([]int64, len(docs))
	embeddings := make([][]float32, len(docs))
	for i, doc := range docs {
		// 这里是从你 Logic 层或 Test 里组装的 MetaData 中提取
		msgIDs[i] = doc.MetaData["msg_id"].(string)
		convIDs[i] = doc.MetaData["conv_id"].(string)
		senderIDs[i] = doc.MetaData["sender_id"].(int64)
		senderTimes[i] = doc.MetaData["send_time"].(int64)
		// 向量 float64 转 float32
		emb := make([]float32, len(vectors[i]))
		for j, v := range vectors[i] {
			emb[j] = float32(v)
		}
		embeddings[i] = emb
	}
	// 拼成 Milvus 需要的强类型列
	return []column.Column{
		column.NewColumnVarChar("msg_id", msgIDs), // 【改动 3】
		column.NewColumnVarChar("conv_id", convIDs),
		column.NewColumnInt64("sender_id", senderIDs),
		column.NewColumnInt64("send_time", senderTimes),
		column.NewColumnFloatVector("embedding", int(VectorDim), embeddings),
	}, nil
}

func MemoryDocumentConverter(_ context.Context, docs []*schema.Document, vectors [][]float64) ([]column.Column, error) {
	memoryIDs := make([]string, len(docs))
	userIDs := make([]int64, len(docs))
	sessionIDs := make([]string, len(docs))
	summary := make([]string, len(docs))
	importances := make([]int64, len(docs))
	memoryTypes := make([]string, len(docs))
	createTimes := make([]int64, len(docs))
	embeddings := make([][]float32, len(docs))
	for i, doc := range docs {
		memoryIDs[i] = doc.ID
		summary[i] = doc.Content
		userIDs[i] = doc.MetaData["user_id"].(int64)
		sessionIDs[i] = doc.MetaData["session_id"].(string)
		createTimes[i] = doc.MetaData["create_time"].(int64)
		// importance / memory_type 带默认值兜底
		if v, ok := doc.MetaData["importance"]; ok {
			importances[i] = v.(int64)
		}
		if v, ok := doc.MetaData["memory_type"]; ok {
			memoryTypes[i] = v.(string)
		} else {
			memoryTypes[i] = "general"
		}
		emb := make([]float32, len(vectors[i]))
		for j, v := range vectors[i] {
			emb[j] = float32(v)
		}
		embeddings[i] = emb
	}
	return []column.Column{
		column.NewColumnVarChar("memory_id", memoryIDs),
		column.NewColumnInt64("user_id", userIDs),
		column.NewColumnVarChar("session_id", sessionIDs),
		column.NewColumnVarChar("summary", summary),
		column.NewColumnInt64("importance", importances),
		column.NewColumnVarChar("memory_type", memoryTypes),
		column.NewColumnInt64("create_time", createTimes),
		column.NewColumnFloatVector("embedding", int(VectorDim), embeddings),
	}, nil
}
