package vector_db

import (
	"context"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino-ext/components/retriever/milvus2/search_mode"
	"github.com/cloudwego/eino/components/embedding"
)

// GetRetriever 返回一个通用的 Milvus 语义检索器
// 动态参数（TopK、Filter）请在调用 Retrieve() 时通过 Option 传入
func GetRetriever(
	ctx context.Context,
	collectionName string,
	vectorField string,
	outputFields []string,
	topK int,
	embedder embedding.Embedder,
) (*milvusret.Retriever, error) {
	return milvusret.NewRetriever(ctx, &milvusret.RetrieverConfig{
		Client:       GetMilvus(),
		Collection:   collectionName,
		VectorField:  vectorField,
		OutputFields: outputFields,
		TopK:         topK,
		Embedding:    embedder,
		SearchMode:   search_mode.NewApproximate(milvusret.COSINE),
	})
}
