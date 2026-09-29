package vector_db

import (
	"context"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino-ext/components/retriever/milvus2/search_mode"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
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
	conf := &milvusret.RetrieverConfig{
		Client:       GetMilvus(),
		Collection:   collectionName,
		VectorField:  vectorField,
		OutputFields: outputFields,
		TopK:         topK,
		Embedding:    embedder,
		SearchMode:   search_mode.NewApproximate(milvusret.COSINE),
	}
	if collectionName == MessageCollection {
		// The Eino default converter only recognizes a primary key named "id".
		// MessageCollection uses msg_id, so preserve it as Document.ID for the
		// authorized MySQL source-message lookup performed by callers.
		conf.DocumentConverter = messageResultConverter
		// Messages are written by a different service than the Agent search
		// process. Strong consistency avoids a user failing to find a message
		// immediately after it has been indexed.
		conf.ConsistencyLevel = milvusret.ConsistencyLevelStrong
	}
	return milvusret.NewRetriever(ctx, conf)
}

func messageResultConverter(_ context.Context, result milvusclient.ResultSet) ([]*schema.Document, error) {
	docs := make([]*schema.Document, 0, result.ResultCount)
	for i := 0; i < result.ResultCount; i++ {
		doc := &schema.Document{MetaData: make(map[string]any)}
		if i < len(result.Scores) {
			doc = doc.WithScore(float64(result.Scores[i]))
		}
		for _, field := range result.Fields {
			value, err := field.Get(i)
			if err != nil {
				continue
			}
			doc.MetaData[field.Name()] = value
			if field.Name() == "msg_id" {
				if id, err := field.GetAsString(i); err == nil {
					doc.ID = id
				}
			}
		}
		docs = append(docs, doc)
	}
	return docs, nil
}
