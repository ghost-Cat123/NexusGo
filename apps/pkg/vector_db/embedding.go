package vector_db

import (
	"context"
	"github.com/cloudwego/eino-ext/components/embedding/dashscope"
	"go-im-system/apps/pkg/config"
	"time"
)

const VectorDim int64 = 1024 // text-embedding-v3 的实际输出维度

func GetEmbedder(milvusConfig config.MilvusConfig) (*dashscope.Embedder, error) {
	ctx := context.Background()
	timeout := 30 * time.Second
	embedder, err := dashscope.NewEmbedder(ctx, &dashscope.EmbeddingConfig{
		APIKey:  milvusConfig.APIKey,
		Timeout: timeout,
		Model:   milvusConfig.ModelName,
	})
	return embedder, err
}
