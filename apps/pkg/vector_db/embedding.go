package vector_db

import (
	"context"
	"github.com/cloudwego/eino-ext/components/embedding/dashscope"
	"go-im-system/apps/pkg/config"
	"time"
)

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
