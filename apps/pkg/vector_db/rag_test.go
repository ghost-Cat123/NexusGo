package vector_db

import (
	"context"
	"fmt"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/schema"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"log"
	"testing"
	"time"
)

func TestVectorDatabaseRAG(t *testing.T) {
	ctx := context.Background()
	if err := config.InitConfig(""); err != nil {
		log.Fatalf("配置初始化失败: %v", err)
	}
	// 0. 初始化日志系统 (修复空指针的关键！)
	logger.InitLogger()
	// 1. 手动配置测试环境

	vectorDbInitErr := InitClient(config.GlobalConfig.Milvus)
	if vectorDbInitErr != nil {
		logger.Log.Fatalf("连接向量数据库失败: %v", vectorDbInitErr)
	}
	// 2. 初始化 Milvus 连接并确保 Collection 存在
	if err := InitMessageCollection(); err != nil {
		t.Fatalf("InitCollection 失败: %v", err)
	}
	// 3. 获取 Embedder、Indexer、Retriever
	embedder, err := GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		t.Fatalf("获取 Embedder 失败: %v", err)
	}
	indexer, err := GetIndxer(ctx, MessageCollection, embedder, MessageDocumentConverter) // 注意：如果你改了函数名叫 GetIndexer 请对应修改
	if err != nil {
		t.Fatalf("获取 Indexer 失败: %v", err)
	}

	// 输出字段
	outputFields := []string{"msg_id", "sender_id", "send_time"}
	retriever, err := GetRetriever(ctx, MessageCollection, "embedding", outputFields, 1, embedder)
	if err != nil {
		t.Fatalf("获取 Retriever 失败: %v", err)
	}
	// 4. 准备模拟聊天数据并写入
	testConvID := "1_2"
	docs := []*schema.Document{
		{
			ID:      "1001",
			Content: "你说得对，但是原神是一款二次元开放大世界游戏",
			MetaData: map[string]any{
				"msg_id":    "1001",
				"conv_id":   testConvID,
				"sender_id": int64(1),
				"send_time": time.Now().Unix(),
			},
		},
		{
			ID:      "1002",
			Content: "今天晚上吃什么？去吃火锅怎么样？",
			MetaData: map[string]any{
				"msg_id":    "1002",
				"conv_id":   testConvID,
				"sender_id": int64(2),
				"send_time": time.Now().Unix(),
			},
		},
	}
	fmt.Println("🚀 正在将模拟数据写入 Milvus...")
	ids, err := indexer.Store(ctx, docs)
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	fmt.Printf("✅ 写入成功，IDs: %v\n", ids)
	// Milvus 写入后需要等待一小段时间建立索引
	time.Sleep(2 * time.Second)
	// 5. 进行语义搜索
	query := "吃饭计划" // 没包含"原神"，完全是语义召回
	filterExpr := fmt.Sprintf("conv_id == \"%s\"", testConvID)

	fmt.Printf("\n🔍 正在搜索问题: [%s]...\n", query)
	results, err := retriever.Retrieve(ctx, query, milvusret.WithFilter(filterExpr))
	if err != nil {
		t.Fatalf("搜索失败: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("❌ 搜索失败: 没搜到任何内容")
	}
	// 6. 验证结果
	fmt.Println("🎉 召回成功！结果如下：")
	for i, res := range results {
		fmt.Printf("Top %d:\n", i+1)
		fmt.Printf("  - MsgID: %v\n", res.MetaData["msg_id"])
		fmt.Printf("  - SenderID: %v\n", res.MetaData["sender_id"])
		fmt.Printf("  - 原始文本: %s\n", res.Content)
	}
}
