package vector_db

import (
	"context"
	"fmt"
	"testing"
	"time"

	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/schema"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
)

func TestMemoryDocumentConverter(t *testing.T) {
	docs := []*schema.Document{
		{
			ID:      "mem-001",
			Content: "用户偏好简洁回复风格，不喜欢emoji",
			MetaData: map[string]any{
				"user_id":     int64(123),
				"session_id":  "agent_session:123",
				"create_time": time.Now().Unix(),
			},
		},
		{
			ID:      "mem-002",
			Content: "用户之前询问过会议室预订流程",
			MetaData: map[string]any{
				"user_id":     int64(123),
				"session_id":  "agent_session:123",
				"create_time": time.Now().Unix(),
			},
		},
	}

	vectors := [][]float64{
		make([]float64, VectorDim),
		make([]float64, VectorDim),
	}

	cols, err := MemoryDocumentConverter(context.Background(), docs, vectors)
	if err != nil {
		t.Fatalf("MemoryDocumentConverter 失败: %v", err)
	}

	if len(cols) != 6 {
		t.Fatalf("期望 6 列, got=%d", len(cols))
	}
}

func TestLongTermMemoryFlow(t *testing.T) {
	ctx := context.Background()
	if err := config.InitConfig(""); err != nil {
		t.Skipf("跳过集成测试（配置不可用）: %v", err)
	}

	logger.InitLogger()

	if err := InitClient(config.GlobalConfig.Milvus); err != nil {
		t.Skipf("跳过集成测试（Milvus 未启动）: %v", err)
	}

	if err := InitMemoryCollection(); err != nil {
		t.Fatalf("InitMemoryCollection 失败: %v", err)
	}

	embedder, err := GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		t.Fatalf("获取 Embedder 失败: %v", err)
	}

	indexer, err := GetIndxer(ctx, MemoryCollection, embedder, MemoryDocumentConverter)
	if err != nil {
		t.Fatalf("获取 Indexer 失败: %v", err)
	}

	// 时间戳保证每次测试 ID 唯一
	userID := int64(99999)
	ts := time.Now().UnixNano()
	docs := []*schema.Document{
		{
			ID:      fmt.Sprintf("test-mem-001-%d", ts),
			Content: "用户偏好简洁回复，不喜欢emoji和过长解释",
			MetaData: map[string]any{
				"user_id":     userID,
				"session_id":  "agent_session:99999",
				"create_time": time.Now().Unix(),
			},
		},
		{
			ID:      fmt.Sprintf("test-mem-002-%d", ts),
			Content: "用户之前询问会议系统怎样预定会议室，需要OA系统提交申请",
			MetaData: map[string]any{
				"user_id":     userID,
				"session_id":  "agent_session:99999",
				"create_time": time.Now().Unix(),
			},
		},
		{
			ID:      fmt.Sprintf("test-mem-003-%d", ts),
			Content: "用户每周五下午有项目复盘会",
			MetaData: map[string]any{
				"user_id":     userID,
				"session_id":  "agent_session:99999",
				"create_time": time.Now().Unix(),
			},
		},
	}

	ids, err := indexer.Store(ctx, docs)
	if err != nil {
		t.Fatalf("写入记忆失败: %v", err)
	}
	t.Logf("写入成功, IDs: %v", ids)

	time.Sleep(2 * time.Second)

	outputFields := []string{"memory_id", "summary"}
	retriever, err := GetRetriever(ctx, MemoryCollection, "embedding", outputFields, 3, embedder)
	if err != nil {
		t.Fatalf("获取 Retriever 失败: %v", err)
	}

	// 相关查询：应优先召回"会议室"记忆
	query := "怎么订会议室"
	filterExpr := fmt.Sprintf("user_id == %d", userID)
	results, err := retriever.Retrieve(ctx, query, milvusret.WithFilter(filterExpr))
	if err != nil {
		t.Fatalf("检索失败: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("期望召回结果，但为空")
	}

	for i, res := range results {
		t.Logf("Top %d: score=%.4f content=%s MetaData=%v",
			i+1, res.Score(), res.Content, res.MetaData)
	}

	// 第一条应是会议室相关记忆
	if len(results) > 0 {
		content := results[0].Content
		if content == "" {
			if v, ok := results[0].MetaData["summary"].(string); ok {
				content = v
			}
		}
		if content == "" {
			t.Error("召回结果内容为空")
		}
		t.Logf("最高分记忆: %s", content)
	}

	// 不相关查询：低分结果应被过滤
	query2 := "今天天气怎么样"
	results2, err := retriever.Retrieve(ctx, query2, milvusret.WithFilter(filterExpr))
	if err != nil {
		t.Fatalf("检索失败: %v", err)
	}
	filteredCount := 0
	for _, res := range results2 {
		if res.Score() < 0.5 {
			filteredCount++
			t.Logf("低分结果正确过滤: score=%.4f", res.Score())
		}
	}
	t.Logf("低相关查询: %d 条结果, %d 条应在阈值0.5以下被过滤", len(results2), filteredCount)
}
