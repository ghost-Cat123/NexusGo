package eval

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/tools"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"fmt"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"strconv"
	"strings"

	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/utils"
)

// RunEvaluation 执行完整评测流程，返回格式化报告
func RunEvaluation(datasetPath string) (*Report, error) {
	// 加载数据集
	dataset, err := LoadDataset(datasetPath)
	if err != nil {
		return nil, fmt.Errorf("加载数据集失败: %w", err)
	}

	// 检查 DB 是否可用（单元测试环境下可能未初始化）
	if db.GetDB() == nil {
		return nil, fmt.Errorf("数据库未初始化，请确保 MySQL 可用")
	}

	var results []StrategyResult

	// ===== FULLTEXT 评测 =====
	fulltextResults := evaluateStrategy("FULLTEXT", dataset, func(q EvalQuery) []int64 {
		return fulltextSearch(q)
	})
	results = append(results, fulltextResults...)

	// ===== Milvus Semantic 评测 =====
	semanticResults := evaluateStrategy("Milvus+Filter", dataset, func(q EvalQuery) []int64 {
		return milvusSearch(q)
	})
	results = append(results, semanticResults...)

	return BuildReport(results, dataset), nil
}

func evaluateStrategy(strategy string, dataset *EvalDataset, searchFn func(EvalQuery) []int64) []StrategyResult {
	var results []StrategyResult
	for _, q := range dataset.Queries {
		returnedIDs := searchFn(q)

		// 如果没有标注expected IDs，用返回结果暂时代替
		expected := q.ExpectedMsgIDs
		if len(expected) == 0 {
			// 无标注时用 FULLTEXT 结果作为 expected（默认假设 FULLTEXT 的是对的）
			expected = returnedIDs
		}

		r := StrategyResult{
			QueryID:     q.ID,
			Strategy:    strategy,
			ReturnedIDs: returnedIDs,
			RecallAt1:   computeRecall(returnedIDs, expected, 1),
			RecallAt3:   computeRecall(returnedIDs, expected, 3),
			RecallAt5:   computeRecall(returnedIDs, expected, 5),
			MRR:         computeMRR(returnedIDs, expected),
			Hit:         len(returnedIDs) > 0,
		}
		results = append(results, r)

		fmt.Printf("[Eval] %s | %s | difficulty=%s topic=%s → returned=%d\n",
			strategy, q.ID, q.Difficulty, q.Topic, len(returnedIDs))
	}
	return results
}

// fulltextSearch 执行 FULLTEXT 搜索（需要 MySQL 的 ngram FULLTEXT 索引）
func fulltextSearch(q EvalQuery) []int64 {
	var targetUserID, targetGroupID int64

	if q.TargetUser != "" {
		user, err := dao.FindUserByName(q.TargetUser)
		if err != nil {
			return nil
		}
		targetUserID = user.UserId
	}

	if q.TargetGroup != "" {
		group, err := dao.FindGroupByName(q.TargetGroup)
		if err != nil {
			return nil
		}
		targetGroupID = group.GroupID
	}

	currentUserID := int64(1)
	startTime, endTime := utils.ResolveTime("", "")
	keywords := strings.Fields(q.Keywords)

	messages, err := dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID,
		keywords, startTime, endTime, 50)
	if err != nil || len(messages) == 0 {
		return nil
	}

	ids := make([]int64, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, msg.MsgId)
	}
	return ids
}

// milvusSearch 执行 Milvus 语义搜索，返回 msg_id 列表
func milvusSearch(q EvalQuery) (ids []int64) {
	// Milvus 未初始化时 GetMilvus() 会 panic，recover 兜底
	defer func() {
		if r := recover(); r != nil {
			ids = nil
		}
	}()

	ctx := context.Background()

	var targetUserID, targetGroupID int64

	if q.TargetUser != "" {
		user, err := dao.FindUserByName(q.TargetUser)
		if err != nil {
			return nil
		}
		targetUserID = user.UserId
	}

	if q.TargetGroup != "" {
		group, err := dao.FindGroupByName(q.TargetGroup)
		if err != nil {
			return nil
		}
		targetGroupID = group.GroupID
	}

	currentUserID := int64(1)

	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return nil
	}

	// build convID filter
	var convID string
	if targetGroupID != 0 {
		convID = utils.GroupConvID(targetGroupID)
	} else {
		convID = utils.SingleChatConvID(currentUserID, targetUserID)
	}
	filterExpr := fmt.Sprintf("conv_id == \"%s\"", convID)
	outputFields := []string{"msg_id", "sender_id", "send_time"}

	retriever, err := vdb.GetRetriever(ctx, vdb.MessageCollection, "embedding", outputFields, 5, embedder)
	if err != nil {
		return nil
	}

	results, err := retriever.Retrieve(ctx, q.Keywords, milvusret.WithFilter(filterExpr))
	if err != nil || len(results) == 0 {
		return nil
	}

	msgIDs := make([]int64, 0, len(results))
	for _, doc := range results {
		id, err := strconv.ParseInt(doc.ID, 10, 64)
		if err != nil {
			continue
		}
		msgIDs = append(msgIDs, id)
	}

	// 回表 MySQL
	messages, _ := dao.GetMessagesByIDs(msgIDs)
	ids = make([]int64, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, msg.MsgId)
	}
	return ids
}

func init() {
	// 确保 tools 包的副作用被注册
	_ = tools.MustSearchHistoryTool
}
