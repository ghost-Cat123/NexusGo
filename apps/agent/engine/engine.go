package engine

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/memory"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/logger"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"fmt"
	"math"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
	"sort"
	"strconv"
	"strings"
	"time"
)

const minScore float64 = 0.5
const TopLongHistory = 3

// PrepareAgentContext 组装 Agent 执行上下文：
// 短期记忆（Redis 滑窗）+ 中期摘要（MySQL）→ + 长期记忆（Milvus） 合并为完整历史消息列表
func PrepareAgentContext(ctx context.Context, senderID int64, message string) (*adk.Runner, *memory.Session, []*schema.Message, error) {
	sessionID := "agent_session:" + strconv.FormatInt(senderID, 10)
	logger.Log.Infof("[AI] 开始处理用户[%d]问题: %s", senderID, strings.TrimSpace(message))

	// 1. 初始化 Runner
	runner, err := GetAgentGraphRunner(ctx)
	if err != nil {
		logger.Log.Errorf("[AI] 初始化 Runner 失败: %v", err)
		return nil, nil, nil, err
	}

	// 2. 获取 Session（传入 userID 供摘要压缩时使用）
	store := memory.NewRedisStore(time.Hour * 24)
	session := store.GetOrCreate(sessionID, senderID)
	// 3. 追加用户当前消息到 Redis 短期记忆
	userMsg := schema.UserMessage(strings.TrimSpace(message))
	if err := session.Append(ctx, userMsg); err != nil {
		return nil, nil, nil, fmt.Errorf("写入 Session 失败: %w", err)
	}

	// 4. 读取短期记忆（含刚写入的用户消息）
	shortTermHistory, err := session.GetMessages(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("读取 Session 失败: %w", err)
	}

	// 5. 读取中期摘要（MySQL 最近一条）
	recentSummary, err := dao.GetLatestSummary(senderID, sessionID)
	if err != nil {
		// 摘要读取失败不应中断正常对话，降级处理
		logger.Log.Warnf("[AI] 读取中期摘要失败（已降级）: %v", err)
		recentSummary = ""
	}

	longTermHistory, err := RecallMemory(ctx, senderID, message, TopLongHistory)
	if err != nil {
		logger.Log.Warnf("[AI] 长期记忆召回失败（已降级）: %v", err)
	}

	// 6. 组装最终历史消息列表：
	//    [中期摘要（若有）] + [短期滑窗对话]
	//    注意：摘要以 SystemMessage 形式插入到历史头部，让 LLM 感知上下文
	var fullHistory []*schema.Message
	// 长期记忆注入
	if longTermHistory != nil && len(longTermHistory) != 0 {
		var longMem string
		for _, mem := range longTermHistory {
			longMem = longMem + mem
		}
		fullHistory = append(fullHistory, schema.SystemMessage(
			"[以下是关于此用户的历史记忆，可能与此对话相关]\n"+longMem,
		))
	}
	// 中期摘要注入
	if recentSummary != "" {
		summaryMsg := schema.SystemMessage(
			fmt.Sprintf("[历史对话摘要，请参考但不必完全重复]\n%s", recentSummary),
		)
		fullHistory = append(fullHistory, summaryMsg)
		logger.Log.Debugf("[AI] 注入中期摘要，长度=%d 字", len(recentSummary))
	}
	// 短期记忆注入
	fullHistory = append(fullHistory, shortTermHistory...)

	return runner, session, fullHistory, nil
}

// recallItem Milvus 召回候选项
type recallItem struct {
	content    string
	score      float64 // Milvus 语义相似度 (0~1)
	importance int64   // 重要性 (0~5)
}

// RecallMemory 语义检索 + 重要性加权混合排序
// 步骤：Milvus 语义检索 → 混合评分 → 重要性优先过滤 → 去重 → 排序 → 截断
func RecallMemory(ctx context.Context, userID int64, query string, topK int) ([]string, error) {
	// 1. 获取Embedder
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		logger.Log.Warnf("[AI] Embedder获取失败: %v", err)
		return nil, err
	}

	// 2. 获取retriever（多取候选，输出字段加 importance 用于混合评分）
	retriever, err := vdb.GetRetriever(ctx, vdb.MemoryCollection, "embedding",
		[]string{"memory_id", "summary", "importance"}, topK*2, embedder)
	if err != nil {
		logger.Log.Warnf("[AI] Retriever获取失败: %v", err)
		return nil, err
	}

	// 3. 标量过滤 user_id
	filterExpr := fmt.Sprintf("user_id == %d", userID)

	// 4. 语义检索
	results, err := retriever.Retrieve(ctx, query, milvusret.WithFilter(filterExpr))
	if err != nil {
		logger.Log.Warnf("[AI] Retriever检索失败: %v", err)
		return nil, err
	}

	// 5. 构建候选列表
	var candidates []recallItem
	for _, doc := range results {
		score := doc.Score()
		if score < minScore {
			continue
		}

		content := doc.Content
		if content == "" {
			if v, ok := doc.MetaData["summary"].(string); ok {
				content = v
			}
		}
		if content == "" {
			continue
		}

		var importance int64
		if v, ok := doc.MetaData["importance"]; ok {
			importance = v.(int64)
		}

		candidates = append(candidates, recallItem{
			content:    content,
			score:      score,
			importance: importance,
		})
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	// 6. 去重
	deduped := deduplicateByContent(candidates)

	// 7. 排序策略：语义优先，重要性打破平局
	//    两条记忆语义分差距 > 0.05 → 纯语义排序（保留精确匹配能力）
	//    两条记忆语义分差距 ≤ 0.05 → 重要性高的优先（在"差不多相关"时选更重要的）
	const tieThreshold = 0.05
	sort.Slice(deduped, func(i, j int) bool {
		si, sj := deduped[i].score, deduped[j].score
		if math.Abs(si-sj) > tieThreshold {
			return si > sj // 语义差距明显 → 语义优先
		}
		// 语义接近 → 重要性打破平局
		if deduped[i].importance != deduped[j].importance {
			return deduped[i].importance > deduped[j].importance
		}
		return si > sj // 重要性相同 → 语义兜底
	})

	// 8. 截断 topK
	if len(deduped) > topK {
		deduped = deduped[:topK]
	}

	var summaries []string
	for _, item := range deduped {
		summaries = append(summaries, item.content)
	}

	logger.Log.Debugf("[AI] 长期记忆召回: query=%q candidates=%d final=%d",
		truncateStr(query, 40), len(candidates), len(summaries))
	return summaries, nil
}

// deduplicateByContent 精确内容去重
func deduplicateByContent(items []recallItem) []recallItem {
	seen := make(map[string]bool, len(items))
	var result []recallItem
	for _, item := range items {
		if !seen[item.content] {
			seen[item.content] = true
			result = append(result, item)
		}
	}
	return result
}

func truncateStr(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}
