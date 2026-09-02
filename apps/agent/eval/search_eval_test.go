package eval

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	vdb "NexusGo/apps/pkg/vector_db"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"NexusGo/apps/pkg/db"
	"NexusGo/apps/pkg/utils"
)

// ── 消息检索策略对比评测 ──
// 对比旧版串行降级 vs 新版并行融合的召回覆盖度
//
// 前提：数据库中有聊天消息（通过正常使用 IM 系统产生）
// 指标：并行融合比串行降级多召回多少条消息（FULLTEXT + Milvus 互补增量）

type searchCompareRow struct {
	QueryID       string
	SerialCount   int // 串行结果数
	ParallelCount int // 并行合并后结果数
	FTCount       int // FULLTEXT 路
	MVCount       int // Milvus 路
	Exclusive     int // 并行独占（串行没找到的）
}

func TestSearchRetrievalCompare(t *testing.T) {
	if db.GetDB() == nil {
		t.Skip("DB 未初始化")
	}
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Milvus 未初始化: %v", r)
		}
	}()
	_ = vdb.GetMilvus()

	wd, _ := os.Getwd()
	datasetPath := filepath.Join(wd, "testdata", "queries.json")
	if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
		datasetPath = filepath.Join(wd, "..", "..", "..", "apps", "agent", "eval", "testdata", "queries.json")
	}
	dataset, err := LoadDataset(datasetPath)
	if err != nil {
		t.Fatalf("加载数据集失败: %v", err)
	}
	t.Logf("数据集: %d 条查询", len(dataset.Queries))

	var rows []searchCompareRow
	totalSerial, totalParallel, totalExclusive := 0, 0, 0
	queriesImproved := 0

	for _, q := range dataset.Queries {
		serialIDs := serialSearch(q)
		ftIDs, mvIDs, mergedIDs := parallelSearch(q)

		serialSet := toSet(serialIDs)
		exclusive := 0
		for _, id := range mergedIDs {
			if !serialSet[id] {
				exclusive++
			}
		}

		totalSerial += len(serialIDs)
		totalParallel += len(mergedIDs)
		totalExclusive += exclusive
		if exclusive > 0 {
			queriesImproved++
		}

		rows = append(rows, searchCompareRow{
			QueryID:       q.ID,
			SerialCount:   len(serialIDs),
			ParallelCount: len(mergedIDs),
			FTCount:       len(ftIDs),
			MVCount:       len(mvIDs),
			Exclusive:     exclusive,
		})

		if exclusive > 0 || len(mergedIDs) != len(serialIDs) {
			t.Logf("[%s] 串行=%d 并行=%d(FT=%d+MV=%d) 增量=%d",
				q.ID, len(serialIDs), len(mergedIDs), len(ftIDs), len(mvIDs), exclusive)
		}
	}

	// 报告
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║     消息检索策略对比：串行降级 vs 并行融合                  ║\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║  查询数: %-3d                                              ║\n", len(rows)))
	sb.WriteString(fmt.Sprintf("║  串行总召回: %-4d 条 (FULLTEXT优先,空则Milvus降级)          ║\n", totalSerial))
	sb.WriteString(fmt.Sprintf("║  并行总召回: %-4d 条 (FT+MV 并行,合并去重)                  ║\n", totalParallel))
	sb.WriteString(fmt.Sprintf("║  并行独占增量: %-4d 条                                     ║\n", totalExclusive))
	sb.WriteString(fmt.Sprintf("║  提升查询数: %-3d / %-3d                                   ║\n", queriesImproved, len(rows)))

	if totalSerial == 0 && totalParallel == 0 {
		sb.WriteString("╠══════════════════════════════════════════════════════════╣\n")
		sb.WriteString("║  ⚡ 数据库为空，无法对比。发送消息后重新运行即可看到效果。    ║\n")
	} else if totalParallel > totalSerial {
		pct := float64(totalParallel-totalSerial) / float64(max(1, totalSerial)) * 100
		sb.WriteString(fmt.Sprintf("║  ✅ 并行融合提升: +%.0f%%                                     ║\n", pct))
	}
	sb.WriteString("╚══════════════════════════════════════════════════════════╝\n")
	fmt.Print(sb.String())

	t.Logf("最终: 串行=%d 并行=%d 增量=%d 提升查询=%d/%d",
		totalSerial, totalParallel, totalExclusive, queriesImproved, len(rows))
}

// ── 两种策略 ──

func serialSearch(q EvalQuery) []int64 {
	currentUserID := int64(1)
	targetUserID, targetGroupID := resolveTargets(q)
	startTime, endTime := utils.ResolveTime("", "")
	keywords := strings.Fields(q.Keywords)
	msgs, _ := dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID, keywords, startTime, endTime, 50)
	if len(msgs) > 0 {
		return msgIDs(msgs)
	}
	return milvusSearch(q)
}

func parallelSearch(q EvalQuery) (ftIDs, mvIDs, merged []int64) {
	currentUserID := int64(1)
	targetUserID, targetGroupID := resolveTargets(q)
	startTime, endTime := utils.ResolveTime("", "")
	keywords := strings.Fields(q.Keywords)

	type fetch struct{ ids []int64 }
	ftCh := make(chan fetch, 1)
	mvCh := make(chan fetch, 1)

	go func() {
		msgs, _ := dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID, keywords, startTime, endTime, 50)
		ftCh <- fetch{msgIDs(msgs)}
	}()
	go func() {
		mvCh <- fetch{milvusSearch(q)}
	}()

	ftRes := <-ftCh
	mvRes := <-mvCh

	seen := make(map[int64]bool)
	for _, id := range ftRes.ids {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	for _, id := range mvRes.ids {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	return ftRes.ids, mvRes.ids, merged
}

func resolveTargets(q EvalQuery) (int64, int64) {
	var targetUserID, targetGroupID int64
	if q.TargetUser != "" {
		user, err := dao.FindUserByName(q.TargetUser)
		if err == nil {
			targetUserID = user.UserId
		}
	}
	if q.TargetGroup != "" {
		group, err := dao.FindGroupByName(q.TargetGroup)
		if err == nil {
			targetGroupID = group.GroupID
		}
	}
	return targetUserID, targetGroupID
}

func msgIDs(msgs []models.Messages) []int64 {
	ids := make([]int64, len(msgs))
	for i, m := range msgs {
		ids[i] = m.MsgId
	}
	return ids
}
