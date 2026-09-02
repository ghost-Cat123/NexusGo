package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"NexusGo/apps/agent/engine"
	"NexusGo/apps/pkg/config"
	vdb "NexusGo/apps/pkg/vector_db"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"

	"github.com/cloudwego/eino/schema"
)

// ── 种子记忆数据（50条：30核心 + 20干扰，模拟真实IM用户画像）──

type seedMemory struct {
	Content    string
	MemoryType string
	Tags       string
	Importance int8
}

// coreMemories 核心记忆（评测期望召回的目标，覆盖技术栈/决策/偏好/生活）
var coreMemories = []seedMemory{
	// ── 技术栈事实 (fact, importance 4-5) ──
	{Content: "用户偏好使用VS Code编辑器进行Go语言开发", MemoryType: "preference", Tags: "编辑器,VS Code,Go", Importance: 4},
	{Content: "用户的后端技术栈选择Go语言加MySQL数据库加Redis缓存", MemoryType: "fact", Tags: "Go,MySQL,Redis,技术栈", Importance: 5},
	{Content: "用户决定使用HikariCP作为数据库连接池组件", MemoryType: "decision", Tags: "HikariCP,连接池,数据库", Importance: 5},
	{Content: "用户选择RabbitMQ作为消息队列中间件用于削峰填谷", MemoryType: "decision", Tags: "RabbitMQ,消息队列,削峰填谷", Importance: 4},
	{Content: "用户的项目NexusGo是一个Go语言开发的即时通讯系统", MemoryType: "fact", Tags: "NexusGo,即时通讯,Go", Importance: 5},
	{Content: "用户使用了Redis缓存来加速消息路由查询", MemoryType: "fact", Tags: "Redis,缓存,消息路由", Importance: 3},
	{Content: "用户采用JWT进行用户认证，token有效期7天", MemoryType: "fact", Tags: "JWT,认证,token", Importance: 4},
	{Content: "用户使用WebSocket作为实时通信协议，基于gorilla/websocket库", MemoryType: "fact", Tags: "WebSocket,实时通信,gorilla", Importance: 4},
	// ── 架构决策 (decision, importance 4-5) ──
	{Content: "用户决定采用写扩散方案处理群聊消息，而非读扩散", MemoryType: "decision", Tags: "写扩散,群聊,消息分发", Importance: 5},
	{Content: "用户选择Milvus向量数据库做语义检索而非Elasticsearch", MemoryType: "decision", Tags: "Milvus,向量数据库,语义检索", Importance: 4},
	{Content: "用户选择使用Eino框架编排AI Agent的工具调用和流式推理", MemoryType: "decision", Tags: "Eino,AI Agent,工具调用", Importance: 4},
	{Content: "用户设计了Gateway-Logic-Agent三服务架构分离关注点", MemoryType: "decision", Tags: "微服务,架构,Gateway,Logic,Agent", Importance: 5},
	{Content: "用户决定在全链路使用RabbitMQ异步解耦而非同步RPC调用", MemoryType: "decision", Tags: "RabbitMQ,异步,解耦,RPC", Importance: 4},
	{Content: "用户选择自建GrowRPC框架而非使用gRPC，因为需要一致性哈希路由", MemoryType: "decision", Tags: "GrowRPC,自建,一致性哈希,gRPC", Importance: 4},
	{Content: "用户实现三层记忆体系：短期Redis滑窗、中期MySQL摘要、长期Milvus向量", MemoryType: "decision", Tags: "记忆体系,Redis,MySQL,Milvus,三层", Importance: 5},
	// ── 偏好与习惯 (preference, importance 2-3) ──
	{Content: "用户偏好使用Docker Compose一键部署开发环境", MemoryType: "preference", Tags: "Docker,Compose,部署", Importance: 3},
	{Content: "用户习惯每天早上9点开始工作并先检查消息队列积压情况", MemoryType: "preference", Tags: "9点,工作习惯,消息队列,积压", Importance: 2},
	{Content: "用户喜欢写单元测试，认为测试覆盖率达到80%才算合格", MemoryType: "preference", Tags: "单元测试,覆盖率,80%", Importance: 3},
	{Content: "用户偏好使用Git命令行操作，不喜欢GUI工具", MemoryType: "preference", Tags: "Git,命令行,GUI", Importance: 2},
	{Content: "用户认为代码review是必须的，每个PR至少需要一人approve", MemoryType: "preference", Tags: "code review,PR,approve", Importance: 3},
	{Content: "用户喜欢在技术文档中使用Markdown格式编写", MemoryType: "preference", Tags: "Markdown,文档,技术写作", Importance: 2},
	{Content: "用户偏好简洁的错误处理风格，不喜欢过度包装的异常体系", MemoryType: "preference", Tags: "错误处理,简洁,Go风格", Importance: 3},
	{Content: "用户习惯在开发前先画架构图梳理思路再写代码", MemoryType: "preference", Tags: "架构图,开发流程,设计先行", Importance: 2},
	// ── 生活事实 (fact, importance 1-2) ──
	{Content: "用户喜欢吃川菜，特别是麻辣火锅和水煮鱼", MemoryType: "preference", Tags: "川菜,火锅,水煮鱼,美食", Importance: 2},
	{Content: "用户上个月去上海出差了一周，住在静安区", MemoryType: "fact", Tags: "上海,出差,静安区", Importance: 1},
	{Content: "用户每天下午3点喝一杯咖啡提神", MemoryType: "preference", Tags: "咖啡,下午茶,习惯", Importance: 1},
	{Content: "用户服务器使用的是Ubuntu 20.04 LTS系统", MemoryType: "fact", Tags: "Ubuntu,服务器,系统", Importance: 2},
	{Content: "用户周末喜欢去健身房锻炼，每周至少去三次", MemoryType: "preference", Tags: "健身,周末,锻炼", Importance: 1},
	{Content: "用户在开发NexusGo项目时用的是家里的台式机，配置是32G内存", MemoryType: "fact", Tags: "台式机,32G,开发环境", Importance: 1},
	{Content: "用户养了一只猫叫咪咪，经常在debug时陪在身边", MemoryType: "fact", Tags: "猫,宠物,debug", Importance: 1},
}

// noiseMemories 干扰记忆（语义相近但低重要性，模拟真实场景中的噪音）
var noiseMemories = []seedMemory{
	// 编辑器/工具相关干扰
	{Content: "用户觉得用Python写脚本很方便，偶尔用VSCode写Python", MemoryType: "preference", Tags: "Python,VS Code,脚本", Importance: 1},
	{Content: "用户尝试过JetBrains的GoLand但最终还是回到VS Code", MemoryType: "preference", Tags: "GoLand,JetBrains,VS Code", Importance: 1},
	{Content: "用户用过Sublime Text和Atom，但都觉得不如VS Code好用", MemoryType: "preference", Tags: "Sublime,Atom,编辑器", Importance: 1},
	// 数据库/存储相关干扰
	{Content: "用户尝试过用MongoDB替代MySQL但发现不适合当前项目", MemoryType: "fact", Tags: "MongoDB,MySQL,数据库", Importance: 2},
	{Content: "用户调研过TiDB分布式数据库但因为运维复杂度放弃了", MemoryType: "fact", Tags: "TiDB,分布式数据库,运维", Importance: 2},
	{Content: "用户以前的项目用过PostgreSQL，觉得它的JSON支持很好", MemoryType: "fact", Tags: "PostgreSQL,JSON,数据库", Importance: 1},
	// 消息队列/中间件相关干扰
	{Content: "用户认为消息队列选型中Kafka也不错但太重了不适合小团队", MemoryType: "decision", Tags: "Kafka,消息队列,选型", Importance: 1},
	{Content: "用户研究过NSQ作为轻量级消息队列替代方案", MemoryType: "fact", Tags: "NSQ,消息队列,轻量级", Importance: 1},
	{Content: "用户觉得Redis的PubSub也可以做简单消息队列但可靠性不够", MemoryType: "fact", Tags: "Redis,PubSub,消息队列", Importance: 2},
	// 容器/部署相关干扰
	{Content: "用户学习过Kubernetes但项目中暂时用不上", MemoryType: "fact", Tags: "Kubernetes,容器,学习", Importance: 2},
	{Content: "用户觉得Nginx做反向代理配置比Traefik简单", MemoryType: "preference", Tags: "Nginx,反向代理,Traefik", Importance: 1},
	{Content: "用户试过用Helm管理Kubernetes应用但觉得太复杂", MemoryType: "fact", Tags: "Helm,Kubernetes,复杂", Importance: 1},
	// 项目/架构相关干扰
	{Content: "用户的朋友在做电商项目用的是Java Spring Boot", MemoryType: "fact", Tags: "Java,Spring,电商", Importance: 1},
	{Content: "用户之前实习时做过一个Python Django的博客项目", MemoryType: "fact", Tags: "Python,Django,博客,实习", Importance: 1},
	{Content: "用户考虑过用微服务网格但觉得对当前规模过度设计", MemoryType: "decision", Tags: "微服务,服务网格,过度设计", Importance: 2},
	// 生活/习惯相关干扰
	{Content: "用户周末喜欢在家煮火锅招待朋友", MemoryType: "preference", Tags: "火锅,周末,朋友", Importance: 1},
	{Content: "用户上次去北京出差时参观了故宫和长城", MemoryType: "fact", Tags: "北京,出差,故宫,长城", Importance: 1},
	{Content: "用户觉得瑞幸咖啡比星巴克性价比高很多", MemoryType: "preference", Tags: "咖啡,瑞幸,星巴克", Importance: 1},
	{Content: "用户偶尔会去游泳作为健身的替代项目", MemoryType: "preference", Tags: "游泳,健身,替代", Importance: 1},
	{Content: "用户家里的猫最近生了三只小猫，在找领养", MemoryType: "fact", Tags: "猫,小猫,领养", Importance: 1},
}

// ── 评测数据结构 ──

type MemoryEvalQuery struct {
	ID               string   `json:"id"`
	Query            string   `json:"query"`
	ExpectedKeywords []string `json:"expected_keywords"`
	Category         string   `json:"category"`
	Difficulty       string   `json:"difficulty"`
	Description      string   `json:"description"`
}

type MemoryEvalResult struct {
	QueryID       string
	Query         string
	Category      string
	Difficulty    string
	RecalledTexts []string
	HitKeywords   int
	TotalKeywords int
	FirstHitRank  int
	RecallAt1     float64
	RecallAt3     float64
	RecallAt5     float64
	MRR           float64
}

type MemoryEvalDataset struct {
	Queries []MemoryEvalQuery `json:"queries"`
}

// ── 数据集加载 ──

func loadMemoryDataset(path string) (*MemoryEvalDataset, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取数据集失败: %w", err)
	}
	var queries []MemoryEvalQuery
	if err := json.Unmarshal(data, &queries); err != nil {
		return nil, fmt.Errorf("解析 JSON 失败: %w", err)
	}
	return &MemoryEvalDataset{Queries: queries}, nil
}

// ── 种子数据写入 ──

func seedMilvusMemory(ctx context.Context, userID int64, sessionID string, mems []seedMemory) error {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return fmt.Errorf("Embedder 获取失败: %w", err)
	}
	indexer, err := vdb.GetIndxer(ctx, vdb.MemoryCollection, embedder, vdb.MemoryDocumentConverter)
	if err != nil {
		return fmt.Errorf("Indexer 获取失败: %w", err)
	}

	// 分批写入，DashScope embedding API 单次最多 10 条
	const batchSize = 10
	total := 0
	for start := 0; start < len(mems); start += batchSize {
		end := start + batchSize
		if end > len(mems) {
			end = len(mems)
		}
		batch := mems[start:end]
		ts := time.Now().UnixNano()
		docs := make([]*schema.Document, 0, len(batch))
		for i, m := range batch {
			docs = append(docs, &schema.Document{
				ID:      fmt.Sprintf("eval-mem-%d-%d", ts, i),
				Content: m.Content,
				MetaData: map[string]any{
					"user_id":     userID,
					"session_id":  sessionID,
					"memory_type": m.MemoryType,
					"importance":  int64(m.Importance),
					"create_time": time.Now().Unix(),
				},
			})
		}
		if _, err := indexer.Store(ctx, docs); err != nil {
			return fmt.Errorf("批次 [%d:%d] 写入失败: %w", start, end, err)
		}
		total += len(batch)
	}
	fmt.Printf("[Seed] %d 条记忆写入完成\n", total)
	return nil
}

// ── 纯语义召回（模拟旧版，用于对比）──

func semanticOnlyRecall(ctx context.Context, userID int64, query string, topK int) ([]string, error) {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return nil, err
	}
	retriever, err := vdb.GetRetriever(ctx, vdb.MemoryCollection, "embedding",
		[]string{"memory_id", "summary", "importance"}, topK*2, embedder)
	if err != nil {
		return nil, err
	}
	filterExpr := fmt.Sprintf("user_id == %d", userID)
	results, err := retriever.Retrieve(ctx, query, milvusret.WithFilter(filterExpr))
	if err != nil {
		return nil, err
	}
	var summaries []string
	for _, doc := range results {
		if doc.Score() < 0.5 {
			continue
		}
		content := doc.Content
		if content == "" {
			if v, ok := doc.MetaData["summary"].(string); ok {
				content = v
			}
		}
		if content != "" {
			summaries = append(summaries, content)
		}
	}
	if len(summaries) > topK {
		summaries = summaries[:topK]
	}
	return summaries, nil
}

// ── 关键词命中检查 ──

func checkRecallKeywords(q MemoryEvalQuery, recalled []string) MemoryEvalResult {
	result := MemoryEvalResult{
		QueryID: q.ID, Query: q.Query, Category: q.Category,
		Difficulty: q.Difficulty, TotalKeywords: len(q.ExpectedKeywords),
		FirstHitRank: -1, RecalledTexts: recalled,
	}
	if len(recalled) == 0 {
		return result
	}
	keywordSet := make(map[string]bool, len(q.ExpectedKeywords))
	for _, kw := range q.ExpectedKeywords {
		keywordSet[strings.ToLower(kw)] = true
	}
	hitKeywords := make(map[string]bool)
	for rank, text := range recalled {
		textLower := strings.ToLower(text)
		for kw := range keywordSet {
			if hitKeywords[kw] {
				continue
			}
			if strings.Contains(textLower, strings.ToLower(kw)) {
				hitKeywords[kw] = true
				if result.FirstHitRank < 0 {
					result.FirstHitRank = rank + 1
				}
			}
		}
	}
	result.HitKeywords = len(hitKeywords)
	if result.FirstHitRank > 0 {
		result.MRR = 1.0 / float64(result.FirstHitRank)
		if result.FirstHitRank <= 1 {
			result.RecallAt1 = 1
		}
		if result.FirstHitRank <= 3 {
			result.RecallAt3 = 1
		}
		if result.FirstHitRank <= 5 {
			result.RecallAt5 = 1
		}
	}
	return result
}

// ── 统计函数 ──

type evalSummary struct {
	Name   string
	Count  int
	AvgR5  float64
	AvgMRR float64
	HitRate float64
}

func aggregateResults(results []MemoryEvalResult) evalSummary {
	if len(results) == 0 {
		return evalSummary{}
	}
	s := evalSummary{Count: len(results)}
	var r5, mrr, hit float64
	for _, r := range results {
		r5 += r.RecallAt5
		mrr += r.MRR
		if r.HitKeywords > 0 {
			hit++
		}
	}
	s.AvgR5 = r5 / float64(len(results))
	s.AvgMRR = mrr / float64(len(results))
	s.HitRate = hit / float64(len(results))
	return s
}

func groupBy(results []MemoryEvalResult, keyFn func(MemoryEvalResult) string) map[string][]MemoryEvalResult {
	groups := make(map[string][]MemoryEvalResult)
	for _, r := range results {
		k := keyFn(r)
		groups[k] = append(groups[k], r)
	}
	return groups
}

// ── 对比报告生成 ──

func formatComparisonReport(weighted, semantic []MemoryEvalResult) string {
	sw := aggregateResults(weighted)
	ss := aggregateResults(semantic)

	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════════════════════════════════════╗\n")
	sb.WriteString(fmt.Sprintf("║  记忆召回双策略对比评测 (50条种子: %d核心+%d干扰, %d条查询)              ║\n",
		len(coreMemories), len(noiseMemories), len(weighted)))
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║  %-20s │ R@5=%-5.2f │ MRR=%-5.2f │ HitRate=%-5.2f            ║\n",
		"重要性加权 (新)", sw.AvgR5, sw.AvgMRR, sw.HitRate))
	sb.WriteString(fmt.Sprintf("║  %-20s │ R@5=%-5.2f │ MRR=%-5.2f │ HitRate=%-5.2f            ║\n",
		"纯语义 (旧)", ss.AvgR5, ss.AvgMRR, ss.HitRate))

	// 按难度分组对比
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║  按难度分组对比                                                        ║\n")
	for _, diff := range []string{"easy", "medium", "hard"} {
		wg := groupBy(weighted, func(r MemoryEvalResult) string { return r.Difficulty })
		sg := groupBy(semantic, func(r MemoryEvalResult) string { return r.Difficulty })
		wd := aggregateResults(wg[diff])
		sd := aggregateResults(sg[diff])
		delta := wd.AvgMRR - sd.AvgMRR
		arrow := "→"
		if delta > 0.005 {
			arrow = "↑"
		} else if delta < -0.005 {
			arrow = "↓"
		}
		sb.WriteString(fmt.Sprintf("║  %-6s N=%-2d │ 加权 MRR=%-5.2f │ 语义 MRR=%-5.2f │ Δ=%+.2f %s              ║\n",
			diff, wd.Count, wd.AvgMRR, sd.AvgMRR, delta, arrow))
	}

	// 按类别分组对比
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║  按类别分组对比                                                        ║\n")
	for _, cat := range []string{"fact", "preference", "decision"} {
		wg := groupBy(weighted, func(r MemoryEvalResult) string { return r.Category })
		sg := groupBy(semantic, func(r MemoryEvalResult) string { return r.Category })
		wd := aggregateResults(wg[cat])
		sd := aggregateResults(sg[cat])
		delta := wd.AvgMRR - sd.AvgMRR
		arrow := "→"
		if delta > 0.005 {
			arrow = "↑"
		} else if delta < -0.005 {
			arrow = "↓"
		}
		sb.WriteString(fmt.Sprintf("║  %-12s N=%-2d │ 加权 MRR=%-5.2f │ 语义 MRR=%-5.2f │ Δ=%+.2f %s              ║\n",
			cat, wd.Count, wd.AvgMRR, sd.AvgMRR, delta, arrow))
	}

	// 提升/下降明细
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════╣\n")
	improved, declined, same := 0, 0, 0
	for i := range weighted {
		if i >= len(semantic) {
			break
		}
		if weighted[i].FirstHitRank > 0 && semantic[i].FirstHitRank > 0 {
			if weighted[i].FirstHitRank < semantic[i].FirstHitRank {
				improved++
			} else if weighted[i].FirstHitRank > semantic[i].FirstHitRank {
				declined++
			} else {
				same++
			}
		}
	}
	sb.WriteString(fmt.Sprintf("║  排名变化: ↑%d条提升  ↓%d条下降  =%d条持平 (共%d条有效对比)              ║\n",
		improved, declined, same, improved+declined+same))

	// 结论
	sb.WriteString("╠══════════════════════════════════════════════════════════════════════╣\n")
	deltaMRR := sw.AvgMRR - ss.AvgMRR
	if deltaMRR > 0.01 {
		sb.WriteString(fmt.Sprintf("║  ✅ 重要性加权 MRR 提升 +%.2f                                        ║\n", deltaMRR))
	} else if deltaMRR < -0.01 {
		sb.WriteString(fmt.Sprintf("║  ⚡ 重要性加权 MRR 略降 %.2f（低重要性查询语义更匹配时权重无法补偿）     ║\n", deltaMRR))
	} else {
		sb.WriteString("║  ➡ 两种策略整体 MRR 持平                                               ║\n")
	}
	if sw.HitRate > ss.HitRate {
		sb.WriteString(fmt.Sprintf("║  ✅ HitRate 提升 +%.0f%%                                                ║\n", (sw.HitRate-ss.HitRate)*100))
	}
	sb.WriteString("╚══════════════════════════════════════════════════════════════════════╝\n")

	// 降级详情（仅显示排名变化的）
	sb.WriteString("\n排名变化详情 (仅显示加权≠语义的查询):\n")
	sb.WriteString(fmt.Sprintf("  %-6s %-8s %-10s %-10s %s\n", "ID", "难度", "加权rank", "语义rank", "查询"))
	shown := 0
	for i := range weighted {
		if i >= len(semantic) {
			break
		}
		w, s := weighted[i], semantic[i]
		if w.FirstHitRank == s.FirstHitRank && w.FirstHitRank == 1 {
			continue // Rank-1 一致，跳过
		}
		if shown >= 25 {
			sb.WriteString(fmt.Sprintf("  ... (还有 %d 条变化，已省略)\n", improved+declined-shown))
			break
		}
		sb.WriteString(fmt.Sprintf("  %-6s %-8s w=%-2d     s=%-2d     %s\n",
			w.QueryID, w.Difficulty, w.FirstHitRank, s.FirstHitRank, truncateRunes(w.Query, 40)))
		shown++
	}

	return sb.String()
}

func truncateRunes(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// ── 测试入口 ──

func TestMemoryBaseline(t *testing.T) {
	ctx := context.Background()
	const testUserID = int64(88888)
	const testSessionID = "agent_session:eval_v2"

	if config.GlobalConfig == nil {
		t.Skip("配置未初始化")
	}
	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Milvus 未初始化: %v", r)
		}
	}()
	_ = vdb.GetMilvus()

	// Step 1: 写入种子
	t.Logf("Step 1: 写入种子 (%d核心 + %d干扰)", len(coreMemories), len(noiseMemories))
	if err := seedMilvusMemory(ctx, testUserID, testSessionID, coreMemories); err != nil {
		t.Fatalf("核心记忆写入失败: %v", err)
	}
	if err := seedMilvusMemory(ctx, testUserID, testSessionID, noiseMemories); err != nil {
		t.Fatalf("干扰记忆写入失败: %v", err)
	}
	t.Log("等待 Milvus 索引 (3s)...")
	time.Sleep(3 * time.Second)

	// Step 2: 加载数据集
	wd, _ := os.Getwd()
	datasetPath := filepath.Join(wd, "testdata", "memory_queries.json")
	if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
		datasetPath = filepath.Join(wd, "..", "..", "..", "apps", "agent", "eval", "testdata", "memory_queries.json")
	}
	dataset, err := loadMemoryDataset(datasetPath)
	if err != nil {
		t.Fatalf("加载数据集失败: %v", err)
	}
	t.Logf("Step 2: 加载 %d 条查询", len(dataset.Queries))

	// Step 3: 双策略评测
	t.Logf("Step 3: 双策略评测 (TopK=5)")

	var weightedResults, semanticResults []MemoryEvalResult
	for _, q := range dataset.Queries {
		// 策略A: 重要性加权
		recalledW, err := engine.RecallMemory(ctx, testUserID, q.Query, 5)
		if err != nil {
			t.Logf("[W] %s err: %v", q.ID, err)
			continue
		}
		rw := checkRecallKeywords(q, recalledW)
		rw.QueryID = q.ID
		weightedResults = append(weightedResults, rw)

		// 策略B: 纯语义
		recalledS, err := semanticOnlyRecall(ctx, testUserID, q.Query, 5)
		if err != nil {
			t.Logf("[S] %s err: %v", q.ID, err)
			continue
		}
		rs := checkRecallKeywords(q, recalledS)
		rs.QueryID = q.ID
		semanticResults = append(semanticResults, rs)
	}

	// Step 4: 报告
	t.Log("Step 4: 生成对比报告")
	fmt.Print(formatComparisonReport(weightedResults, semanticResults))

	sw := aggregateResults(weightedResults)
	ss := aggregateResults(semanticResults)
	t.Logf("最终: 加权 HitRate=%.2f MRR=%.2f | 语义 HitRate=%.2f MRR=%.2f",
		sw.HitRate, sw.AvgMRR, ss.HitRate, ss.AvgMRR)

	if len(weightedResults) == 0 {
		t.Error("评测结果为空")
	}
	_ = schema.Document{}
}
