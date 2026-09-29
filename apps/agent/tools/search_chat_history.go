package tools

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/graph"
	"NexusGo/apps/agent/models"
	"NexusGo/apps/agent/retrieval"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/utils"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"fmt"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"strconv"
	"strings"
	"time"
)

// 借助LLM 通过语义检索和清洗离散的用户聊天信息

// SearHistoryReq  定义入参
type SearHistoryReq struct {
	TargetUser  string `json:"target_user" jsonschema:"description=单聊对象用户名, 与target_group至少填一个"`
	TargetGroup string `json:"target_group" jsonschema:"description=群名称, 与target_user至少填一个"`
	StartTime   string `json:"start_time" jsonschema:"description=搜索的起始时间, RFC3339格式"`
	EndTime     string `json:"end_time" jsonschema:"description=搜索的结束时间, RFC3339格式"`
	Keywords    string `json:"keywords" jsonschema:"description=关键词,用空格分隔,例如'数据库 密码'"`
	Limit       int    `json:"limit" jsonschema:"description=最多返回多少条消息,默认50,maximum=200"`
}

// SearHistoryResp 定义出参
type SearHistoryResp struct {
	Result string `json:"result"`
}

// 业务逻辑
func searchHistoryInvoker(ctx context.Context, req *SearHistoryReq) (*SearHistoryResp, error) {
	currentUserID, ok := ctx.Value("current_user_id").(int64)
	if !ok {
		return nil, fmt.Errorf("internal error: missing user context")
	}

	if req.TargetUser == "" && req.TargetGroup == "" {
		return nil, fmt.Errorf("请指定搜索对象：用户名或群名称")
	}
	if req.Keywords == "" {
		return nil, fmt.Errorf("请提供搜索关键词，多个词用空格分隔")
	}

	// 任意一个没有都是默认值0对应单聊和群聊
	var targetUserID, targetGroupID int64
	var targetUserName string

	if req.TargetUser != "" {
		user, err := dao.FindUserByName(req.TargetUser)
		if err != nil {
			return nil, fmt.Errorf("未找到用户 '%s'，请确认用户名", req.TargetUser)
		}
		targetUserID = user.UserId
		targetUserName = user.Username
	}

	if req.TargetGroup != "" {
		group, err := dao.FindGroupByName(req.TargetGroup)
		if err != nil {
			return nil, fmt.Errorf("未找到群 '%s'，请确认群名称", req.TargetGroup)
		}
		targetGroupID = group.GroupID
	}

	queryStart, queryEnd := utils.ResolveTime(req.StartTime, req.EndTime)
	keywords := strings.Fields(req.Keywords)

	// 群聊兜底，避免恶意传group_id
	if targetGroupID != 0 {
		exists, _ := dao.IsGroupMember(targetGroupID, currentUserID)
		if !exists {
			return nil, fmt.Errorf("你不在该群中")
		}
	}

	// Agent receives a natural-language question, so semantic retrieval is the
	// primary path. FULLTEXT remains a cheap fallback for exact terms when the
	// vector path is unavailable or has no candidate.
	candidateLimit := req.Limit
	if candidateLimit < 20 {
		candidateLimit = 20
	}
	if candidateLimit > 200 {
		candidateLimit = 200
	}
	messages := semanticFallback(ctx, currentUserID, req.Keywords, targetUserID, targetGroupID, queryStart, queryEnd, candidateLimit)
	source := "semantic"
	if len(messages) == 0 {
		var err error
		messages, err = dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID, keywords, queryStart, queryEnd, candidateLimit)
		if err != nil {
			fmt.Printf("[Search] FULLTEXT 查询异常: %v\n", err)
		}
		source = "fulltext_fallback"
	}

	candidates := make([]graph.Document, 0, len(messages))
	for _, msg := range messages {
		senderName := resolveSenderName(msg.SenderId, currentUserID, targetUserID, targetUserName)
		candidates = append(candidates, graph.Document{
			Content: fmt.Sprintf("[%s]: %s", senderName, msg.Content),
			Source:  source,
		})
	}

	if len(candidates) == 0 {
		return &SearHistoryResp{
			Result: "未找到匹配的聊天记录，建议调整关键词或扩大时间范围",
		}, nil
	}

	// 组装返回
	var sb strings.Builder
	sb.WriteString("以下是相关聊天记录，请根据用户的需求进行总结或提取:\n")
	for i, doc := range candidates {
		sb.WriteString(doc.Content)
		sb.WriteString("\n")
		if i >= req.Limit && req.Limit > 0 {
			break
		}
	}

	return &SearHistoryResp{Result: sb.String()}, nil
}

// semanticFallback 语义召回：Embedding → Milvus → 回表 MySQL。
func semanticFallback(ctx context.Context, currentUserID int64, keywords string, targetUserID, targetGroupID int64, queryStart, queryEnd time.Time, limit int) []models.Messages {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		fmt.Printf("[Search] Embedder 初始化失败: %v\n", err)
		return nil
	}

	// 2. 构建标量过滤表达式（聊天权限核心） 发送者+时间范围过滤
	var convID string
	if targetGroupID != 0 {
		convID = utils.GroupConvID(targetGroupID)
	} else {
		convID = utils.SingleChatConvID(currentUserID, targetUserID)
	}
	filterExpr := fmt.Sprintf("conv_id == \"%s\"", convID)

	// 追加时间范围过滤
	if queryStart.Unix() > 0 {
		filterExpr += fmt.Sprintf(" && send_time >= %d", queryStart.Unix())
	}
	if queryEnd.Unix() > 0 {
		filterExpr += fmt.Sprintf(" && send_time <= %d", queryEnd.Unix())
	}

	// 使用 milvus2.WithFilter 生成实现特定选项
	// WithFilter：绑定标量过滤条件
	filterOption := milvusret.WithFilter(filterExpr)

	// 输出字段
	outputFields := []string{"msg_id", "sender_id", "send_time"}
	retriever, err := vdb.GetRetriever(ctx, vdb.MessageCollection, "embedding", outputFields, limit, embedder)

	if err != nil {
		fmt.Printf("[Search] Retriever 初始化失败: %v\n", err)
		return nil
	}

	// 将标量过滤选项放入Retrieve的选项中
	results, err := retriever.Retrieve(ctx, keywords, filterOption)
	if err != nil {
		fmt.Printf("[Search] 语义检索无结果: err=%v count=%d\n", err, len(results))
		return nil
	}
	results = retrieval.FilterDocumentsByScore(results, retrieval.MinMessageSemanticScore)
	if len(results) == 0 {
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
	if len(msgIDs) == 0 {
		return nil
	}

	// 回表查询 + 权限校验（只返回当前用户相关的消息）
	messages, err := dao.GetMessagesByIDs(msgIDs)
	if err != nil {
		fmt.Printf("[Search] 回表查询失败: %v\n", err)
		return nil
	}
	ordered := retrieval.OrderMessagesByIDs(messages, msgIDs)
	if !retrieval.HasContentKeywordCoverage(searchHistoryMessageContents(ordered), strings.Fields(keywords)) {
		return nil
	}
	return ordered
}

func searchHistoryMessageContents(messages []models.Messages) []string {
	contents := make([]string, 0, len(messages))
	for _, message := range messages {
		contents = append(contents, message.Content)
	}
	return contents
}

func resolveSenderName(senderId, currentUserID, targetUserID int64, targetUserName string) string {
	if senderId == currentUserID {
		return "你"
	}
	if targetUserID != 0 && senderId == targetUserID {
		return targetUserName
	}
	return fmt.Sprintf("用户%d", senderId)
}

func SearchHistoryTool() (tool.InvokableTool, error) {
	return toolutils.InferTool(
		"search_chat_history", // Tool 名称
		"查询与特定用户的历史聊天记录，支持时间范围和关键词过滤", // Tool 描述
		searchHistoryInvoker, // 执行函数
	)
}

func MustSearchHistoryTool() tool.InvokableTool {
	t, err := SearchHistoryTool()
	if err != nil {
		// 这里的 panic 是合理的，因为工具创建失败意味着 Agent 根本无法工作
		panic(fmt.Sprintf("failed to init search history tool: %v", err))
	}
	return t
}
