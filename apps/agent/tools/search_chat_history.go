package tools

import (
	"context"
	"fmt"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"go-im-system/apps/agent/dao"
	"go-im-system/apps/agent/models"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/utils"
	vdb "go-im-system/apps/pkg/vector_db"
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

	// ── 第一级：MySQL FULLTEXT 精确路由 ──
	messages, err := dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID, keywords, queryStart, queryEnd, req.Limit)
	if err != nil {
		// DB 异常不算命中失败，打日志后按 0 条处理，尝试降级
		fmt.Printf("[Search] FULLTEXT 查询异常: %v，尝试语义降级\n", err)
		messages = nil
	}

	// ── 第二级：Milvus 语义降级 ──
	if len(messages) == 0 {
		messages = semanticFallback(ctx, currentUserID, req.Keywords, targetUserID, targetGroupID, queryStart)
	}

	if len(messages) == 0 {
		return &SearHistoryResp{
			Result: "未找到匹配的聊天记录，建议调整关键词或扩大时间范围",
		}, nil
	}

	var rawText string
	for _, msg := range messages {
		senderName := resolveSenderName(msg.SenderId, currentUserID, targetUserID, targetUserName)
		rawText += fmt.Sprintf("[%s]: %s\n", senderName, msg.Content)
	}

	return &SearHistoryResp{
		Result: fmt.Sprintf("以下是原始聊天记录，请根据用户的需求进行总结或提取:\n%s", rawText),
	}, nil
}

// semanticFallback 语义降级：Embedding → Milvus → 回表 MySQL
func semanticFallback(ctx context.Context, currentUserID int64, keywords string, targetUserID, targetGroupID int64, queryStart time.Time) []models.Messages {
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

	// 使用 milvus2.WithFilter 生成实现特定选项
	// WithFilter：绑定标量过滤条件
	filterOption := milvusret.WithFilter(filterExpr)

	// 输出字段
	outputFields := []string{"msg_id", "sender_id", "send_time"}
	retriever, err := vdb.GetRetriever(ctx, vdb.CollectionName, "embedding", outputFields, 5, embedder)

	if err != nil {
		fmt.Printf("[Search] Retriever 初始化失败: %v\n", err)
		return nil
	}

	// 将标量过滤选项放入Retrieve的选项中
	results, err := retriever.Retrieve(ctx, keywords, filterOption)
	if err != nil || len(results) == 0 {
		fmt.Printf("[Search] 语义检索无结果: err=%v count=%d\n", err, len(results))
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
	return messages
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
