package mcp

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	"NexusGo/apps/agent/retrieval"
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/utils"
	vdb "NexusGo/apps/pkg/vector_db"
	"context"
	"fmt"
	milvusret "github.com/cloudwego/eino-ext/components/retriever/milvus2"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"strconv"
	"strings"
	"time"
)

func RegisterSearchChatHistory(s *server.MCPServer) {
	tool := mcp.NewTool("search_chat_history",
		mcp.WithDescription("搜索聊天记录。优先使用会话隔离的 Milvus 语义检索，结果为空时回退 MySQL 全文索引。"),
		mcp.WithString("target_user", mcp.Description("单聊对象用户名")),
		mcp.WithString("target_group", mcp.Description("群名称")),
		mcp.WithString("start_time", mcp.Description("起始时间，RFC3339格式")),
		mcp.WithString("end_time", mcp.Description("结束时间，RFC3339格式")),
		mcp.WithString("keywords", mcp.Required(), mcp.Description("搜索关键词，空格分隔")),
		mcp.WithNumber("current_user_id", mcp.Required(), mcp.Description("当前用户ID")),
		mcp.WithNumber("limit", mcp.Description("最大返回条数，默认50")),
	)

	s.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.Params.Arguments
		argsMap, ok := args.(map[string]any)
		if !ok {
			return mcp.NewToolResultError("参数格式错误，arguments 必须为 JSON 对象"), nil
		}
		currentUserID := int64(argsMap["current_user_id"].(float64))

		var targetUserID, targetGroupID int64
		var targetUserName string

		if v, ok := argsMap["target_user"].(string); ok && v != "" {
			u, err := dao.FindUserByName(v)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("未找到用户: %s", v)), nil
			}
			targetUserID = u.UserId
			targetUserName = u.Username
		}
		if v, ok := argsMap["target_group"].(string); ok && v != "" {
			g, err := dao.FindGroupByName(v)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("未找到群: %s", v)), nil
			}
			targetGroupID = g.GroupID
		}
		if targetUserID == 0 && targetGroupID == 0 {
			return mcp.NewToolResultError("请指定搜索对象"), nil
		}
		if targetGroupID != 0 {
			if ok, _ := dao.IsGroupMember(targetGroupID, currentUserID); !ok {
				return mcp.NewToolResultError("你不在该群中"), nil
			}
		}

		startStr, _ := argsMap["start_time"].(string)
		endStr, _ := argsMap["end_time"].(string)
		queryStart, queryEnd := utils.ResolveTime(startStr, endStr)

		kwStr, _ := argsMap["keywords"].(string)
		keywords := strings.Fields(kwStr)

		limit := 50
		if v, ok := argsMap["limit"].(float64); ok {
			limit = int(v)
		}

		candidateLimit := limit
		if candidateLimit < 20 {
			candidateLimit = 20
		}
		if candidateLimit > 200 {
			candidateLimit = 200
		}

		msgs := semanticMCP(ctx, currentUserID, kwStr, targetUserID, targetGroupID, queryStart, queryEnd, candidateLimit)
		if len(msgs) == 0 {
			var err error
			msgs, err = dao.SearchHistoryMessages(currentUserID, targetUserID, targetGroupID, keywords, queryStart, queryEnd, candidateLimit)
			if err != nil {
				msgs = nil
			}
		}

		if len(msgs) == 0 {
			return mcp.NewToolResultText("未找到匹配的聊天记录"), nil
		}

		var lines []string
		for index, m := range msgs {
			if index >= limit {
				break
			}
			name := fmt.Sprintf("用户%d", m.SenderId)
			if m.SenderId == currentUserID {
				name = "你"
			} else if targetUserID != 0 && m.SenderId == targetUserID {
				name = targetUserName
			}
			lines = append(lines, fmt.Sprintf("[%s] %s | %s",
				m.CreateTime.Format("01-02 15:04"), name, m.Content))
		}
		return mcp.NewToolResultText(strings.Join(lines, "\n")), nil
	})
}

func semanticMCP(ctx context.Context, currentUserID int64, keywords string, targetUserID, targetGroupID int64, queryStart, queryEnd time.Time, limit int) []models.Messages {
	embedder, err := vdb.GetEmbedder(config.GlobalConfig.Milvus)
	if err != nil {
		return nil
	}
	var convID string
	if targetGroupID != 0 {
		convID = utils.GroupConvID(targetGroupID)
	} else {
		convID = utils.SingleChatConvID(currentUserID, targetUserID)
	}
	filterExpr := fmt.Sprintf("conv_id == \"%s\"", convID)
	if queryStart.Unix() > 0 {
		filterExpr += fmt.Sprintf(" && send_time >= %d", queryStart.Unix())
	}
	if queryEnd.Unix() > 0 {
		filterExpr += fmt.Sprintf(" && send_time <= %d", queryEnd.Unix())
	}
	retriever, err := vdb.GetRetriever(ctx, vdb.MessageCollection, "embedding",
		[]string{"msg_id", "sender_id", "send_time"}, limit, embedder)
	if err != nil {
		return nil
	}
	docs, err := retriever.Retrieve(ctx, keywords, milvusret.WithFilter(filterExpr))
	if err != nil {
		return nil
	}
	docs = retrieval.FilterDocumentsByScore(docs, retrieval.MinMessageSemanticScore)
	if len(docs) == 0 {
		return nil
	}
	var ids []int64
	for _, d := range docs {
		if id, e := strconv.ParseInt(d.ID, 10, 64); e == nil {
			ids = append(ids, id)
		}
	}
	msgs, _ := dao.GetMessagesByIDs(ids)
	ordered := retrieval.OrderMessagesByIDs(msgs, ids)
	contents := make([]string, 0, len(ordered))
	for _, message := range ordered {
		contents = append(contents, message.Content)
	}
	if !retrieval.HasContentKeywordCoverage(contents, strings.Fields(keywords)) {
		return nil
	}
	return ordered
}
