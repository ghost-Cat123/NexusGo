package tools

import (
	"NexusGo/apps/agent/dao"
	"NexusGo/apps/agent/models"
	"NexusGo/apps/pkg/utils"
	"context"
	"fmt"
	"github.com/cloudwego/eino/components/tool"
	toolutils "github.com/cloudwego/eino/components/tool/utils"
	"strings"
)

type SummarizeGroupReq struct {
	GroupName   string `json:"group_name" jsonschema:"required,description=群聊名称"`
	StartTime   string `json:"start_time" jsonschema:"description=起始时间 RFC3339 格式, 默认24小时前"`
	EndTime     string `json:"end_time" jsonschema:"description=结束时间 RFC3339 格式, 默认当前时间"`
	MaxMessages int    `json:"max_messages" jsonschema:"description=最大拉取消息条数, 默认200, maximum=500"`
}

type SummarizeGroupResp struct {
	Result string `json:"result"`
}

func summarizeGroupInvoker(ctx context.Context, req *SummarizeGroupReq) (*SummarizeGroupResp, error) {
	currentUserID, ok := ctx.Value("current_user_id").(int64)
	if !ok {
		return nil, fmt.Errorf("internal error: missing user context")
	}

	group, err := dao.FindGroupByName(req.GroupName)
	if err != nil {
		return nil, fmt.Errorf("未找到群 '%s'，请确认群名称", req.GroupName)
	}

	exists, _ := dao.IsGroupMember(group.GroupID, currentUserID)
	if !exists {
		return nil, fmt.Errorf("你不在群 '%s' 中，无法查看消息", req.GroupName)
	}

	queryStart, queryEnd := utils.ResolveTime(req.StartTime, req.EndTime)
	limit := req.MaxMessages
	if limit <= 0 || limit > 500 {
		limit = 200
	}

	msgs, err := dao.GetGroupMessages(currentUserID, group.GroupID, queryStart, queryEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("拉取群消息失败: %w", err)
	}

	if len(msgs) == 0 {
		return &SummarizeGroupResp{Result: "该时间段内无群聊消息"}, nil
	}

	// 按时间正序（DAO 返回倒序）
	var rawText strings.Builder
	rawText.WriteString(fmt.Sprintf("群聊「%s」最近 %d 条消息：\n\n", req.GroupName, len(msgs)))
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		name := resolveGroupSender(m, currentUserID)
		rawText.WriteString(fmt.Sprintf("[%s] %s: %s\n",
			m.CreateTime.Format("01-02 15:04"), name, m.Content))
	}

	return &SummarizeGroupResp{
		Result: rawText.String(),
	}, nil
}

func resolveGroupSender(msg models.Messages, currentUserID int64) string {
	if msg.SenderId == currentUserID {
		return "你"
	}
	return fmt.Sprintf("用户%d", msg.SenderId)
}

func SummarizeGroupTool() (tool.InvokableTool, error) {
	return toolutils.InferTool(
		"summarize_group",
		"拉取指定群聊最近的消息记录。当用户需要总结群聊、回顾讨论内容时使用此工具。返回原始消息文本，由 LLM 进一步总结提炼",
		summarizeGroupInvoker,
	)
}

func MustSummarizeGroupTool() tool.InvokableTool {
	t, err := SummarizeGroupTool()
	if err != nil {
		panic(fmt.Sprintf("MustSummarizeGroupTool failed: %v", err))
	}
	return t
}
