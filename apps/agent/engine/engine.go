package engine

import (
	"context"
	"errors"
	"fmt"
	"go-im-system/apps/agent/dao"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
	"go-im-system/apps/agent/memory"
	"go-im-system/apps/pkg/logger"
)

// PrepareAgentContext 组装 Agent 执行上下文：
// 短期记忆（Redis 滑窗）+ 中期摘要（MySQL）→ 合并为完整历史消息列表
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

	// 6. 组装最终历史消息列表：
	//    [中期摘要（若有）] + [短期滑窗对话]
	//    注意：摘要以 SystemMessage 形式插入到历史头部，让 LLM 感知上下文
	var fullHistory []*schema.Message
	if recentSummary != "" {
		summaryMsg := schema.SystemMessage(
			fmt.Sprintf("[历史对话摘要，请参考但不必完全重复]\n%s", recentSummary),
		)
		fullHistory = append(fullHistory, summaryMsg)
		logger.Log.Debugf("[AI] 注入中期摘要，长度=%d 字", len(recentSummary))
	}
	fullHistory = append(fullHistory, shortTermHistory...)

	return runner, session, fullHistory, nil
}

func GetAssistantFromEvents(events *adk.AsyncIterator[*adk.AgentEvent]) (<-chan string, error) {
	outChan := make(chan string, 64)
	go func() {
		defer close(outChan)
		for {
			event, ok := events.Next()
			if !ok {
				break
			}
			if event.Err != nil {
				logger.Log.Errorf("AI 事件流解析报错: %v", event.Err)
			}
			if event.Output == nil || event.Output.MessageOutput == nil {
				continue
			}
			mv := event.Output.MessageOutput
			if mv.Role != schema.Assistant {
				continue
			}
			if mv.IsStreaming {
				mv.MessageStream.SetAutomaticClose()
				for {
					frame, err := mv.MessageStream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						logger.Log.Errorf("接收流式帧报错: %v", err)
						break
					}
					if frame != nil && frame.Content != "" {
						outChan <- frame.Content
					}
				}
				continue
			}
			if mv.Message != nil && mv.Message.Content != "" {
				outChan <- mv.Message.Content
			}
		}
	}()
	return outChan, nil
}
