package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
	"github.com/gin-gonic/gin"
	"go-im-system/apps/agent/dao"
	"go-im-system/apps/agent/engine"
	sseevent "go-im-system/apps/agent/event"
	"go-im-system/apps/agent/middleware"
	"go-im-system/apps/agent/models"
	"go-im-system/apps/pkg/logger"
	"io"
	"net/http"
	"strconv"
	"time"
)

// 以JSON格式将事件写入SSE，返回写入错误
func writeSSEEvent(w io.Writer, eventType sseevent.EventType, payload any, flusher http.Flusher) error {
	data, _ := json.Marshal(map[string]any{
		"type": string(eventType),
		"data": payload,
	})
	if _, err := fmt.Fprintf(w, "data: %s\n\n", string(data)); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func ChatSSE(c *gin.Context) {
	// 提取请求参数
	senderId, _ := strconv.ParseInt(c.Query("user_id"), 10, 64)
	message := c.Query("message")

	// SSE标准响应头
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	// 检查接口实现
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	// 构造context
	ctx := context.WithValue(c.Request.Context(), "current_user_id", senderId)
	runner, session, history, err := engine.PrepareAgentContext(ctx, senderId, message)
	if err != nil {
		logger.Log.Errorf("[SSE] 准备上下文失败: %v", err)
		_ = writeSSEEvent(c.Writer, sseevent.EventError, "AI服务初始化失败，请稍后重试", flusher)
		_ = writeSSEEvent(c.Writer, sseevent.EventDone, nil, flusher)
		return
	}
	// 生成唯一checkpointID (和redis的sessionID不一样)
	checkPointID := fmt.Sprintf("%s:%d", session.ID, time.Now().UnixNano())

	// 创建可取消的 context，handler 退出时通知 Runner 停止 Token生成
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 处理AI流
	events := runner.Run(ctx, history, adk.WithCheckPointID(checkPointID))

	// 循环消费事件
	var fullText string
	for {
		// 同步消费
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			logger.Log.Errorf("AI 事件流解析报错: %v", event.Err)
			continue
		}
		// 中断检测
		if event.Action != nil && event.Action.Interrupted != nil {
			if err := sendInterruptEvent(c.Writer, event, checkPointID, flusher); err != nil {
				logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
				return
			}

			sessionCh := engine.RegisterSessionChan(checkPointID)
			var result *middleware.ApprovalResult
			select {
			// 超时 兜底
			case <-ctx.Done():
				return
			// 获取中断结果
			case result = <-sessionCh:
			}
			targets := buildTargets(event.Action.Interrupted.InterruptContexts, result)
			events, err = runner.ResumeWithParams(ctx, checkPointID, &adk.ResumeParams{Targets: targets})
			if err != nil {
				logger.Log.Errorf("中断恢复错误: %v", err)
				break
			}
			continue
		}

		// 提取文本内容
		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}
		mv := event.Output.MessageOutput

		switch mv.Role {
		case schema.Assistant:
			if mv.IsStreaming {
				// 流式消息 逐chunk解析
				mv.MessageStream.SetAutomaticClose()
				var fullMsg *schema.Message
				for {
					frame, err := mv.MessageStream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						logger.Log.Errorf("[SSE] 读取流式帧报错: %v", err)
						break
					}
					fullMsg = frame
					// 思考模式的内容
					if frame.ReasoningContent != "" {
						if err := writeSSEEvent(c.Writer, sseevent.EventReasoningChunk, frame.ReasoningContent, flusher); err != nil {
							logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
							return
						}
					}
					// 正常输出消息内容
					if frame != nil && frame.Content != "" {
						fullText += frame.Content
						if err := writeSSEEvent(c.Writer, sseevent.EventStreamChunk, frame.Content, flusher); err != nil {
							logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
							return
						}
					}
				}
				// 流结束后，检查是否有工具调用
				if fullMsg != nil && len(fullMsg.ToolCalls) > 0 {
					for _, tc := range fullMsg.ToolCalls {
						if err := writeSSEEvent(c.Writer, sseevent.EventToolCall, map[string]string{
							"tool": tc.Function.Name,
							"args": tc.Function.Arguments,
						}, flusher); err != nil {
							logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
							return
						}
					}
				}
			} else if mv.Message != nil {
				// 非流式信息
				fullText += mv.Message.Content
				if err := writeSSEEvent(c.Writer, sseevent.EventStreamChunk, mv.Message.Content, flusher); err != nil {
					logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
					return
				}
				// 处理工具调用
				for _, tc := range mv.Message.ToolCalls {
					if err := writeSSEEvent(c.Writer, sseevent.EventToolCall, map[string]string{
						"tool": tc.Function.Name,
						"args": tc.Function.Arguments,
					}, flusher); err != nil {
						logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
						return
					}
				}
			}
		case schema.Tool:
			// 工具执行结果
			if err := writeSSEEvent(c.Writer, sseevent.EventToolResult, map[string]string{
				"tool":   mv.ToolName,
				"result": mv.Message.Content,
			}, flusher); err != nil {
				logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
				return
			}
		}
	}

	if err := writeSSEEvent(c.Writer, sseevent.EventDone, nil, flusher); err != nil {
		logger.Log.Warnf("[SSE] 客户端断开，停止推送: user=%d", senderId)
		return
	}
	if fullText == "" {
		return
	}

	// 异步落库AI消息， 更新Redis Session
	go func() {
		assistantMsg := schema.AssistantMessage(fullText, nil)
		if err := session.Append(context.Background(), assistantMsg); err != nil {
			logger.Log.Errorf("[SSE] session 更新失败: %v", err)
		}
		if err = dao.InsertMessage(models.NewMessages(-1, senderId, fullText, false)); err != nil {
			logger.Log.Errorf("AI消息落库失败: %v", err)
		}
		logger.Log.Infof("[SSE] AI 回复已落库: user=%d", senderId)
	}()
}

func buildTargets(interrupts []*adk.InterruptCtx, result *middleware.ApprovalResult) map[string]any {
	targets := make(map[string]any, len(interrupts))
	// 中断时可能有多个 InterruptCtx 并行工具调用的中断
	for _, ic := range interrupts {
		// 只处理原始中断触发点（跳过传播的子中断）
		if ic.IsRootCause {
			targets[ic.ID] = &middleware.ApprovalResult{
				Approved: result.Approved,
				Reason:   result.Reason,
			}
		}
	}
	return targets
}

func sendInterruptEvent(w io.Writer, event *adk.AgentEvent, checkpointID string, flusher http.Flusher) error {
	for _, ic := range event.Action.Interrupted.InterruptContexts {
		info, ok := ic.Info.(*middleware.ApprovalInfo)
		if !ok {
			logger.Log.Errorf("提取中断Info失败")
			continue
		}
		if err := writeSSEEvent(w, sseevent.EventInterrupt, map[string]any{
			"checkpoint_id": checkpointID,
			"tool_name":     info.ToolName,
			"arguments":     info.ArgumentsInJson,
		}, flusher); err != nil {
			return err
		}
	}
	return nil
}
