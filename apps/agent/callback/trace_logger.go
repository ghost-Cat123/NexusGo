package callback

import (
	"NexusGo/apps/pkg/logger"
	"context"
	"github.com/cloudwego/eino/components"
	"time"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func init() {
	callbacks.AppendGlobalHandlers(&TraceLoggerCallback{})
}

type TraceLoggerCallback struct {
	// 继承基础 Handler，这样你就不需要实现所有的几十个接口，只需重写你关心的
	callbacks.Handler
}

// OnStartWithStreamInput 流式输出callback
func (c *TraceLoggerCallback) OnStartWithStreamInput(ctx context.Context, info *callbacks.RunInfo,
	input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	// 将当前时间注入 Context 传递给后续生命周期
	defer input.Close()
	if info.Component == components.ComponentOfChatModel {
		return context.WithValue(ctx, "chat_model_start_time", time.Now())
	}
	return ctx
}

func (c *TraceLoggerCallback) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo,
	output *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	// 只处理大模型的流式输出
	if info.Component == components.ComponentOfChatModel {
		startTime, ok := ctx.Value("chat_model_start_time").(time.Time)
		if !ok {
			// 如果没拿到时间，直接关闭副本防止泄漏，并立刻返回
			output.Close()
			return ctx
		}

		// 【核心修复】：开启极其轻量级的旁路协程，去慢慢读这个副本
		go func() {
			// 【极其重要】：关闭流的操作必须放在协程内部！
			defer output.Close()

			isFirstToken := true
			var finalUsage *model.TokenUsage

			for {
				chunk, err := output.Recv()

				// 1. TTFT 首字耗时统计
				if isFirstToken && chunk != nil {
					ttft := time.Since(startTime)
					logger.Log.Infof("[Model Trace] 首字响应耗时 (TTFT): %v", ttft)
					isFirstToken = false
				}

				// 2. 流结束，计算总耗时并跳出
				if err != nil { // 包含 io.EOF 和其他报错
					if err.Error() == "EOF" {
						logger.Log.Infof("[Model Trace] 模型流式生成完毕 | 总耗时: %v", time.Since(startTime))
					} else {
						logger.Log.Errorf("[Model Trace] 读取流副本报错: %v", err)
					}
					break
				}

				// 3. 提取 Token 消耗 (覆盖到最后一个 chunk)
				modelOutput := model.ConvCallbackOutput(chunk)
				if modelOutput != nil && modelOutput.TokenUsage != nil {
					finalUsage = modelOutput.TokenUsage
				}
			}

			// 循环结束，打印最终拿到的 Token
			if finalUsage != nil {
				logger.Log.Infof("[Model Trace] Token消耗 | Input: %d, Output: %d, Total: %d",
					finalUsage.PromptTokens, finalUsage.CompletionTokens, finalUsage.TotalTokens)
			}
		}()

		// 外部直接瞬间 return ctx，不占用框架一丝一毫的时间！
		return ctx
	}

	// 如果不是 ChatModel（比如别的组件），我们不需要看副本，直接关掉防止泄漏！
	output.Close()
	return ctx
}

func (c *TraceLoggerCallback) OnStart(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
	// 将当前时间注入 Context 传递给后续生命周期
	// 区分不同组件，注入不同的 StartTime，绝对防撞车
	if info.Component == components.ComponentOfChatModel {
		return context.WithValue(ctx, "chat_model_start_time", time.Now())
	}
	if info.Component == components.ComponentOfTool {
		return context.WithValue(ctx, "tool_start_time", time.Now())
	}
	return ctx
}

func (c *TraceLoggerCallback) OnEnd(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
	// A. 工具调用结束统计 (你的 search_history 和 schedule_message 会走这里)
	if info.Component == components.ComponentOfTool {
		if startTime, ok := ctx.Value("tool_start_time").(time.Time); ok {
			logger.Log.Infof("[Tool Trace] 工具 [%s] 执行完毕 | 耗时: %v", info.Type, time.Since(startTime))
		}
	}
	// B. 大模型同步调用结束统计 (防兜底)
	if info.Component == components.ComponentOfChatModel {
		if startTime, ok := ctx.Value("chat_model_start_time").(time.Time); ok {
			logger.Log.Infof("[Model Trace] 模型同步生成完毕 | 总耗时: %v", time.Since(startTime))
		}
		modelOutput := model.ConvCallbackOutput(output)
		if modelOutput != nil && modelOutput.TokenUsage != nil {
			logger.Log.Infof("[Model Trace] Token消耗 | Input: %d, Output: %d, Total: %d",
				modelOutput.TokenUsage.PromptTokens, modelOutput.TokenUsage.CompletionTokens, modelOutput.TokenUsage.TotalTokens)
		}
	}
	return ctx
}
