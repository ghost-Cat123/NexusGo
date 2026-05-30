package engine

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/middlewares/patchtoolcalls"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"go-im-system/apps/agent/memory"
	"go-im-system/apps/agent/middleware"
	"go-im-system/apps/agent/tools"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"strings"
	"sync"
)

var (
	cacheRunner *adk.Runner
	runnerMu    sync.Mutex
	runnerOnce  sync.Once
)

const instruction = "You are a helpful assistant."
const maxIterations = 5
const maxRetries = 5

func GetAgentGraphRunner(ctx context.Context) (*adk.Runner, error) {
	// 快速路径
	// 缓存中有runner 直接返回 避免重复创建开销
	if cacheRunner != nil {
		return cacheRunner, nil
	}

	// 慢速路径
	runnerMu.Lock()
	defer runnerMu.Unlock()
	// 再次检查 避免加锁时并发创建
	if cacheRunner != nil {
		return cacheRunner, nil
	}
	// 没有才创建runner
	runner, err := buildRunner(ctx)
	if err != nil {
		return nil, err
	}
	// 缓存runner
	cacheRunner = runner
	return cacheRunner, nil
}

func buildRunner(ctx context.Context) (*adk.Runner, error) {
	cm, err := buildChatModel(ctx)
	if err != nil {
		return nil, err
	}
	agent, err := buildAgent(ctx, cm)
	if err != nil {
		return nil, err
	}
	return adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
		CheckPointStore: memory.NewCheckPointStore(),
	}), nil
}

func buildChatModel(ctx context.Context) (*deepseek.ChatModel, error) {
	defaultAgent := config.GetDefaultAgent()
	apiKey := config.ResolveAgentAPIKey(defaultAgent)
	if apiKey == "" {
		return nil, fmt.Errorf("AI API Key 为空，请在 apps/config.yaml 填写 agent.providers.%s.api_key 或设置环境变量 DEEPSEEK_API_KEY", config.GlobalConfig.Agent.Default)
	}

	// 配置ChatModel
	cm, err := deepseek.NewChatModel(ctx, &deepseek.ChatModelConfig{
		APIKey:  apiKey,
		Model:   defaultAgent.ModelName,
		BaseURL: defaultAgent.BaseURL,
	})
	if err != nil {
		return nil, fmt.Errorf("init model failed: %w", err)
	}
	logger.Log.Infof("AI 模型初始化成功，provider=%s model=%s", config.GlobalConfig.Agent.Default, defaultAgent.ModelName)
	return cm, nil
}

func buildAgent(ctx context.Context, cm *deepseek.ChatModel) (*adk.ChatModelAgent, error) {
	// 声明内置中间件
	patchToolMiddleware, err := patchtoolcalls.New(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("初始化内置中间件失败: %w", err)
	}
	// 创建Agent
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "IM-System-Agent",
		Description: "DeepSeek Agent",
		Model:       cm,
		Instruction: instruction,

		MaxIterations: maxIterations,

		Handlers: []adk.ChatModelAgentMiddleware{
			patchToolMiddleware,                  // 修复工具调用 ID 一致性
			&middleware.AutoContinueMiddleware{}, // AfterModel: 截断续写
			&middleware.TrimResultMiddleWare{},   // BeforeModel: 裁剪旧结果
			&middleware.RateLimitMiddleware{},    // BeforeAgent: 限流
			&middleware.ApprovalMiddleware{},     // WrapInvokable: 审批
			&middleware.SafeAgentMiddleware{},    // WrapModel + WrapTool: 兜底
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{
					tools.MustSearchHistoryTool(),
					tools.MustSchMessageTool(),
				},
				ToolCallMiddlewares: []compose.ToolMiddleware{
					middleware.ToolFixMiddleware(),
				},
			},
		},
		ModelRetryConfig: &adk.ModelRetryConfig{
			MaxRetries: maxRetries,
			IsRetryAble: func(_ context.Context, err error) bool {
				return strings.Contains(err.Error(), "429") ||
					strings.Contains(err.Error(), "Too Many Requests") ||
					strings.Contains(err.Error(), "qpm limit")
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("初始化Agent失败: %w", err)
	}
	return agent, nil
}
