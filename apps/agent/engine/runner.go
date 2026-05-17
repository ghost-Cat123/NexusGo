package engine

import (
	"context"
	"fmt"
	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"go-im-system/apps/agent/callback"
	"go-im-system/apps/agent/middleware"
	"go-im-system/apps/agent/tools"
	"go-im-system/apps/pkg/config"
	"go-im-system/apps/pkg/logger"
	"os"
	"strings"
)

func resolveAgentAPIKey(defaultAgent config.ProviderConfig) string {
	if strings.TrimSpace(defaultAgent.APIKey) != "" {
		return strings.TrimSpace(defaultAgent.APIKey)
	}

	defaultName := strings.ToUpper(strings.TrimSpace(config.GlobalConfig.Agent.Default))
	if defaultName != "" {
		// 支持 Viper 的层级环境变量写法：AGENT_PROVIDERS_DEEPSEEK_API_KEY
		if key := strings.TrimSpace(os.Getenv("AGENT_PROVIDERS_" + defaultName + "_API_KEY")); key != "" {
			return key
		}
	}

	// 兼容常见命名
	if key := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY")); key != "" {
		return key
	}
	return ""
}

func GetAgentGraphRunner(ctx context.Context) (*adk.Runner, error) {
	defaultAgent := config.GetDefaultAgent()
	instruction := "You are a helpful assistant."
	apiKey := resolveAgentAPIKey(defaultAgent)
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
	// 注册全局chatModel and tools callbacks
	callbacks.AppendGlobalHandlers(&callback.TraceLoggerCallback{})
	// 创建Agent
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "IM-System-Agent",
		Description: "DeepSeek Agent",
		Model:       cm,
		Instruction: instruction,

		MaxIterations: 5,
		Handlers: []adk.ChatModelAgentMiddleware{
			&middleware.RateLimitMiddleware{},
			&middleware.SafeAgentMiddleware{},
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{
					tools.MustSearchHistoryTool(),
					tools.MustSchMessageTool(),
				},
			},
		},
		ModelRetryConfig: &adk.ModelRetryConfig{
			MaxRetries: 5,
			IsRetryAble: func(_ context.Context, err error) bool {
				return strings.Contains(err.Error(), "429") ||
					strings.Contains(err.Error(), "Too Many Requests") ||
					strings.Contains(err.Error(), "qpm limit")
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	}), nil
}
