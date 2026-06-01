package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// GlobalConfig 全局单例配置对象
// 利用atomic.Pointer进行配置热重载
var (
	globalConfig atomic.Pointer[Config]
	GlobalConfig *Config
	reloadHooks  []func()
	reloadMu     sync.Mutex
)

// Get 返回当前配置（未初始化时返回 nil）
func Get() *Config {
	return globalConfig.Load()
}

// Reload 重新从文件加载配置，触发缓存失效
func Reload() error {
	reloadMu.Lock()
	defer reloadMu.Unlock()
	// 复用 resolveConfigPath + viper 重新读取
	// 你已有的 loadConfig() 逻辑提取出来
	cfg, err := loadConfigFromFile("") // "" 让 resolveConfigPath 自动找
	if err != nil {
		return err
	}
	// 缓存
	globalConfig.Store(cfg)
	GlobalConfig = cfg
	// 触发缓存失效
	for _, hook := range reloadHooks {
		hook()
	}
	return nil
}

// OnReload 注册热重载回调（runner/CM 缓存自己注册）
func OnReload(fn func()) {
	reloadHooks = append(reloadHooks, fn)
}

func loadConfigFromFile(configPath string) (*Config, error) {
	path, err := resolveConfigPath(configPath)
	if err != nil {
		return nil, err
	}
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")
	if err = viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	cfg := &Config{}
	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	return cfg, nil
}

func resolveConfigPath(configPath string) (string, error) {
	candidates := make([]string, 0, 8)
	if configPath != "" {
		candidates = append(candidates, configPath)
	}
	if envPath := strings.TrimSpace(os.Getenv("APP_CONFIG")); envPath != "" {
		candidates = append(candidates, envPath)
	}
	candidates = append(candidates,
		"./apps/config.yaml",
		"./config.yaml",
		"../apps/config.yaml",
		"../config.yaml",
		"../../apps/config.yaml",
		"../../config.yaml",
	)

	for _, candidate := range candidates {
		absPath, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if stat, err := os.Stat(absPath); err == nil && !stat.IsDir() {
			return absPath, nil
		}
	}
	return "", fmt.Errorf("未找到配置文件，请通过参数传入路径或设置 APP_CONFIG 环境变量")
}

// InitConfig 初始化配置，供 main.go 启动时调用
func InitConfig(configPath string) error {
	cfg, err := loadConfigFromFile(configPath)
	if err != nil {
		return err
	}
	globalConfig.Store(cfg)
	GlobalConfig = cfg
	return nil
}

func ResolveAgentAPIKey(defaultAgent ProviderConfig) string {
	if strings.TrimSpace(defaultAgent.APIKey) != "" {
		return strings.TrimSpace(defaultAgent.APIKey)
	}

	defaultName := strings.ToUpper(strings.TrimSpace(GlobalConfig.Agent.Default))
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

// GetDefaultAgent 获取【默认Agent】配置（切换后自动生效）
func GetDefaultAgent() ProviderConfig {
	agentName := GlobalConfig.Agent.Default
	config, ok := GlobalConfig.Agent.Providers[agentName]
	if !ok {
		log.Fatalf("Agent %s 不存在", agentName)
	}
	return config
}
