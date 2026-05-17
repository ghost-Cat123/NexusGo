package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// GlobalConfig 全局单例配置对象
var GlobalConfig *Config

// Config 根配置结构体
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Agent    AgentConfig    `mapstructure:"agent"`
	Log      LogConfig      `mapstructure:"log"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
	Milvus   MilvusConfig   `mapstructure:"milvus"`
}

type ServerConfig struct {
	GatewayPort int `mapstructure:"gateway_port"`
	LogicPort   int `mapstructure:"logic_port"`
	AgentPort   int `mapstructure:"agent_port"`
	// Snowflake 节点号 0–1023，网关与 Logic 必须使用不同值以避免 msg_id 冲突
	GatewaySnowflakeNode int    `mapstructure:"gateway_snowflake_node"`
	LogicSnowflakeNode   int    `mapstructure:"logic_snowflake_node"`
	AgentSnowflakeNode   int    `mapstructure:"agent_snowflake_node"`
	AgentAddr            string `mapstructure:"agent_addr"`   // Logic 调用 Agent 的地址
	GatewayAddr          string `mapstructure:"gateway_addr"` // Gateway 对外地址（Docker 用 gateway:8080）
}

type MySQLConfig struct {
	DSN          string `mapstructure:"dsn"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// RabbitMQConfig MQ 连接配置
type RabbitMQConfig struct {
	URL      string `mapstructure:"url"`      // AMQP 连接地址
	Exchange string `mapstructure:"exchange"` // Exchange 名称，默认 gateway.exchange
}

type MilvusConfig struct {
	Addr      string `mapstructure:"addr"` // 向量数据库地址
	APIKey    string `mapstructure:"api_key"`
	ModelName string `mapstructure:"model_name"`
}

// AgentConfig 一级结构体：管理默认Agent + 所有Agent提供商
type AgentConfig struct {
	Default   string                    `mapstructure:"default"`   // 默认Agent名称（切换用）
	Providers map[string]ProviderConfig `mapstructure:"providers"` // 多Agent配置（key=名称，value=配置）
}

// ProviderConfig 二级结构体：单个Agent的具体配置（DeepSeek/OpenAI等）
type ProviderConfig struct {
	APIKey    string `mapstructure:"api_key"`
	BaseURL   string `mapstructure:"base_url"`
	ModelName string `mapstructure:"model_name"`
}

type LogConfig struct {
	Level      string `mapstructure:"level"`
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
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
	path, err := resolveConfigPath(configPath)
	if err != nil {
		return err
	}

	viper.SetConfigFile(path)   // 指定配置文件路径
	viper.SetConfigType("yaml") // 明确指定文件类型

	// 1. 读取配置文件
	if err = viper.ReadInConfig(); err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 2. 自动读取环境变量
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 3. 将配置解析到 GlobalConfig 结构体指针中
	GlobalConfig = &Config{}
	if err = viper.Unmarshal(GlobalConfig); err != nil {
		return fmt.Errorf("解析配置文件到结构体失败: %w", err)
	}

	log.Printf("✅ 配置文件加载成功: %s", path)
	return nil
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

// // GetAgentByName 根据名称获取【指定Agent】配置（多Agent同时使用）
// func GetAgentByName(name string) ProviderConfig {
// 	config, ok := GlobalConfig.Agent.Providers[name]
// 	if !ok {
// 		log.Fatalf("Agent %s 不存在", name)
// 	}
// 	return config
// }
