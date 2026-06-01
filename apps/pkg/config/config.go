package config

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
	AgentAddr            string `mapstructure:"agent_addr"`   // Gateway 调用 Agent 的地址
	GatewayAddr          string `mapstructure:"gateway_addr"` // Gateway 对外地址（Docker 用 gateway:8080）
	LogicAddr            string `mapstructure:"logic_addr"`   // Logic RPC 地址（Docker 用 logic:8001）
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
