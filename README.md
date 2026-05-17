# Go-IM-System

基于 Go 开发的分布式微服务即时通讯系统，支持实时消息、离线同步、AI 流式输出，具备削峰填谷的高可靠性消息链路。

## 项目特性

- **实时通讯**：基于 WebSocket 的实时消息传输，支持单聊
- **离线消息**：用户离线时消息落库，上线后自动同步
- **AI 流式输出**：AI Agent 独立微服务，原生 SSE 流式推送，不丢帧
- **削峰填谷**：全链路 RabbitMQ 异步解耦，网关不查 DB 不调 RPC，M 级并发无压力
- **消息可靠性**：消息持久化 + 手动 ACK + 死信队列，保证每条消息可追溯
- **分布式架构**：Gateway / Logic / Agent 三服务，自建 geeRPC + 服务发现 + 一致性哈希

## TODO List

- [x] **全面接入 MQ 保证消息可靠性**：上行下行独立 Exchange，削峰填谷，DLX 死信兜底
- [x] **拆分 Agent 微服务**：Agent 独立部署，流式输出从 Redis PubSub 替换为原生 SSE
- [x] **SSE 流式推送**：Gateway 直连 Agent SSE 端点，TCP 长连接替代 fire-and-forget
- [ ] **写扩散群聊消息**：群聊写扩散，共用上行 MQ 削峰
- [ ] **基于群聊扩充 AI 功能**：群聊 tools + 意图流转 + Eino Graph 编排

## 技术栈

| 分类 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 语言 | Go | 1.25.8 | 核心开发 |
| Web 框架 | Gin | v1.12.0 | HTTP / SSE 端点 |
| WebSocket | gorilla/websocket | v1.5.3 | 实时消息传输 |
| 消息队列 | RabbitMQ (amqp091-go) | v1.11.0 | 上行下行削峰、死信队列 |
| 数据库 | MySQL + GORM | v1.31.1 | 消息 / 用户持久化 |
| 缓存 | Redis | v8.11.0 | 在线路由表、AI 会话记忆 |
| AI 框架 | Eino (cloudwego) | v0.8.5 | AI Agent 编排、工具调用、流式推理 |
| AI 模型 | DeepSeek (eino-ext) | v0.1.2 | ChatModel |
| 认证 | JWT | v5.2.1 | 用户鉴权 |
| 序列化 | Protocol Buffers | v1.36.11 | RPC 传输 |
| 配置 | Viper | v1.21.0 | YAML 配置管理 |
| 日志 | Zap + Lumberjack | v1.27.1 | 结构化日志 + 滚动归档 |
| 定时任务 | cron | v3.0.1 | 预约消息调度 |
| 自建 RPC | geeRPC | — | 服务间通信 + 负载均衡 |

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        客户端 (WebSocket)                    │
└──────┬────────────────────────────────────────────────┬─────┘
       │                                                │
       ▼                                                ▼
┌──────────────┐  MQ Upload   ┌──────────────┐  SSE    ┌──────────────┐
│   Gateway    │ ───────────▶ │    Logic     │ ◀───── │    Agent     │
│   :8080      │              │    :8001     │  HTTP   │    :8050     │
│              │ ◀─────────── │              │         │              │
│  Gin + WS    │  MQ Down     │  GeeRPC      │         │  Gin + SSE   │
│  连接管理     │              │  落库 + 路由  │         │  Eino + AI   │
│  消息路由     │              │  SyncUnread  │         │  session记忆  │
│  心跳检测     │              │  ACK/已读    │         │  定时消息     │
└──────────────┘              └──────────────┘         └──────────────┘
       │                              │                        │
       └──────────────────────────────┴────────────────────────┘
                            MySQL    Redis    RabbitMQ
```

### 三服务职责

| 服务 | 端口 | 通信 | 不做什么 |
|------|------|------|---------|
| Gateway | 8080 | Gin HTTP + WS, MQ, RPC | 不查 DB、不处理业务逻辑 |
| Logic | 8001 | GeeRPC, MQ 消费 | 不做 AI 推理 |
| Agent | 8050 | Gin HTTP + SSE | 不碰 MQ、不参与消息路由 |

### 消息流程

**普通消息**：
```
Client WS → Gateway(生成MsgID) → MQ Upload → Logic(落库+查路由)
  → MQ Down → 目标Gateway → WS push → 接收方
```

**AI 消息**：
```
Client WS → Gateway(生成MsgID, MQ Upload → Logic落库)
  → SSE GET /agent/chat/sse → Agent(Eino推理)
  → SSE stream → Gateway逐chunk推WS → 发送方
  Agent异步落库AI回复 + 更新Redis session
```

## 目录结构

```
apps/
├── config.yaml              # 统一配置
├── gateway/                 # 网关服务
│   ├── main.go
│   ├── api/user_api/        # REST 登录/注册
│   ├── router/              # 路由注册 + CORS
│   ├── rpcclient/           # GeeRPC 客户端
│   └── ws/                  # WebSocket 核心
│       ├── chat.go          # 单聊 + AI聊天 + ACK
│       ├── client.go        # WS 客户端读写泵
│       ├── handler.go       # WS 升级 + 未读同步
│       ├── manager.go       # 全局连接池 (CliMap)
│       ├── mq_consumer.go   # 下行 MQ 消费
│       └── reliability.go   # SeqID / MsgID 生成
├── logic/                   # 业务逻辑服务
│   ├── main.go
│   ├── dao/                 # 数据层 (仅Logic相关)
│   ├── models/              # 数据模型
│   └── service/
│       ├── chat_service.go  # RPC (SyncUnread/Ack/Delivered)
│       ├── upload_consumer.go # 上行 MQ 消费 + 下行发布
│       └── user_service.go  # RPC (Login/Register)
├── agent/                   # AI Agent 独立服务
│   ├── main.go
│   ├── handler/
│   │   └── chat_sse.go      # SSE 流式端点
│   ├── engine/
│   │   ├── engine.go        # PrepareAgentContext + 流事件解析
│   │   └── graph.go         # Eino Graph 编排
│   ├── memory/
│   │   └── memory.go        # Redis Session 记忆
│   ├── dao/                 # Agent 独立数据层
│   ├── models/              # Agent 独立模型
│   ├── tools/               # AI Tools
│   │   ├── schedule_message.go
│   │   └── search_chat_history.go
│   ├── middleware/           # 限流 + 安全兜底
│   │   ├── rate_limit.go
│   │   └── safe_agent.go
│   ├── callback/
│   │   └── trace_logger.go  # Token / TTFT 追踪
│   └── task/
│       └── schedule_msg.go  # 预约消息 Cron
└── pkg/                     # 共享基础设施
    ├── cache/               # Redis 单例
    ├── config/              # Viper 配置
    ├── db/                  # MySQL GORM 单例
    ├── geeRPC/              # 自建 RPC 框架
    ├── logger/              # Zap 日志
    ├── mq/                  # RabbitMQ 连接 + 发布/消费
    ├── proto/               # pb_msg / pb_user
    └── utils/               # JWT / Bcrypt / Snowflake
```

## 快速开始

### 前置条件
- Go 1.25.8+
- MySQL 5.7+
- Redis 6.0+
- RabbitMQ 3.12+

### 配置
修改 `apps/config.yaml` 中 MySQL DSN、Redis 地址密码、RabbitMQ URL、Agent API Key。

### 启动服务（按顺序，三个终端）

```bash
# 1. Logic
cd apps/logic && go run main.go

# 2. Agent
cd apps/agent && go run main.go

# 3. Gateway
cd apps/gateway && go run main.go
```

### 接口

| 接口 | 说明 |
|------|------|
| `POST /api/login` | 用户登录，获取 JWT |
| `POST /api/register` | 用户注册 |
| `WS /ws?token=xxx` | WebSocket 长连接 |
| `GET /agent/chat/sse?user_id=&message=` | Agent SSE 流式端点 |

### WebSocket 协议

| chat_type | 方向 | 说明 |
|-----------|------|------|
| `single_chat` | Client → Server | 发送消息（receiver=-1 走 AI） |
| `chat_push` | Server → Client | 收到新消息 |
| `ai_chunk` | Server → Client | AI 流式 chunk |
| `ai_end` | Server → Client | AI 回复结束 |
| `server_ack` | Server → Client | 消息已被 MQ 接受 |
| `sync_unread` | Server → Client | 上线同步未读消息 |
| `ack` | Client → Server | 已读回执（双向） |
| `ping` / `pong` | 双向 | 心跳 |

## 可靠性保障

```
链路                    保障机制
─────────────────────────────────────────
Gateway PublishUpload   → 失败打日志，客户端超时重试
MQ 持久化               → amqp.Persistent
Logic 消费崩溃          → 手动 ACK，消息重回队列
Logic 消费解析失败      → Nack → 死信队列
Logic 落库后 PublishDown → 即使失败已在 DB，SyncUnread 兜底
Gateway 下行消费        → 消息已落库，ACK 不重入队
接收方离线              → DB 保底 + SyncUnread 全量拉取
```

## 性能测试

```bash
# 在线单聊
k6 run perf/k6/ws_online_chat.js -e VUS=200 -e DURATION=30s

# AI 流式输出
k6 run perf/k6/ws_ai_stream.js -e VUS=2 -e DURATION=30s
```

## 部署

### 单机
三个 `go run` 即可。

### 分布式
- **Gateway**：多实例 + 负载均衡，每个实例独立 Queue（`gateway.queue.<addr>`）
- **Logic**：多实例竞争消费 `logic.upload.queue`（天然负载均衡）
- **Agent**：可独立扩缩容，无状态（session 存 Redis）
- **MySQL/Redis/RabbitMQ**：建议集群 / 主从部署

## License

MIT
