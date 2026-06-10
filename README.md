# NexusGo

基于 Go 开发的分布式微服务即时通讯系统，支持实时消息、离线同步、AI 流式输出，具备削峰填谷的高可靠性消息链路。

## 项目特性

- **实时通讯**：基于 WebSocket 的实时消息传输，支持单聊与群聊
- **离线消息**：用户离线时消息落库，上线后自动同步
- **AI 流式输出**：AI Agent 独立微服务，原生 SSE 流式推送，不丢帧
- **削峰填谷**：全链路 RabbitMQ 异步解耦，网关不查 DB 不调 RPC，千级并发无压力
- **消息可靠性**：消息持久化 + 手动 ACK + 死信队列，保证每条消息可追溯
- **分布式架构**：Gateway / Logic / Agent 三服务，自建 geeRPC + 服务发现 + 一致性哈希
- **向量记忆**：Milvus 向量数据库接入，RabbitMQ 异步双写，支持语义相似度历史检索
- **容器化部署**：完整 Docker Compose 一键启动，含 MySQL/Redis/RabbitMQ/Milvus/MinIO/etcd

## TODO List

### ✅ 已完成

- [x] **全面接入 MQ 保证消息可靠性**：上行下行独立 Exchange，削峰填谷，DLX 死信兜底
- [x] **拆分 Agent 微服务**：Agent 独立部署，流式输出从 Redis PubSub 替换为原生 SSE
- [x] **SSE 流式推送**：Gateway 直连 Agent SSE 端点，TCP 长连接替代 fire-and-forget
- [x] **写扩散群聊消息**：群聊写扩散，共用上行 MQ 削峰
- [x] **接入向量数据库**：Milvus + MinIO + etcd，RabbitMQ 异步双写，消息向量化索引
- [x] **语义历史搜索**：Eino RAG 工具，向量检索 → 回表取真实消息 → LLM 总结
- [x] **三层 AI 记忆体系**：短期（Redis 滑动窗口）+ 中期（LLM 摘要 → MySQL 落库）+ 长期（摘要 Embedding → Milvus 向量索引）
- [x] **Agent 完整工程化**：工具安全分类（只读/写入）、HITL 审批中断、配置热重载、Runner 缓存、SSE JSON 传递、续写/裁剪/兜底全套中间件、Token/TTFT 追踪回调
- [x] **MySQL 批量写入**：`batchInsertWorker` 双重触发（满 50 条 / 30ms 定时），Redis `MGet` 批量查路由，失败幂等降级单条
- [x] **Docker 容器化部署**：完整 `docker-compose.yml`，三微服务 + 六基础设施一键启动

### 🚧 进行中 / 待实现

#### Agent 能力完善
- [x] **工具安全分类**：`readOnly` / `destructive` 两级标注（`metadata.go`），中间件按标签自动拦截
- [x] **HITL 审批中断**：`ApprovalMiddleware` 对写入型工具触发 `StatefulInterrupt`，前端 Approve/Reject 后恢复执行
- [x] **自动续写中间件**：`AutoContinueMiddleware` 检测截断标志自动补全，防止长响应被切断
- [x] **工具结果裁剪中间件**：`TrimResultMiddleWare` 裁剪过长工具返回，防止 Context 溢出
- [x] **配置热重载**：Viper `OnReload` 回调清除 Runner 缓存，下次请求自动重建 ChatModel + Agent
- [ ] **P1**：摘要 ChatModel 与主模型解耦（独立小模型专跑摘要，降低成本）
- [ ] **P3**：前端实时推送工具调用过程（显示"正在搜索聊天记录…"）

#### 群聊 AI Tools（参见 plan.md）
- [ ] **T1 `search_chat_history` 重构**：加 `group_name`/`group_id` 参数，MySQL FULLTEXT + ngram 索引
- [ ] **T2 `schedule_message` 群发改造**：加 `group_id` 字段，到点写扩散到所有群成员
- [ ] **T3 `summarize_group`（P0）**：群消息智能摘要，提取核心议题/决议/争议点，Markdown 输出
- [ ] **T4 `create_poll`（P1）**：AI 自动提取投票选项，创建结构化投票卡片，推送到群
- [ ] **T5 `action_item_extractor`（P1）**：识别承诺性话语，输出结构化待办清单（assignee + deadline）
- [ ] **T6 `group_remind`（P2）**：复用 schedule_message 群发分支，批量 @ 群成员定时提醒

#### Eino Graph 编排（参见 plan.md）
- [ ] **G1 意图路由（Intent Router）**：闲聊走轻量路径不加载 Tools，工具类走 ReAct，预期降低闲聊延迟 60%+
- [ ] **G2 多步串联（Chain）**：复杂任务自动拆解，`search → summarise → extract → remind` 串联执行
- [ ] **G3 条件分支（Conditional）**：根据上步工具返回结果动态选择下一步，支持"找不到则追问"等逻辑
- [ ] **G4 ReAct 深层检索**：Embedding → Milvus 向量检索 → 回表取真实文本 → LLM 汇总，最大 3 轮迭代

#### 基础功能 CRUD
- [ ] **好友系统**：好友申请/同意/拒绝、好友列表、删除好友
- [ ] **消息列表**：会话列表、历史消息分页、消息已读状态展示
- [ ] **群聊管理**：创建群、邀请成员、踢人、修改群信息、转让群主

#### 性能 & 可观测性
- [x] **MySQL 批量写入**：`batchInsertWorker` 满 50 条或 30ms 触发批量 INSERT，`Redis MGet` 批量查路由，失败幂等降级单条
- [ ] **Nginx 反向代理**：Gateway 前置 Nginx，`ip_hash` 保证 WS 长连接粘性
- [ ] **Logic 水平扩展压测**：多实例竞争消费，验证线性扩展能力
- [ ] **Prometheus + Grafana**：MQ 积压监控、RPC 延迟、消息吞吐大盘


## 技术栈

| 分类 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 语言 | Go | 1.26.3 | 核心开发 |
| Web 框架 | Gin | v1.12.0 | HTTP / SSE 端点 |
| WebSocket | gorilla/websocket | v1.5.3 | 实时消息传输 |
| 消息队列 | RabbitMQ (amqp091-go) | v1.11.0 | 上行下行削峰、死信队列 |
| 数据库 | MySQL + GORM | v1.31.1 | 消息 / 用户持久化 |
| 缓存 | Redis | v8.11.0 | 在线路由表、AI 会话记忆 |
| 向量数据库 | Milvus | v2.6.14 | 消息语义检索 |
| 对象存储 | MinIO | 2023-03-13 | Milvus 后端存储 |
| 服务发现 | etcd | v3.5.5 | Milvus 元数据存储 |
| AI 框架 | Eino (cloudwego) | v0.8.5 | AI Agent 编排、工具调用、流式推理 |
| AI 模型 | DeepSeek (eino-ext) | v0.1.2 | ChatModel |
| 认证 | JWT | v5.3.0 | 用户鉴权 |
| 序列化 | Protocol Buffers | v1.36.11 | RPC 传输 |
| 配置 | Viper | v1.21.0 | YAML 配置管理 |
| 日志 | Zap + Lumberjack | v1.27.1 | 结构化日志 + 滚动归档 |
| 定时任务 | cron | v3.0.1 | 预约消息调度 |
| 自建 RPC | geeRPC | — | 服务间通信 + 一致性哈希负载均衡 |
| 容器化 | Docker Compose | — | 一键启动全栈环境 |

## 系统架构

```
                    ┌─────────────────┐
                    │  Nginx (可选)    │  ← WS 粘性 ip_hash，Gateway 水平扩展入口
                    └────────┬────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                        客户端 (WebSocket)                         │
└──────┬────────────────────────────────────────────────┬──────────┘
       │                                                │
       ▼                                                ▼
┌──────────────┐  MQ Upload   ┌──────────────┐  SSE    ┌──────────────┐
│   Gateway    │ ───────────▶ │    Logic     │ ◀────── │    Agent     │
│   :8080      │              │    :8001     │  HTTP   │    :8050     │
│              │ ◀─────────── │              │         │              │
│  Gin + WS    │  MQ Down     │  GeeRPC      │         │  Gin + SSE   │
│  连接管理     │              │  落库 + 路由  │         │  Eino + AI   │
│  消息路由     │              │  SyncUnread  │         │  session记忆  │
│  心跳检测     │              │  ACK/已读    │         │  定时消息     │
└──────────────┘              └──────────────┘         └──────────────┘
       │                              │                        │
       └──────────────────────────────┴────────────────────────┘
                    MySQL    Redis    RabbitMQ    Milvus
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
  Agent异步落库AI回复 + 更新Redis session + 异步向量写入Milvus
```

**语义搜索（RAG）**：
```
用户: "上次说的方案是什么" → Agent → Embedding
  → Milvus 向量检索(topK=10) → 回表 MySQL 取真实文本
  → LLM 汇总 → SSE 流式返回
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
│       ├── vector_consumer.go # 向量异步消费 + Milvus 写入
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
│   │   └── search_chat_history.go  # RAG 语义搜索
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
    ├── geeRPC/              # 自建 RPC 框架（一致性哈希负载均衡）
    ├── logger/              # Zap 日志
    ├── mq/                  # RabbitMQ 连接 + 发布/消费
    ├── proto/               # pb_msg / pb_user
    ├── utils/               # JWT / Bcrypt / Snowflake
    └── vector_db/           # Milvus 客户端单例
```

## 快速开始

### Docker Compose（推荐）

```bash
# 一键启动全部服务（基础设施 + 三微服务）
docker-compose up -d --build

# 查看服务状态
docker ps

# 查看某个服务日志
docker logs nexusgo-logic-1 -f
```

### 本地开发（三终端）

**前置条件**：Go 1.26+、MySQL 8.0+、Redis 6.0+、RabbitMQ 3.12+、Milvus 2.6+

```bash
# 修改 apps/config.yaml 中各服务地址和密钥

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
向量写入失败            → 仅影响语义搜索，不影响主链路
```

## 性能测试

```bash
# 在线单聊
k6 run perf/k6/ws_online_chat.js -e VUS=200 -e DURATION=30s

# AI 流式输出
k6 run perf/k6/ws_ai_stream.js -e VUS=2 -e DURATION=30s
```

## 部署

### 单机（Docker Compose）

```bash
docker-compose up -d --build
```

### 分布式水平扩展

- **Gateway**：Nginx `ip_hash` 前置（保证 WS 粘性） → 多实例，每个实例独立 Queue（`gateway.queue.<addr>`）
- **Logic**：多实例竞争消费 `logic.upload.queue`（天然负载均衡，直接翻倍吞吐）
- **Agent**：无状态（session 存 Redis），可独立扩缩容
- **MySQL/Redis/RabbitMQ**：建议集群 / 主从部署

## License

MIT
