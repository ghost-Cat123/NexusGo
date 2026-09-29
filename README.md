# NexusGo

基于 Go 的 Agent-Enabled 即时通讯系统。项目将 WebSocket 实时消息、RabbitMQ 异步链路、GrowRPC 服务发现与用户路由，以及带安全边界的 Agent 检索能力拆分为 Gateway、Logic、Agent 三个服务。

> 校招项目定位：展示可运行的 IM 消息链路、并发可靠性问题定位，以及 Agent Memory / RAG / 工具安全的工程化实现。性能数据和可靠性边界见下文，未将单机结果表述为分布式生产能力。

## 核心能力

- **实时 IM**：单聊、群聊写扩散、离线未读同步、已读回执、游标分页拉取历史消息。
- **异步消息链路**：Gateway 上行、Logic 落库与下行分发通过 RabbitMQ 解耦；消息持久化、Publisher Confirm、手动 ACK、死信队列和 MsgID 幂等共同实现至少一次处理与重复抑制。
- **服务路由**：GrowRPC + etcd 服务发现；Gateway 的用户态 RPC 使用 `userID` 作为一致性哈希路由键，在实例成员不变时将同一用户稳定路由到同一 Logic 实例。
- **Agent**：Eino ReAct 工具调用、SSE 流式输出、MCP Client 扩展；写操作通过工具分级和 HITL 审批隔离。
- **三层记忆**：Redis 短期窗口、MySQL 对话摘要、Milvus 长期向量记忆；重启时保留既有记忆集合。
- **消息检索**：会话/时间过滤的 Milvus 语义召回经分数门控后回表 MySQL；候选必须覆盖 Agent 提取的关键词，否则回退 MySQL FULLTEXT 或拒答，避免缺字段的相似消息误答。

## 架构

```text
Client (HTTP / WebSocket)
          |
          v
Gateway :8080  -- RabbitMQ upload -->  Logic :8001  -- RabbitMQ down --> Gateway
     |                 |                   |
     |                 |                   +-- MySQL / Redis / Milvus
     |                 +-- Publisher Confirm, manual ACK, DLX
     |
     +-- HTTP / SSE --> Agent :8050 -- Eino / MCP / Memory / RAG

Gateway -- GrowRPC + etcd + userID consistent hash --> Logic
```

### 服务职责

| 服务    | 职责                                                | 不承担的职责         |
| ------- | --------------------------------------------------- | -------------------- |
| Gateway | HTTP、JWT、WebSocket 连接、上/下行 MQ、SSE 转发     | 不直接访问业务数据库 |
| Logic   | IM 业务、持久化、离线同步、群聊写扩散、向量索引投递 | 不执行 LLM 推理      |
| Agent   | Agent 编排、Memory、检索工具、HITL、SSE 输出        | 不参与实时消息路由   |

### 普通消息端到端流程

```text
Sender WebSocket
  -> Gateway (Snowflake MsgID)
  -> RabbitMQ upload (persistent + Publisher Confirm)
  -> Logic (idempotent write / route lookup)
  -> RabbitMQ down (persistent + Publisher Confirm)
  -> Receiver Gateway
  -> Receiver WebSocket
```

`server_ack` 仅表示 Gateway 已收到 RabbitMQ 对上行发布的确认，不代表接收端已收到消息。接收端 WebSocket 写入后，Gateway 再通过 RPC 回写投递状态。

## 可靠性边界

| 环节                | 当前措施                                           | 边界                                                 |
| ------------------- | -------------------------------------------------- | ---------------------------------------------------- |
| Gateway -> RabbitMQ | 持久化消息、Publisher Confirm、5 秒确认超时        | Confirm 表示 Broker 接收，不代表消费者已完成业务处理 |
| MQ 消费             | 手动 ACK、死信队列、失败重试                       | 语义为至少一次，可能发生重复投递                     |
| 数据写入            | Snowflake MsgID、唯一性/幂等校验、批量失败降级单条 | 不宣称恰好一次；未使用 Outbox                        |
| 接收端投递          | Gateway 路由、WebSocket 推送、投递状态回写         | 客户端断线时由离线同步补偿                           |

## Agent 与检索设计

```text
Agent context = Redis 短期历史
              + MySQL 摘要
              + Milvus 用户隔离的长期记忆

chat-history query
  -> Milvus 语义召回
  -> 会话范围 / 群成员权限过滤 + 分数门控
  -> MySQL 回表 + 关键词证据校验
  -> 证据不足时回退 MySQL FULLTEXT
  -> LLM 汇总
```

工具按只读与写入风险分级。定时发送等写入类工具必须经 HITL 审批后才可继续执行；限流、工具结果裁剪和长输出续写用于控制恶意调用、上下文膨胀与响应截断。

## 压测口径与结果

`perf/k6/ws_true_qps.js` 使用 100 对收发用户（200 条 WebSocket 连接），每对约每 100 ms 发送一条消息，统计从发送端发出到接收端 WebSocket 收到同一 `MsgID` 的端到端时延。

| 环境               | 负载                       |      吞吐 | 端到端 P95 |
| ------------------ | -------------------------- | --------: | ---------: |
| Windows 单机单节点 | 100 对 / 200 WS，约 5 分钟 | 990 msg/s |     131 ms |

该结果是本地 Windows 环境的单节点 E2E 基线，不外推为 Linux、多节点或生产容量结论。运行前后应同时记录 `sent`、唯一接收数、重复数和缺失数；仅有客户端 ACK 或 `recvOK` 不能单独证明零丢失。

```bash
k6 run perf/k6/ws_true_qps.js
```

## 快速开始

### 前置依赖

- Go 1.26+
- MySQL、Redis、RabbitMQ、etcd、Milvus、MinIO
- Agent 功能额外需要模型与 Embedding 凭证

从 `apps/config.example.yaml` 创建本地 `apps/config.yaml` 并填入凭证。该文件被 Git 忽略；不要提交 API Key、数据库密码或 Coze 凭证。

先启动基础设施，再在三个终端分别启动服务：

```bash
go run ./apps/logic/main.go
go run ./apps/agent/main.go
go run ./apps/gateway/main.go
```

默认端口：Gateway `8080`、Logic RPC `8001`、Agent HTTP `8050`、MCP `8051`。

## 验证

```bash
go test ./...
go vet ./apps/gateway/... ./apps/pkg/mq/... ./apps/agent/memory/...
```

端到端验证应至少覆盖：注册/登录、两名用户 WebSocket 收发、离线同步、重复 MsgID 幂等、MQ 消费失败进入死信队列，以及写入类 Agent Tool 的审批拒绝路径。

### 消息检索测评

正式测评不使用早期 10 条冒烟集，而是使用两套版本化仿真自然聊天集：校准集 42 条消息 / 36 查询，独立留出集 30 条消息 / 34 查询。测试每次以隔离 `msg_id` 写入 MySQL 与 Milvus，运行真实 Embedding、会话与时间过滤、回表和清理流程。

```bash
NEXUSGO_RUN_CHAT_HISTORY_EVAL=1 go test ./apps/agent/eval -run '^TestChatHistoryRetrievalEvaluation$' -v -count=1
```

当前线上策略的回归门槛是 `R@5 >= 0.75`、`MRR >= 0.75`、无答案误召回为 `0`、会话/时间越界为 `0`。最近一次 Linux 容器实测：校准集 `R@5=0.766 / MRR=0.781 / P95=277.5ms`，留出集 `R@5=0.900 / MRR=0.900 / P95=249.8ms`，两集无答案误召回均为 `0`。留出集仍待二次人工审核，不能表述为生产数据或已人工盲审结果。完整的数据构建、标签边界、指标表和面试说明见 [docs/retrieval_evaluation.md](docs/retrieval_evaluation.md)。

## 目录结构

```text
apps/
  gateway/        HTTP / WebSocket / MQ producer-consumer / GrowRPC client
  logic/          IM business service / persistence / MQ consumers
  agent/          Eino Agent / SSE / Memory / RAG tools / MCP
  pkg/
    GrowRPC/      custom RPC, discovery and consistent-hash selection
    mq/           RabbitMQ topology, Publisher Confirm and consumers
    vector_db/    Milvus collections, embedding and retrieval
perf/k6/          WebSocket E2E load scripts
```

## 后续方向

- 固化 Linux 单机与多 Logic 实例的独立压测报告。
- 扩充人工标注的固定数据集，比较 FULLTEXT、Milvus 和 RRF 的 Recall@K、MRR、NDCG、HitRate。
- 引入未路由消息的 Return / Alternate Exchange 监控，以及可观测性指标。
