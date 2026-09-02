# NexusGo 项目介绍

---

## 简洁版（面试开场口语版，约 90 秒）

> 说完架构后主动抛出三个坑，引导面试官来问你，而不是被动等问题。

```
我这个项目叫 NexusGo，是我用 Go 从零独立完成的一套分布式微服务即时通讯系统。

系统拆成三个微服务：
- Gateway 专门管 WebSocket 长连接，它不查数据库、不做任何业务逻辑，
  只负责接收客户端消息、生成消息 ID，然后扔进 RabbitMQ；
- Logic 消费 MQ 上行消息，负责落库、查接收方路由、把消息推到下行 MQ，
  再由接收方的 Gateway 推给客户端；
- Agent 是独立的 AI 推理服务，基于字节跳动的 Eino 框架，
  用 SSE 长连接流式输出，和主链路完全隔离，互不干扰。

基础设施这块用了 MySQL 做消息持久化、Redis 做在线路由缓存、
RabbitMQ 做削峰、Milvus 做向量语义检索，
最后用 Docker Compose 把九个容器打包，一键部署。

整个项目都是我自己独立完成的，从表结构设计、接口开发、联调到容器化部署全流程都有。
过程中踩过几个比较有意思的坑：
  一是历史消息接口慢查询，我通过 EXPLAIN 定位到缺少联合索引，加了之后 RT 从百毫秒降到个位数；
  二是 RabbitMQ 手动 ACK 在多 worker 并发时出现乱序漏 ACK，
    改成 multiple=false 独立单条确认之后彻底解决；
  三是压测时消息偶发丢失，排查下来是批量 INSERT 整批失败时直接 Nack 了，
    我加了逐条降级 + 幂等检测的兜底逻辑；
  另外 Docker Compose 启动时序没配好，Logic 连不上 Milvus，
    加了 healthcheck 和 depends_on condition 解决的。
这几个问题都可以展开讲，您感兴趣哪个？
```

---

## 压测数据（面试话术版）

| 指标 | 结果 | 来源脚本 |
|------|------|---------|
| 并发用户对数 | 100 对（200 账号） | `ws_true_qps.js` VUS=100 |
| 消息吞吐量 | 990 msg/s | 100 对 × 每 100ms 一条 = 1000 目标，实测 990 |
| 端到端 P95 延迟 | 131ms | `true_e2e_ms` 指标 p(95) |
| 消息投递成功率 | 100% | `true_recv_ok` rate = 1.0 |
| AI 首字响应 (TTFT) | 483ms | `ws_ai_stream.js` `ai_first_chunk_ms` avg |

批量写入上线后 DB 写入 RT 降低约 80%，slow query 从高峰期数十条/秒降为 0。

---

## 压测数据详细说明（面试被追问时用）

### 1. 脚本逻辑是什么？100 人并发是怎么回事？

**用的是 `ws_true_qps.js`，核心逻辑：**

```
VUS=100 → 生成 100 对账号（200 个账号）
每对账号 = 1 个 Sender VU + 1 个 Receiver VU，共 200 个 VU

账号规划（完全隔离，互不干扰）：
  第 0 对：user_1001（sender）→ user_1002（receiver）
  第 1 对：user_1003（sender）→ user_1005（receiver）
  ...
  第 99 对：user_1199（sender）→ user_1200（receiver）

Sender：每隔 SEND_INTERVAL=100ms 发一条消息，消息体嵌入发送时间戳
Receiver：收到消息后用当前时间 - 消息体里的时间戳 = 真实端到端延迟
```

所以**「100 人并发」= 100 对用户同时互发消息**，不是 200 人各自发，而是 100 个真实的「发→收」链路同时运行。

### 2. QPS 是怎么定义的，为什么是 990？

**QPS（这里实际是 msg/s）的计算：**

```
理论值 = VUS 对数 × (1000ms / SEND_INTERVAL)
       = 100 对   × (1000ms / 100ms)
       = 100 × 10 = 1000 msg/s

实测值 = 990 msg/s
```

**为什么略低于理论值（990 vs 1000）？**

1. **网络抖动**：每次发送受 WebSocket 帧封装和本机网络栈影响，实际间隔略大于 100ms
2. **k6 定时器精度**：JavaScript 的 `setInterval` 在高并发下有几毫秒抖动
3. **Gateway 处理时间**：生成 Snowflake MsgID + 发布 MQ 需要时间，偶尔导致下一个 interval 稍微延迟

990/1000 = 99% 发送成功率，属于正常范围。

### 3. P95 延迟 131ms 是怎么测出来的？

**端到端延迟（E2E）的测量方式：**

```
Sender 发送时：将发送时间戳嵌入消息体
  消息体格式：msgTs:{时间戳}:rid:{本轮唯一ID}:p{对号}:s{序号}
  例如：msgTs:1718600001234:rid:Rabc123:p5:s42

Receiver 收到时：
  latency = Date.now() - parseInt(match[1])  // 当前时间 - 消息体里的发送时间
  e2eLatency.add(latency)

这个延迟覆盖了完整链路：
  Client发送 → Gateway接收 → RabbitMQ → Logic落库→路由 → MQ下行
  → 目标Gateway → WebSocket推送 → Receiver收到
```

P95=131ms 说明 95% 的消息在 131ms 内完成了全链路投递。

### 4. QPS 为什么这么「低」？

面试官可能会问：「1000 msg/s 看起来不高啊」，**诚实回答**：

**测试环境限制（Windows + Docker Desktop）：**
- 所有服务（Gateway、Logic、MySQL、Redis、RabbitMQ）都跑在同一台笔记本上的 Docker 容器里
- Docker Desktop on Windows 本质是 Hyper-V 虚拟机，网络栈有额外开销（Host↔VM 的 NAT 转发）
- k6 压测进程本身也在同一台机器上，CPU 资源存在竞争

**这不是系统能力上限，而是本地测试环境的瓶颈。** 估算裸机 Linux 部署时：
- 消除 Docker NAT 层开销：延迟降低 30~50ms
- CPU 不再被压测进程和服务进程共享：吞吐提升 3~5 倍
- **预估裸机 Linux 下可达 3000~5000 msg/s**

**1000 msg/s 这个数字的价值在于**：证明了全链路（WebSocket→MQ→落库→下行推送）在 100 并发下的**稳定性**——消息零丢失、延迟可控、系统无崩溃，而不是压出极限吞吐。

### 5. 消息投递成功率 100% 是怎么保证的？

脚本用了 RUN_ID 机制过滤历史消息污染：

```javascript
// 每轮测试生成唯一 RUN_ID（6位随机字母数字）
// Receiver 只接受包含本轮 RUN_ID 的消息，过滤掉：
//   1. SyncUnread 推来的历史积压消息
//   2. 上一轮测试遗留的消息
const match = msg.match(MSG_PATTERN);  // MSG_PATTERN = /msgTs:(\d+):rid:{runId}:/
if (!match) { msgSkipped.add(1); return; }  // 过滤掉
```

这保证了 `true_recv_ok` = 本轮发出的消息 / 本轮收到的消息，数据干净可信。

---

## 详细版（技术深度版）

### 一、项目背景与动机

传统 IM 项目普遍存在三类问题：
1. 单体架构下 AI 推理会阻塞消息主链路；
2. 消息在高并发下存在丢失、重复投递风险；
3. AI 模型无状态设计导致"失忆"，用户体验差。

NexusGo 的目标是从架构层面解决这三类问题，同时探索 AI Agent 在 IM 场景下的工程化落地。

---

### 二、整体架构

```
客户端 (WebSocket)
       │
       ▼
┌──────────────┐   MQ Upload   ┌──────────────┐   SSE    ┌──────────────┐
│   Gateway    │ ────────────▶ │    Logic     │ ◀─────── │    Agent     │
│   :8080      │               │    :8001     │  HTTP    │    :8050     │
│ Gin + WS     │ ◀──────────── │ GeeRPC + MQ  │          │ Eino + RAG   │
│ 连接管理      │   MQ Down     │ 落库 + 路由   │          │ 三层记忆      │
└──────────────┘               └──────────────┘          └──────────────┘
       │                              │                          │
       └──────────────────────────────┴──────────────────────────┘
                   MySQL · Redis · RabbitMQ · Milvus · MinIO · etcd
```

**职责边界设计**（关键）：
- Gateway 不查 DB、不做业务逻辑，只管连接和 MQ 发布
- Logic 不做 AI 推理，只做消息落库和路由
- Agent 不碰 MQ、不参与消息路由，纯 AI 推理

---

### 三、消息链路与可靠性机制

#### 上行链路（发送消息）
```
Client → Gateway(生成 Snowflake MsgID + server_ack) → MQ Upload Exchange
  → Logic(消费 → 批量落库 → 批量 Redis MGet 查路由 → MQ Down Exchange)
  → 目标 Gateway → WebSocket push → 接收方
```

#### 可靠性保障矩阵

## 实际故障
| 故障场景 | 保障机制 |
|---------|---------|
| 网络抖动消息丢失 | 手动 ACK，消息重回队列 |
| Logic 消费崩溃 | 未 ACK 消息重新投递 |
| 消息解析失败 | Nack → DLX 死信队列，可人工重处理 |
| 接收方离线 | DB 保底 + SyncUnread 上线全量拉取 |
| 重复消费 | Snowflake MsgID 唯一索引幂等去重 |
| DB 写入失败 | 批量降级单条，单条失败检测 Error 1062 幂等跳过 |

## 总体环节

|环节	 | 失败场景 |	处理方式 |	消息是否丢失 |
|------|----------|----------|--------------|
|[1] | 用户→Gateway	WS | 解析失败 |	打日志 return，不回错误帧 |	等客户端重发（无 server_ack 超时触发） |
|[2] | Gateway→MQ（上行） |	PublishUpload 失败 |	打日志 return，不发 server_ack |	客户端超时未收到 ack，自行重发 |
|[3] | MQ→Logic |	消费/DB 失败 |	Nack→重投/DLX，Snowflake 幂等 |	不丢（已讨论） |
|[4] | Logic→MQ（下行） |	PublishDown 失败 |	只打 Warn 日志，照样 ACK 上行消息 |	消息已落库，上线后 SyncUnread 兜底 |
|[5] | MQ→Gateway（下行消费） |	JSON 解析失败 |	Nack 不重入队，送 DLX |	解析失败的消息送 DLX |
|[5] | MQ→Gateway（下行消费） |	接收方不在本节点 |	直接 ACK（不重入队） |	已落库，SyncUnread 兜底 |
|[6] | Gateway→用户 |	SendMessage channel 满 |	80%水位打 Warn；100%满时丢弃 + 踢下线 |	丢弃（保护网关不卡死） |


#### 批量写入设计（核心优化）
```go
const (
    uploadWorkers   = 50
    batchSize       = 50  // 满 50 条触发
    batchFlushMs    = 30  // 最长等 30ms 兜底
    numBatchWorkers = 4   // 并行写库 worker
)
```
批量 INSERT + Redis `MGet` 批量查路由，相比单条处理，DB 写入 RT 降低 ~80%。

---

### 四、自建 RPC 框架（geeRPC）

基于 TCP + Protocol Buffers 实现：
- **服务注册**：Logic 启动时向注册中心注册地址
- **服务发现**：Gateway 启动时拉取 Logic 实例列表
- **一致性哈希负载均衡**：按 UserID 哈希，同一用户消息路由到同一 Logic 实例，避免跨实例查 Redis 路由表
- **连接复用**：客户端连接池，避免频繁建连

---

### 五、AI Agent 工程化

#### 5.1 Eino 框架 & ReAct 图

基于 CloudWeGo Eino 框架，使用 `adk.ChatModelAgent` + `adk.Runner` 构建 ReAct 循环：

```
用户输入 → Runner.Stream() → ChatModel → ToolCalls? ─Yes→ Tools执行 → 回送结果 → 继续推理
                                                    └─No→  SSE 流式输出
```

#### 5.2 工具安全分类 + HITL 审批中断

```go
// metadata.go：两级分类
var destructiveToolNames = map[string]bool{"schedule_message": true}
var readOnlyToolNames    = map[string]bool{"search_chat_history": true}
```

写入型工具执行前，`ApprovalMiddleware` 触发 `tool.StatefulInterrupt()`：
1. Agent 挂起，序列化当前工具参数，通过 SSE 推送 `ApprovalInfo` 给前端
2. 前端渲染确认框，用户点击 Approve/Reject
3. 携带 `ApprovalResult` 恢复 Agent，`Approved=true` 则继续执行，否则返回取消原因

这实现了真正的 Human-in-the-Loop，而不是简单的"问一下用户"。

#### 5.3 完整中间件 Pipeline

```
请求进入
  ├─ BeforeAgent:   AutoContinueMiddleware  注入续写伪工具
  ├─ BeforeModel:   TrimResultMiddleWare    裁剪过长工具返回
  ├─ AfterModel:    AutoContinueMiddleware  检测 finish_reason=length，注入续写 ToolCall
  ├─ WrapInvokable: ApprovalMiddleware      写入型工具 HITL 中断
  ├─ WrapModel:     SafeAgentMiddleware     模型调用 panic 兜底
  └─ WrapTool:      SafeAgentMiddleware     工具执行 panic 兜底
```

自动续写核心原理：检测到 `finish_reason == "length"` 时，向消息历史注入一个 `continue_output` 伪 ToolCall，迫使 LLM 继续输出，最多续写 3 次防止无限循环。

#### 5.4 配置热重载

```go
func init() {
    config.OnReload(ClearCachedRunner) // 配置变更 → 清除 Runner 缓存 → 下次请求自动重建
}
```

---

### 六、三层记忆体系

```
每次对话
    │
    ▼
短期记忆（Redis List，24h TTL）
    │  达到 WindowSize=4 条
    ▼
中期摘要（LLM 压缩 → Redis 原子替换 → MySQL 持久化）
    │  摘要通过 Embedding 向量化
    ▼
长期记忆（Milvus Collection: memory_vectors，按 user_id 标量过滤）
```

**注入顺序**（每次请求组装 fullHistory）：
1. Milvus 语义召回（相似度 ≥ 0.5 的历史摘要）→ SystemMessage
2. MySQL 最近一条中期摘要 → SystemMessage  
3. Redis 短期滑窗对话 → 完整消息历史

Redis 摘要替换使用 Lua 脚本保证原子性：
```lua
redis.call('LTRIM', KEYS[1], ARGV[2], -1)  -- 删旧消息
redis.call('LPUSH', KEYS[1], ARGV[1])       -- 头插摘要
redis.call('EXPIRE', KEYS[1], ARGV[3])      -- 刷新 TTL
```

---

### 七、双路 RAG 检索（历史搜索工具）

```
用户: "上次和张三聊数据库的内容"
         │
         ▼
第一级：MySQL FULLTEXT Boolean 模式
  WHERE MATCH(content) AGAINST('+数据库' IN BOOLEAN MODE)
  AND (sender_id=A AND receiver_id=B) OR (sender_id=B AND receiver_id=A)
         │ 无结果
         ▼
第二级（语义降级）：
  Embedding("数据库") → Milvus 向量检索（conv_id 标量过滤）
  → msg_id 列表 → 回表 MySQL 取真实消息文本
         │
         ▼
  LLM 总结 → SSE 流式返回
```

权限控制：搜索群聊历史前校验 `IsGroupMember`，防止越权查看。

---

### 八、SSE 流式推送

```
Gateway(WS 接收 AI 请求)
  → HTTP GET /agent/chat/sse?user_id=&message=
  → Agent 保持 SSE 长连接
  → 逐 token 推送 chunk → Gateway 读取 → WebSocket push 给客户端
  → 最终 ai_end 帧关闭流
```

相比原 Redis PubSub 方案：
- 无需维护 channel 订阅，无跨实例路由问题
- 天然背压控制（SSE 消费速度 = 推送速度）
- AI 首字响应延迟优化至 **483ms**

---

### 九、压测数据

| 指标 | 结果 |
|------|------|
| 并发用户 | 100 人 |
| 吞吐量 | 990 msg/s |
| 端到端 P95 延迟 | 131ms |
| 消息投递成功率 | 100% |
| AI 首字响应 (TTFT) | 483ms |

测试工具：k6（WebSocket 压测脚本）  
测试环境：Windows + Docker Desktop（Linux 裸机预计 3~5 倍）

---

### 十、容器化部署

```yaml
# docker-compose.yml 服务列表
services:
  gateway:   nexusgo-gateway   :8080   # 网关
  logic:     nexusgo-logic     :8001   # 业务逻辑
  agent:     nexusgo-agent     :8050   # AI Agent
  mysql:     mysql:8.0         :3306   # 关系数据库
  redis:     redis:7           :6379   # 缓存
  rabbitmq:  rabbitmq:3-mgmt   :5672   # 消息队列
  milvus:    milvusdb/milvus   :19530  # 向量数据库
  minio:     minio/minio       :9000   # 对象存储
  etcd:      quay.io/coreos/etcd       # Milvus 元数据
```

Dockerfile 使用多阶段构建（builder + alpine），最终镜像 < 20MB，设置 `GOPROXY=https://goproxy.cn` 保障国内构建速度。

---

*技术栈：Go 1.26 · Gin · Eino · RabbitMQ · MySQL · Redis · Milvus · geeRPC · Protocol Buffers · Docker Compose*
