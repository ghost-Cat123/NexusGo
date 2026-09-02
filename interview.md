# NexusGo 面试题库

> 针对得物「中间件 AI 开发工程师（Golang方向）」岗位，结合项目实际代码整理

---

## Part 1：项目综合问题

### Q1：介绍一下你这个项目，为什么要做成微服务？


**答：**  
NexusGo 是一个分布式 IM 系统，核心是把消息链路、业务逻辑、AI 推理三个职责拆成独立微服务。

拆分原因：
- **AI 推理会阻塞消息主链路**：如果 Gateway 直接调 LLM，一次推理 3~5 秒，期间整个 WS 连接被占满。拆出 Agent 服务后，AI 请求走独立 HTTP SSE，不影响普通消息路由。
- **扩容粒度不同**：Logic（消费 MQ）是 CPU 密集型，可以多实例水平扩展；Agent（LLM 调用）是 IO 等待型，扩容策略完全不同。
- **职责边界清晰**：Gateway 不查 DB，保证它是无状态的，任何实例都可以处理任何连接。

---

### Q2：消息从发送到接收，完整链路是什么？

**答：**
```
1. Client 发 WebSocket 消息 (chat_type=single_chat)
2. Gateway 接收，生成 Snowflake MsgID，立即回 server_ack（告知客户端已收到）
3. Gateway Publish 到 RabbitMQ Upload Exchange (fanout)
4. Logic 消费 Upload Queue：
   a. 解析 payload
   b. 放入 batchCh channel
   c. batchInsertWorker 积满 50 条或 30ms，批量 INSERT MySQL
   d. 批量 Redis MGet 查接收者路由（gateway 地址）
   e. Publish 到 RabbitMQ Down Exchange（per-gateway queue）
5. 目标 Gateway 消费自己的 Down Queue，查 CliMap 找 WS 连接，推 chat_push
6. 接收方看到消息，发 ack，Logic 更新 is_read
```

---

### Q3：如果 Logic 消费到一半崩溃了，消息会丢吗？

**答：** 不会丢。

RabbitMQ 手动 ACK 模式：消息被取走后，Logic 没有显式调用 `d.Ack()` 的话，MQ 认为消息未被处理，重新投递给其他消费者（或等 Logic 重启后再次消费）。

批量写入场景的额外保障：
- 批量 INSERT 成功后，对每条消息独立 `d.Ack(false)`（`multiple=false`），不用 `multiple=true`，避免多 worker 并行时 delivery tag 乱序导致漏 ACK
- 批量 INSERT 失败时降级逐条单插，失败的单条送死信队列（`d.Nack(false, false)`），不影响其他消息

---

### Q4：Snowflake 幂等是怎么实现的？消息重复消费会怎样？

**答：**  
MySQL `messages` 表对 `msg_id` 建了唯一索引。MQ 重投导致同一条消息被重复消费时，第二次 INSERT 会触发 Error 1062 主键冲突。

代码里检测到 `isDuplicateKey` 后走幂等分支：视为成功，照样推下行 + ACK，不报错、不重入队。

```go
func isDuplicateKey(err error) bool {
    return strings.Contains(err.Error(), "1062") ||
        strings.Contains(err.Error(), "Duplicate entry")
}
```

---

### Q5：一致性哈希负载均衡是怎么做的？为什么用一致性哈希？

**答：**  
用 UserID 作为哈希 key，在一致性哈希环上找到对应的 Logic 节点。

用一致性哈希而不是轮询的原因：**状态亲和性**。同一个 UserID 的消息路由到同一个 Logic 实例，该实例可以缓存该用户的在线路由信息（Redis 查询结果），避免每条消息都去 Redis 查一次。如果用轮询，同一用户的连续消息可能被分散到不同 Logic 实例，缓存命中率极低。

新增/删除 Logic 实例时，一致性哈希只有少量 key 重新分配，比普通取模稳定得多。

---

## Part 2：消息队列深挖

### Q6：为什么用 RabbitMQ 而不是 Kafka？

**答：**  
IM 场景的特点：
- 消息量级中等（单机千级 QPS），不需要 Kafka 的百万级吞吐
- 需要**死信队列**（处理失败消息）和**延迟队列**（预约消息）
- 消费端需要**细粒度 ACK**（单条确认，不是 offset 提交）

Kafka 的 offset 提交模式在"某条消息失败、其他消息继续"的场景下非常难处理；RabbitMQ 的手动单条 ACK/Nack 完美契合 IM 消息的可靠性需求。

如果未来需要日志流水、事件溯源等场景，可以补充引入 Kafka。

---

### Q7：死信队列（DLX）是怎么配置的，什么情况下消息会进死信队列？

**答：**  
三种情况消息进 DLX：
1. 消息被 `Nack(false, false)`（requeue=false）
2. 消息在队列中超过 TTL
3. 队列满了溢出

配置方式（在声明业务队列时绑定）：
```go
args := amqp.Table{
    "x-dead-letter-exchange":    "dlx.exchange",
    "x-dead-letter-routing-key": "dead.messages",
}
```

项目里主要用场景是 Logic 消费解析失败（JSON 格式错误等），这类消息没有重试价值，直接送 DLX，后续可以人工查看和处理。

---

### Q8：MQ 积压了怎么处理？

**答：**  
短期（紧急）：
1. 临时增加 Logic 消费实例（MQ 竞争消费，天然负载均衡）
2. 增大 `uploadWorkers` 和 `numBatchWorkers`

中期（优化）：
1. 识别积压根因：通过 MQ 管理界面看 `messages ready` 指标，确认是消费端慢还是生产端爆
2. 如果是 DB 写入慢：调大连接池，或用更大的 `batchSize`
3. 如果是下行推送慢：解耦消费和推送，消费完先 ACK，推送失败不影响消费进度

长期：消息分级，AI 消息走低优先级队列，普通消息走高优先级队列，优先保障实时聊天体验。

---

## Part 3：AI Agent 工程化深挖

### Q9：HITL 审批中断机制是怎么实现的？

**答：**  
基于 Eino ADK 的 `tool.StatefulInterrupt` 实现，核心是将 Agent 执行状态持久化后挂起。

流程：
```
1. ApprovalMiddleware.WrapInvokableToolCall 包装写入型工具
2. 第一次进入：检测 IsDestructiveTool → 触发 StatefulInterrupt(ctx, ApprovalInfo, storedArgs)
3. Agent 状态序列化到 CheckPointStore（内存/Redis）
4. SSE 推送 interrupt 事件给前端（含工具名、参数 JSON）
5. 前端展示确认框，用户选择 Approve/Reject
6. 前端携带 ApprovalResult 调用 Resume 接口
7. Runner.Resume() 恢复状态，re-enter 工具调用
8. GetResumeContext 拿到 ApprovalResult，Approved=true 则执行工具，否则返回取消消息
```

关键点：`storedArgs` 在中断时保存，恢复时传给真实工具，保证参数一致性。

---

### Q10：自动续写中间件为什么要用"伪工具调用"而不是直接再次调用 LLM？

**答：**  
因为 Eino Agent 的执行流程是由 `finish_reason` 驱动的：
- `stop`：结束
- `tool_calls`：执行工具，然后继续推理
- `length`：截断，正常情况下会停止

如果直接再调用 LLM，需要在中间件外部手动管理状态，破坏了 Agent 的状态机。

用伪工具的方式更优雅：把 `finish_reason` 改成 `tool_calls`，注入一个 `continue_output` ToolCall，Agent 的状态机认为还有工具要执行，自然地继续推理循环，从截断位置续写，且不重复已有内容。

---

### Q11：TrimResult 中间件是在什么时机裁剪的？裁剪后 LLM 看到的是什么？

**答：**  
在 `BeforeModelRewriteState`（每次喂给 LLM 之前）裁剪。

**裁剪条件（两步判断）：**

第一步：从后往前扫消息历史，找最后一条 assistant **有文本内容**的消息位置（`lastTextIdx`）。

- 如果找不到（`lastTextIdx < 0`）：说明 LLM 还没生成过文本回答，整个工具链还在"活跃调用中"（比如刚调完 search，还没生成回答），**不裁剪** —— 此时 LLM 还需要完整看到工具结果。
- 如果找到：说明 LLM 已经基于工具结果生成了文本回答，该结果是"已消费"的，可以裁剪。

第二步：把 `lastTextIdx` **之前**的 noisy 工具结果内容，直接替换为占位符：

```go
msg.Content = fmt.Sprintf("[result omitted] (~%d chars)", len(msg.Content))
```

**`[result omitted]` 不是让 LLM 压缩，而是中间件自己直接做的替换。** LLM 下次被调用时看到的是这个短占位符（约 30 字），而非原来的 5000 字工具结果。LLM 知道"这里有个工具被调用过、返回了约 N 字的内容"，不需要重新处理，只是减少了 Context token 消耗，防止多轮 ReAct 后 Context 溢出。

**时序示例：**
```
轮次1: [user问题] → [search返回5000字] → [assistant文本回答]
                                              ↑ lastTextIdx
轮次2 BeforeModel触发裁剪:
      [user问题] → [result omitted (~5000 chars)] → [assistant文本回答] → [user新问题]
      → 喂给LLM（节省5000字token）
```

> ⚠️ **代码细节**：循环中 `msg := state.Messages[i]` 拿到的是结构体副本，`msg.Content = ...` 修改的是副本。如果 `adk.Message` 是值类型，这里实际上不会修改原始 state（静默失效）。正确写法应为 `state.Messages[i].Content = ...`。需确认 Eino 里 `Messages` 是 `[]Message` 还是 `[]*Message`。


### Q12：三层记忆中，摘要写入 Redis 为什么用 Lua 脚本？

**答：**  
摘要替换需要三步原子操作：
1. `LTRIM`：删除已被压缩的旧消息
2. `LPUSH`：在头部插入摘要占位消息
3. `EXPIRE`：刷新 TTL

如果用普通命令分三次发送，在高并发场景下可能出现：
- LTRIM 完成后，另一个 goroutine 又 RPush 了新消息进来
- LPUSH 还没执行，EXPIRE 已经跑了

Lua 脚本在 Redis 中是原子执行的（单线程），三步操作不可被打断，彻底避免竞态条件。

---

## Part 4：分布式系统通识

### Q13：Redis 做在线路由表，key 是什么？会过期吗？用户下线怎么处理？

**答：**  
Key 格式：`route:user:{userID}` → Value：Gateway 的地址（`host:port`）

生命周期：
- 用户 WS 连接建立时 `SET route:user:xxx gatewayAddr EX 86400`
- 心跳每次刷新 TTL（防止僵尸 key）
- 用户主动断开或心跳超时，`DEL route:user:xxx`

接收方不在线时（key 不存在），Logic 跳过下行推送，消息已落 DB。用户重新上线时，Gateway 的 `SyncUnread` RPC 调用 Logic 全量拉取未读消息。

---

### Q14：WebSocket 心跳是怎么做的？服务端如何检测僵尸连接？

**答：**  
客户端定时发 `{chat_type: "ping"}` 消息，服务端 `ReadPump()` 收到后做两件事：
1. `Expire(redisKey, 30s)`：刷新 Redis 路由 key 的 TTL
2. 回 `{"action":"pong"}` 给客户端

**超时检测的实现方式**：没有额外的全局扫描 goroutine，而是把超时语义**外包给 Redis TTL**：
- 客户端每次 ping → Redis TTL 刷新为 30 秒
- 如果 30 秒内没有 ping → Redis key 自动过期 → Logic 查不到路由 → 认为用户离线，下行消息停止推送

**WS 连接本身的回收**：依赖 `ReadPump` 里 `ReadMessage()` 返回错误（TCP 断开/超时/客户端关闭），触发 `defer Remove()`，从 `CliMap` 删除连接并 `DEL` Redis 路由 key：

```go
// manager.go
func (m *CliMap) Remove(key string, newClient *Client) {
    if m.Clients[key] == newClient {  // 防止新连接误删旧连接
        delete(m.Clients, key)
        cache.GetCache().Del(ctx, "route:user:"+key)
    }
}
```

这是应用层心跳，比 TCP keepalive 更可靠（TCP keepalive 只能检测 TCP 层断开，检测不到应用层假死）。

---

### Q15：如果两个 Gateway 实例，同一个 UserID 可能连接到不同 Gateway，怎么处理消息路由？

**答：**  
每个 Gateway 实例启动时，在 RabbitMQ 中声明一个**以自身地址命名的独占队列**（`gateway.queue.{addr}`），绑定到 Down Exchange。

Logic 下行推送时，从 Redis 查出接收者所在的 `gatewayAddr`，用这个地址作为 routing key 发布消息，只有对应 Gateway 实例的队列会收到这条消息，精确路由。

这样不需要任何全局广播，每个 Gateway 只消费发给自己实例的消息。

---

## Part 5：Go 并发与底层

### Q16：你的批量写入用到了哪些 Go 并发原语？

**答：**  
```go
batchCh := make(chan batchItem, batchSize*uploadWorkers*2) // buffered channel
```

- `chan` 作为生产者-消费者队列，50 个 upload worker goroutine 往 batchCh 写，4 个 batchInsertWorker goroutine 从 batchCh 读
- `time.NewTicker` 做定时兜底（30ms）
- `select` 同时监听 batchCh 和 ticker.C，哪个先来处理哪个
- 不需要 Mutex，channel 本身是并发安全的

**channel 容量设计**：`batchSize * uploadWorkers * 2 = 50 * 50 * 2 = 5000`，留足缓冲防止 upload worker 在 batchInsertWorker 慢时阻塞。

---

### Q17：Go GMP 模型中，你的 50 个 goroutine 会对应多少个线程？

**答：**  
不是 1:1 对应。GMP 中 M（线程）数量由 `GOMAXPROCS` 控制（默认等于 CPU 核数）。

50 个 goroutine 会被调度到有限的 M 上：
- **G 可运行**：放在 P 的本地队列，等待 M 执行
- **G 阻塞在系统调用**（如文件 IO）：对应 M 会被剥离，新 M 绑定 P 继续调度其他 G
- **G 阻塞在 channel**（纯 Go 层面的阻塞）：不会真正阻塞 M，只是 G 状态变为 waiting，M 去执行其他 G

我的 batchInsertWorker 主要阻塞在 channel 读取和 MySQL 网络 IO，前者是纯 Go 阻塞（不占 M），后者是系统调用（会触发 M 剥离），整体上 50 个 goroutine 实际占用的 M 远少于 50。

---

### Q18：为什么 Runner 要用双重检查锁（DCL）？`sync.Once` 不够吗？

**答：**  
```go
// 快速路径（不加锁）
if cacheRunner != nil { return cacheRunner, nil }

// 慢速路径（加锁 + 再次检查）
runnerMu.Lock()
defer runnerMu.Unlock()
if cacheRunner != nil { return cacheRunner, nil }
runner, err := buildRunner(ctx)
cacheRunner = runner
```

`sync.Once` 的问题：`buildRunner` 可能失败，`Once` 执行后不会再次尝试，导致一次失败后永远无法创建 Runner。

DCL 的优势：
- 热路径（已有缓存）完全无锁，性能极高
- 失败时 `cacheRunner` 保持 nil，下次请求会再次尝试创建

热重载时调用 `ClearCachedRunner()` 把 `cacheRunner` 置 nil，下次请求自动触发重建。

---

## Part 6：向量数据库 & RAG

### Q19：Milvus 的向量检索原理是什么？你用的是什么索引？

**答：**  
Milvus 的核心是 ANN（近似最近邻）搜索，不是精确搜索（精确搜索 O(n) 太慢）。

常见索引类型：
- **IVF_FLAT**：先 K-Means 聚类，搜索时只在最近的几个簇内暴力搜索，速度/精度可调
- **HNSW**（Hierarchical Navigable Small World）：多层图结构，搜索时从稀疏的上层快速定位，再到稠密的下层精确搜索，速度极快，精度高，是目前最常用的

项目中实际配置取决于集合创建时的 `index_params`，消息向量集合使用的是 Milvus 默认的 AUTOINDEX，生产环境建议显式指定 HNSW。

相似度计算：Inner Product（内积）或 L2 欧氏距离，Embedding 模型归一化后两者等价。

---

### Q20：RAG 双路召回（关键词 + 语义）是怎么融合结果的？

**答：**  
我的实现是**串行降级**而非并行融合：

```
MySQL FULLTEXT 关键词搜索
    ↓ 有结果 → 直接返回（精确度高，优先）
    ↓ 无结果
Milvus 语义搜索 → msg_id → 回表 MySQL 取真实文本
    ↓
LLM 总结
```

选择串行降级而非并行的原因：
1. FULLTEXT 精确匹配结果质量更高，有结果时语义召回是冗余的
2. 两路结果合并需要去重和排序，增加复杂度
3. Milvus 搜索有延迟，不必要时不调用

更完善的方案（如果需要更高召回率）：并行搜索，用 RRF（Reciprocal Rank Fusion）融合两路排名，得物内部可能就是这么做的。

---

## Part 7：对应 JD 的扩展问题

### Q21：你了解 MCP（Model Context Protocol）吗？和你项目里的 Tool 调用有什么关系？

**答：**（见 supplement.md 详细展开）

MCP 是 Anthropic 提出的开放协议，标准化了 AI 模型和外部工具/数据源的交互方式。

类比：
- 你的项目：Agent 通过 Eino 框架调用 `search_chat_history`、`schedule_message` 这些内置 Go 函数
- MCP：把这些工具变成独立进程（MCP Server），AI 模型（MCP Client）通过标准化 JSON-RPC 协议调用它们，实现**工具解耦和跨语言复用**

你项目里最接近 MCP 的设计：`metadata.go` 里的工具安全分类（readOnly/destructive），这和 MCP 的工具能力声明理念一致。未来可以把 `search_chat_history` 包装成 MCP Server，让任何 MCP Client（不只是你的 Agent）都能调用。

---

### Q22：LLM Gateway 你了解吗？你项目里哪个部分最接近这个概念？

**答：**  
LLM Gateway 是 AI 基础设施层的核心组件，职责包括：
- **请求路由**：把上游请求分发到不同 LLM Provider（OpenAI/Claude/DeepSeek）
- **负载均衡**：多 API Key 轮询，防止单 Key 限流
- **限流熔断**：保护下游 LLM Provider
- **可观测性**：Token 计数、延迟追踪、费用统计
- **缓存**：相同 prompt 命中缓存，节省成本

你项目里的 Agent 服务承担了部分 LLM Gateway 的职责：
- `RateLimitMiddleware`：限流
- `ModelRetryConfig`：检测 429 错误自动重试（等同于熔断重试）
- `trace_logger.go`：Token/TTFT 追踪

得物的 JD 里提到「大模型 API Gateway」，面试时可以从这个角度切入，说明你理解 LLM Gateway 的核心价值，并且已经在项目中实现了其中的关键模块。

---

### Q23：Service Mesh 是什么？你的 geeRPC 和 Service Mesh 有什么区别？

**答：**  
Service Mesh 是把服务间通信的治理能力（负载均衡、熔断、重试、可观测性）从业务代码里剥离出来，下沉到基础设施层（通常是 Sidecar 代理，如 Envoy）。

| 对比维度 | geeRPC（你的项目） | Service Mesh（Istio + Envoy） |
|---------|-----------------|------------------------------|
| 实现位置 | 业务代码内（Go 库） | Sidecar 进程（与业务解耦） |
| 语言绑定 | Go 专用 | 语言无关 |
| 运维复杂度 | 低 | 高（需要 K8s） |
| 功能范围 | 负载均衡、服务发现 | 全量治理（mTLS、流量镜像、A/B测试等） |

你的 geeRPC 是「胖客户端」模式，Service Mesh 是「Sidecar」模式，两者不是替代关系，而是不同规模下的不同选择。

---

### Q24：CozeLoop 是什么？你是如何接入的？

**答：**  
CozeLoop 是字节跳动旗下的 LLMOps（大模型运维）平台，提供：
- **Trace 追踪**：每次 LLM 调用的输入/输出/延迟/Token 消耗全链路可视化
- **Prompt 管理**：版本化管理 System Prompt，支持 A/B 测试
- **评估**：自动化评估模型输出质量

项目接入方式：创建cozeloop客户端，直接将客户端注册为全局callback，每次 LLM 调用前后触发回调，上报 Token 数、TTFT、延迟等指标到 CozeLoop 平台。这是无侵入式接入，不修改任何业务逻辑。
```
// 创建 CozeLoop 客户端
client, err := cozeloop.NewClient(
    cozeloop.WithAPIToken(apiToken),
    cozeloop.WithWorkspaceID(workspaceID),
)

// 注册为全局 Callback
callbacks.AppendGlobalHandlers(clc.NewLoopHandler(client))
```

---

## Part 8：小厂专项面试话术（飞机管控 / 售后 / 设备运维方向）

> 本节针对「飞机管控、设备售后、仿真数据处理」类小厂岗位，将 NexusGo 项目经历转化为该岗位最关心的维度来表达。面试官重点考察：项目独立度、DB 优化、故障排查、AI Coding 观念。

---

### 〇、开场自我介绍脚本（面试官友好版，约 90 秒）

> **策略**：先用一句话说清每个微服务的职责，再主动抛 4 个具体技术坑，引导面试官来追问你最擅长的方向。

```
我这个项目叫 NexusGo，是我用 Go 从零独立完成的分布式微服务即时通讯系统。

系统拆成三个微服务：
  Gateway  —— 专管 WebSocket 长连接，不查数据库、不做业务逻辑，
              只接收客户端消息、生成 Snowflake MsgID，然后扔进 RabbitMQ；
  Logic    —— 消费 MQ 上行消息，落库、查接收方路由、推下行 MQ，
              再由接收方 Gateway 推给客户端；
  Agent    —— 独立 AI 推理服务，基于字节跳动 Eino 框架，
              用 SSE 流式输出，和主链路完全隔离，互不干扰。

基础设施这块：MySQL 做消息持久化、Redis 做在线路由缓存、RabbitMQ 做削峰、
Milvus 做向量语义检索，用 Docker Compose 把九个容器打包，一键部署。

整个项目我从表结构设计、接口开发、联调到容器化部署都是自己独立完成的。
过程中踩过几个比较有意思的坑——

  第一个：历史消息查询接口很慢，我用 EXPLAIN 分析 SQL 发现走了全表扫描，
    定位到缺少联合索引，加上 (receiver_id, is_read) 之后 RT 从百毫秒降到个位数毫秒；

  第二个：RabbitMQ 手动 ACK 在多个并行 worker 消费时出现漏 ACK，
    原因是 multiple=true 在多 worker 场景下 delivery tag 乱序，
    改成 multiple=false 独立单条确认之后彻底解决；

  第三个：压测时消息偶发丢失，排查是批量 INSERT 整批失败时直接 Nack 了，
    我加了降级逐条单插 + 幂等检测，正常消息不受影响；

  第四个：Docker Compose 启动时序没配好，Logic 连不上 Milvus，
    因为 Milvus 自身依赖 etcd 和 MinIO 初始化，
    给 MySQL 加 healthcheck、给 Milvus 配 depends_on 之后解决了。

这几个问题都可以展开细讲，您对哪个更感兴趣？
```

---

### Q25：你说是独立完成的，哪些地方体现了你的独立判断？

**答（话术）：**

独立性主要体现在三个层面：

**1. 架构选型判断**：最开始 AI 推理放在 Gateway 里，一次 LLM 调用 3~5 秒，整个 WebSocket 连接被卡死，普通用户发消息也受影响。我自己分析出根因是「AI 是 IO 等待密集型，消息路由是计算轻量型，混在一起互相拖垮」，于是把 Agent 拆成独立微服务，用 HTTP SSE 独立通道推流，主链路完全不受影响。这个决策没有任何人告诉我，是我自己看了瓶颈数据之后判断的。

**2. 数据库方案的取舍**：批量写入这块，我最初是每条消息来一条 INSERT，压测 200 并发时 MySQL 写入就成了瓶颈（QPS 撑不住，连接池饱和）。我自己设计了「双触发批量写入」：积满 50 条或超过 30ms 就触发一次 `INSERT ... VALUES (),(),()`，大幅降低数据库连接压力。这个 30ms 的阈值也是我自己测出来的——时间太长低流量场景消息延迟高，太短批次太小效果不好，30ms 是最佳平衡点。

**3. 幂等和容错设计**：批量 INSERT 失败时，我没有直接全部 Nack 扔回队列（那会导致所有消息重试，其中正常的也被反复重处理）。我自己想到「降级逐条单插」策略，失败的单条才送死信队列，其他成功的正常 ACK 推下行，最大程度保留正常消息，隔离脏数据。

---

### Q26：说说你项目里的慢 SQL 排查经历和数据库优化？

**答（话术，按链路讲最有说服力）：**

---

#### 链路一：历史消息慢查询 → EXPLAIN 分析 → 联合索引优化

**发现时机**：我在做历史消息列表接口时，本地数据量小的时候没问题，压测插入约 5 万条消息后，调「查某用户未读消息」接口，RT 直接从几毫秒飙到两三百毫秒。

**排查过程**：
```sql
-- 先用 EXPLAIN 看执行计划
EXPLAIN SELECT * FROM messages
WHERE receiver_id = 123 AND is_read = false
ORDER BY msg_id DESC LIMIT 20;
```
EXPLAIN 结果的 `key` 列显示 `NULL`——走的是全表扫描，`rows` 扫了几万行。

**根因**：`receiver_id` 有单独索引，但 `is_read` 没有，两个条件组合时 MySQL 选择了全表扫，而不是走 `receiver_id` 索引——因为 `is_read = false` 这个条件过滤掉的数据太少，优化器认为回表代价太高。

**解决方案**：在 GORM 的 Model 里加联合索引：
```go
// messages.go
ReceiverId int64 `gorm:"index;index:idx_receiver_read,priority:1;column:receiver_id"`
IsRead     bool  `gorm:"index:idx_receiver_read,priority:2;default:false;column:is_read"`
```
加上 `(receiver_id, is_read)` 联合索引后，再 EXPLAIN：`key` 显示 `idx_receiver_read`，`rows` 从几万降到个位数，RT 回到毫秒级。

**延伸：游标分页（避免 OFFSET 深翻页）**

历史消息翻页接口用的是游标分页：
```go
// dao/messages.go — GetChatHistory
query := db.GetDB().
    Where("((sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?))",
        userID, targetID, targetID, userID).
    Where("msg_id < ?", cursor).    // 游标：只取比这条更早的消息
    Order("msg_id DESC").
    Limit(20)
```

**面试官追问：msg_id 是雪花算法生成的，不连续，这样也能用游标分页吗？**

**可以，完全没问题。** 理解误区在于：游标分页的核心是「定位」，不需要 ID 连续。

雪花算法生成的 ID 有两个关键性质：
1. **全局唯一**：跨节点不重复，可以做主键
2. **单调递增（时间有序）**：高 41 位是时间戳，后生成的 ID 一定大于先生成的

所以 `msg_id < cursor` 的语义是**「时间上早于这条消息的所有消息」**，和 ID 有没有空洞完全无关：

```
实际 msg_id 可能是：
  ...→ 782300000001 → 782300004521 → 782300004899 → 782300009012 → ...
         ↑ 有跳跃，但单调递增

cursor = 782300009012，查询返回所有 msg_id < 782300009012 的该会话消息
→ MySQL 在聚簇索引（主键 B-Tree）里二分定位到 cursor，然后向左扫 20 条
→ 跳过的空洞在 B-Tree 里本来就不存在，扫描时自然跳过，没有任何开销
```

**对比 OFFSET 深翻页：**

```sql
-- OFFSET 方式（有性能问题）
SELECT * FROM messages
WHERE ((sender_id=A AND receiver_id=B) OR ...)
ORDER BY msg_id DESC
LIMIT 20 OFFSET 10000;
-- → MySQL 必须先扫出 10020 条满足 WHERE 条件的行，扔掉前 10000 条，返回后 20 条
-- → 第几页决定扫多少行，翻页越深越慢，O(页数 × 每页大小)

-- 游标方式（我的实现）
SELECT * FROM messages
WHERE ((sender_id=A AND receiver_id=B) OR ...)
AND msg_id < 782300009012
ORDER BY msg_id DESC
LIMIT 20;
-- → MySQL 用主键聚簇索引，先二分找到 cursor 的位置，再向左扫满足 WHERE 的 20 条
-- → 成本与翻到第几页无关，始终 O(每页大小)
```

**一个真实的注意点**：这里的 WHERE 条件是 OR（`sender_id=A AND receiver_id=B OR sender_id=B AND receiver_id=A`），MySQL 优化器可能选择走主键而非 sender_id/receiver_id 索引——这取决于会话消息的密度。如果要进一步优化，可以加 `(sender_id, receiver_id, msg_id)` 复合索引，让过滤和排序都走同一个索引，消除 filesort。

---

#### 链路二：批量写入优化——四层递进

**背景**：压测 200 VU 并发发消息，Logic 的 MySQL 写入成了瓶颈——slow log 里大量单条 INSERT，RT 飙到 50ms+，连接池 100 个连接全部打满。

**第 1 层：批量 INSERT（核心）**

把单条 `INSERT` 改成攒批：满 50 条或超过 30ms 就触发一次 `INSERT INTO messages VALUES (...),(...),(...)...`
50 条消息一个网络 round-trip 完成，连接池压力降低约 50 倍，slow query 消失。

30ms 这个阈值是我测出来的：太长低流量场景消息延迟高（用户感觉"发出去没反应"），太短批次太小优化效果差，30ms 是最佳平衡点。

```go
ticker := time.NewTicker(30 * time.Millisecond)
for {
    select {
    case item := <-batchCh:
        buf = append(buf, item)
        if len(buf) >= 50 { flush() }  // 满 50 条立刻刷
    case <-ticker.C:
        flush()                          // 30ms 超时兜底
    }
}
```

**第 2 层：单条 INSERT 降级兜底（批量失败时）**

批量 INSERT 如果整批失败（比如批次里有一条重复 MsgID），不能把整批直接 Nack 扔回队列——那样正常消息也被反复重试。

我的处理是：批量失败 → 降级逐条单插 → 每条独立判断：
- 是 `Error 1062 Duplicate entry`（重复键）→ 幂等跳过，视为成功
- 是其他真实错误（磁盘满、连接断）→ `d.Nack(false, false)` 送死信队列

```go
if isDuplicateKey(sErr) {
    // 幂等跳过，推下行 + ACK
    go pushDownAndAckWithAddr(it, addr)
} else {
    // 真实错误，送死信
    _ = it.d.Nack(false, false)
}
```

**第 3 层：Redis MGet 批量查路由（减少 Round-Trip）**

批量写入的同时，我用一次 `Redis MGet` 拿出这批消息所有接收者的路由地址，而不是每条消息单独 `Redis Get`：
```go
// 50 条消息可能只有 5~10 个不同接收者
// 一次 MGet 搞定，比 50 次 Get 省去大量网络 RTT
vals, err := cache.GetCache().MGet(ctx, redisKeys...).Result()
```

**第 4 层：4 个并行 batchInsertWorker**

单个 worker 写库能力有限，用 4 个并行 worker 同时消费 `batchCh`，消耗能力是单个的 4 倍。channel 本身是并发安全的，不需要额外加锁。

**最终效果**：批量写入上线后，DB 写入 RT 降低约 80%，slow query 从高峰期数十条/秒降为 0，消息吞吐达到 990 msg/s。

---

### Q27：你项目里出现过什么问题？是怎么发现的，怎么排查的？

**答（话术，按下面顺序讲，每个给 1~2 分钟）：**

---

#### 【问题 1】RabbitMQ 手动 ACK 在多 Worker 并发时出现乱序漏 ACK

**现象**：联调阶段所有日志正常，但打开 RabbitMQ 管理界面，`Unacked`（未确认消息数）一直在涨，重启 Logic 进程后才归零。说明有大量消息永远卡在 `Unacked` 状态——既没被 ACK，也没被 Nack 重投，相当于「悬空」了。

**排查过程**：

先看 Logic 日志，`flush()` 每批写库都有执行、没有 panic，消息确实被处理了。

加日志在 `flush()` 里打印每条消息的 `delivery_tag` 和 ACK 时间点，发现 **tag 有跳号**——比如 worker-A ACK 了 tag=9，但 tag=2、tag=6 从来没有对应的 ACK 日志。

查 RabbitMQ AMQP 文档，关键发现：

> `multiple=true` 的语义：**ACK 当前 delivery tag，以及该 channel 上所有 tag ≤ 当前 tag 的未确认消息。**

根因就在这里。下面用时间线展示为什么多 worker 下 `multiple=true` 会出问题：

```
============ delivery tag 分配（全局单调递增，但 50 个 goroutine 竞争同一个 msgs channel）============

RabbitMQ 下发消息顺序：tag=1, 2, 3, 4, 5, 6, 7, 8, 9, 10...

goroutine 调度（随机竞争，不保证顺序）：
  consumeLoop-1 拿到 tag = 1, 4, 7
  consumeLoop-2 拿到 tag = 2, 5, 8
  consumeLoop-3 拿到 tag = 3, 6, 9
  （每个 item 携带 delivery d，放入 batchCh）

batchInsertWorker-A 的 batch = [tag=1, tag=4, tag=7]   ← 碰巧攒到这三条
batchInsertWorker-B 的 batch = [tag=2, tag=5, tag=6, tag=8, tag=3, tag=9]

============ 时序 ============

t1: worker-A 处理完 batch，对最后一条 tag=7 调用 d.Ack(multiple=true)
    → AMQP 协议发送："ACK tag=7, multiple=true"
    → RabbitMQ 含义：确认 tag ≤ 7 的所有未确认消息
    → 被确认的 tag：1, 2, 3, 4, 5, 6, 7  ← tag=2,3,5,6 是 worker-B 的！

t2: worker-B 继续处理，对 tag=2 调用 d.Ack(false)
    → AMQP 返回错误（或静默忽略）：该 tag 已被 t1 的 multiple ACK 覆盖
    → 如果 worker-B 的 tag=2 处理失败想 Nack → 也无法 Nack（已被确认）

============ 结果 ============

[正常情况] tag=1,4,7 被 worker-A 正确处理后确认 ✓
[被误 ACK]  tag=2,3,5,6 被 worker-A 的 multiple ACK 确认，但 worker-B 还没处理完
            → RabbitMQ 认为已交付成功，不再重投
            → worker-B 后续的 Ack/Nack 对这些 tag 无效
            → 这些消息的落库结果完全取决于 worker-B 有没有来得及写完
            → 如果 worker-B 失败 → 消息丢失，无法补救
```

**这个 bug 实际上造成了两个问题（Unacked 只是表象）**

**【问题 A：消息静默丢失】**

`multiple=true` ACK 了 tag=7 → tag=1~7 全部从 Unacked 移除（你说得对，它们确实被 ACK 了）。

但 worker-B 持有的 tag=4 此时已被 worker-A 预先 ACK，worker-B 即使 DB 写入失败，也无法再 Nack 这条消息送死信队列——**消息永久丢失，且没有任何错误日志**。这才是最危险的后果。

**【问题 B：AMQP channel 反复崩溃 → Unacked 波动】**

Unacked 增长的真正根因不是「tag 被卡住」，而是 **double-ACK 触发 AMQP 协议异常**：

```
t1: worker-A 调 Ack(tag=7, multiple=true)
    → RabbitMQ 把 tag=1~7 全部确认，Unacked 减少 7
    → RabbitMQ 看到 7 个槽位空出，立即投递 tag=8~14（Unacked 又加回来）

t2: worker-B 处理完 tag=4（已被 worker-A 提前 ACK），调 Ack(tag=4, false)
    → RabbitMQ 收到"对已确认 tag 的重复 ACK"
    → 返回 PRECONDITION_FAILED（AMQP 协议层异常）
    → AMQP channel 被强制关闭！

t3: channel 关闭的后果
    → channel 上所有「已投递但未 ACK」的消息（如 tag=8~14）
       被 RabbitMQ 重新放回 Ready 队列，等待重投
    → 消费端需要重建 channel，重新注册 consumer
    → 重建期间新消息继续进来 → 新的 Unacked 累积
    → 如果 channel 反复崩溃重建，Unacked 就持续波动增长
```

**为什么重启才归零？**

重启关闭整个 TCP 连接 → RabbitMQ 把该连接上所有 Unacked 消息全部重新入队（变成 Ready）→ Unacked 归零。

**两个 bug 的对比**：

| | 问题 A（消息丢失） | 问题 B（channel 崩溃） |
|--|---|---|
| **根因** | multiple ACK 越权确认其他 worker 的消息 | 对已 ACK 的 tag 重复确认触发 PRECONDITION_FAILED |
| **现象** | 消息不见了，无日志 | Unacked 波动，偶发 amqp error 日志 |
| **危险程度** | 更危险（数据丢失） | 较轻（重试后能恢复） |

**解决**：
```go
// 改前（有两个 bug）
_ = it.d.Ack(true)   // multiple=true

// 改后（两个都解）
_ = it.d.Ack(false)  // multiple=false，只确认这一条
// → 不会越权 ACK 其他 worker 的 tag（问题 A 消失）
// → 不会对已 ACK 的 tag 重复确认（问题 B 消失）
```

**核心原则**：
- `multiple=true` 只能安全用于**单 consumer 单线程顺序消费**（tag 严格单调，只有一个处理者）
- 多 goroutine / 多 worker 并发消费，必须用 `multiple=false` 逐条确认
- 「用 multiple=true 省网络往返」是错误的优化方向，并发下两个 bug 都会被触发

---

#### 【问题 2】压测消息丢失 + 批量 INSERT 整批静默失败

**现象**：k6 压测 200 VU 并发，接收方偶尔收不到消息，发送方有 `server_ack`（说明 Gateway 收到了），但 Logic 侧没有对应的落库记录。且没有任何 error 日志——错误被静默吞掉了。

**排查过程**：
1. 加日志：在 `BatchInsertMessages` 调用处打印 `err` 返回值
2. 复现：在压测期间人为重启 Logic 实例（模拟崩溃），看到了 `Error 1062: Duplicate entry 'xxx' for key 'PRIMARY'`
3. 还原场景：Logic 崩溃后 MQ 手动 ACK 模式下未 ACK 的消息重新投递，同一条 MsgID 被消费两次，同批次里出现重复 MsgID，整批 `INSERT` 失败
4. **静默失败的原因**：原代码批量失败直接返回，没有做任何 Nack，消息既不 ACK（挂在 Unacked）也不重试（没有 Nack），最终 Logic 重启才清空——期间所有消息实际上已经丢了

**解决方案（四层容错）**：
```
批量 INSERT 失败
  └─→ 降级逐条单插（隔离脏数据）
        ├─ 成功 → pushDown + Ack(false)
        ├─ Error 1062（重复键）→ 幂等跳过，视为成功，pushDown + Ack(false)
        └─ 其他错误（磁盘满/连接断）→ Nack(false, false) → 死信队列
```
```go
for _, it := range items {
    if sErr := dao.InsertMessage(it.msg); sErr != nil {
        if isDuplicateKey(sErr) {
            go pushDownAndAckWithAddr(it, addr) // 幂等成功
        } else {
            _ = it.d.Nack(false, false)         // 真实错误送 DLX
        }
        continue
    }
    go pushDownAndAckWithAddr(it, addr) // 正常成功
}
```
改完后压测 100 万条消息，投递成功率 100%，无丢失。

---

#### 【问题 3】Docker Compose 启动时序——Logic 连不上 Milvus

**现象**：`docker-compose up -d --build` 后，Logic 容器启动 30 秒后崩溃，重启还是崩，日志显示：
```
FATAL milvus 连接超时: context deadline exceeded
```
但 `docker ps` 显示 Milvus 容器状态是 `Up`。

**排查过程**：

1. **第一层：Milvus 启动慢**：进 Milvus 容器看日志 `docker logs im-milvus`，发现它还在做初始化（加载 etcd schema、MinIO bucket check），要等 30~60 秒才真正 ready，但容器状态已经是 `Up` 了

2. **第二层：Milvus 依赖 etcd 和 MinIO**：Milvus 自身也依赖 etcd（存元数据）和 MinIO（存向量文件），如果这两个没起好，Milvus 也起不来，但 `depends_on` 只保证容器启动顺序，不保证服务就绪

3. **第三层：depends_on 的误解**：Docker Compose 的 `depends_on: milvus` 默认只等 milvus **容器启动**，不等 milvus **服务可用**

**解决方案**：
```yaml
mysql:
  healthcheck:
    test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-p123456"]
    interval: 5s
    timeout: 5s
    retries: 10
    start_period: 20s   # 给 MySQL 初始化足够时间

milvus:
  depends_on:
    - etcd    # Milvus 依赖 etcd 和 MinIO，必须先起
    - minio

logic:
  depends_on:
    mysql:
      condition: service_healthy   # 等 MySQL healthcheck 通过
    milvus:
      condition: service_started   # 等 Milvus 容器启动（Milvus 本身没 healthcheck）
    rabbitmq:
      condition: service_started
```
同时在 Logic 的初始化代码里加了 Milvus 连接重试（指数退避，最多重试 5 次），作为双重保障。

---

#### 【问题 4】压测时 MQ 出现积压，消费跟不上

**现象**：k6 压测时把 VU 从 100 加到 200，RabbitMQ 管理界面 `messages ready`（待消费数）开始增长，从 0 慢慢涨到几千条，说明消费速度跟不上生产速度。

**排查**：
1. 看 Logic 日志，`batchInsertWorker` 的每批耗时：平均 8ms，但有时会飙到 60ms
2. 飙高时看 MySQL 状态：`SHOW PROCESSLIST` 显示有慢查询在跑
3. 发现原因：**`GetGroupMembers` 查询没有索引**——群消息写扩散时，每条群消息要查一次群成员，这个查询走了全表扫

**短期止损**：
```bash
# 临时增加 Logic 消费实例（MQ 竞争消费，天然负载均衡）
# 两个 Logic 实例消费同一个 logic.upload.queue，消耗能力翻倍
docker-compose scale logic=2
```

**根因修复**：给 `group_members` 表的 `group_id` 加索引，`GetGroupMembers` 从全表扫变为索引查找，查询时间从 20ms 降到 <1ms，积压消失。

**长期预案**：
- Logic 多实例水平扩展（直接翻倍消耗能力，MQ 竞争消费天然负载均衡）
- 消息分级：AI 消息（耗时高）走低优先级队列，普通消息走高优先级队列，保障实时聊天体验不受 AI 消息积压影响

---

### Q28：测试阶段出现过哪些问题，你是怎么处理的？

**答（话术）：**

**① 单元测试：幂等逻辑难以测试**

`isDuplicateKey` 这个函数只是字符串匹配，但真正触发 Error 1062 需要真实 MySQL 环境。我的处理方式是：
- 用 table-driven test 测字符串匹配逻辑本身（不依赖 DB）
- 另起集成测试，在测试前 `BeforeEach` 用 GORM 把表清空，然后先插一条，再插相同 MsgID 的，验证第二次返回的错误被正确识别为 duplicate key

**② 压测：k6 压测时消息乱序到达**

用 k6 跑 200 并发 WS 连接发消息时，发现接收方收到消息的顺序跟发送顺序不一致。

排查：MQ fanout 模式下多个 upload worker 并发处理，处理速度不一致，后发的消息可能先落库、先下行。

我的解法：告知这是「系统设计上的 trade-off」——为了高并发批量写入，放弃了严格顺序；客户端用 `seq_id` 排序展示，不依赖到达顺序。这个和微信、Telegram 等主流 IM 的设计一致。

**③ 联调阶段：前端 WebSocket 频繁断连**

测试时前端同学反馈 WS 连接频繁断开，但 Gateway 日志里没有 close 记录。

排查过程：
1. 看 `ReadPump` 里的 SetReadDeadline，默认是 `time.Now().Add(60s)`
2. 前端在本地调试时 DevTools 有 pause，心跳超时了
3. 调整为前端每 10 秒发一次 ping，服务端 ReadDeadline 设为 60 秒，给足余量
4. 另外发现 Nginx（开发环境没有但生产有）默认的 proxy_read_timeout 是 60s，需要加 `proxy_read_timeout 3600s` 和 `proxy_send_timeout 3600s`

---

### Q29：你怎么排查线上服务慢的问题（故障诊断 SOP）？

**答（话术）：**

我的排查顺序是固定的，从外到内，从现象到根因：

**第一步：看监控大盘确认现象**
- 看 CPU、内存是否异常飙升（Go 服务一般内存稳定，CPU 飙高说明有热点计算）
- 看 RabbitMQ 管理界面 `messages ready` 有没有积压（积压说明消费跟不上）
- 看 MySQL 的 `Threads_running`（`SHOW STATUS LIKE 'Threads%'`），正常应该很低，高说明慢查询阻塞

**第二步：快速定位是哪一层慢**
- 看 Zap 日志里的 `[Upload]` 关键词，看每批次的处理耗时（我打了详细的 Debug 日志）
- 如果 MQ ready 积压但消费日志正常，说明是 DB 写入或网络慢
- 如果消费日志有超时，定位是 `BatchInsertMessages` 慢还是 `Redis MGet` 慢

**第三步：MySQL 慢查询定位**
```sql
-- 开慢查询日志（如果没开）
SET GLOBAL slow_query_log = 1;
SET GLOBAL long_query_time = 0.1; -- 超过 100ms 的都记录

-- 查看当前慢查询
SHOW PROCESSLIST;

-- 用 EXPLAIN 分析
EXPLAIN SELECT * FROM messages WHERE receiver_id = 123 AND is_read = false;
-- 看 key 字段，确认走了 idx_receiver_read 联合索引
```

**第四步：Redis 排查**
```bash
redis-cli --latency           # 实时延迟
redis-cli info stats | grep rejected_connections  # 是否有连接被拒
redis-cli slowlog get 10      # 最近 10 条慢命令
```

**第五步：Go 服务本身**
```bash
# 如果怀疑 goroutine 泄漏或内存问题
curl http://localhost:8001/debug/pprof/goroutine?debug=1
# 分析是否有大量 goroutine 阻塞在同一个地方
```

**定位完成后：** 立即写 RCA（根因分析），内容包括：时间线、影响范围、根因、修复方案、复盘改进。这个习惯我在自己的项目里也保持，出问题不只是修好，要沉淀成文档。

---

### Q30：服务器运维，你熟悉哪些 Linux 常用操作？

**答（话术）：**

我在本地开发和 Docker 部署中用得比较多的：

**进程管理**
```bash
ps aux | grep gateway         # 查 gateway 进程
kill -9 <pid>                 # 强制杀进程
nohup ./gateway > gateway.log 2>&1 &  # 后台运行并重定向日志
```

**日志分析（这个在排查问题时最常用）**
```bash
tail -f logs/nexusgo.log                         # 实时看日志
grep "ERROR" logs/nexusgo.log | tail -100        # 只看最近 100 条错误
grep "BatchInsert" logs/nexusgo.log | grep -v DEBUG  # 过滤关键词
```

**网络排查**
```bash
netstat -tlnp | grep 8080     # 看端口是否在监听
ss -s                         # 查看连接统计，关注 CLOSE_WAIT 数量
curl -v http://localhost:8080/health  # 快速确认服务是否响应
```

**资源监控**
```bash
top -p $(pgrep gateway)       # 只看 gateway 进程的 CPU/内存
iostat -x 1                   # 磁盘 IO 监控
free -m                       # 内存使用情况
df -h                         # 磁盘空间
```

**Docker 运维（我项目实际用的）**
```bash
docker logs nexusgo-logic-1 -f --tail 100  # 实时看 logic 日志
docker stats                               # 所有容器资源用量
docker exec -it im-mysql mysql -u root -p  # 进 MySQL 容器
docker-compose restart logic               # 单独重启 logic 服务
```

---

### Q31：你对 AI Coding 的看法和实际使用方式是什么？

**答（话术）：**

我对 AI Coding 的看法是：**它是个加速器，不是替代品，关键在于你会不会用**。

**我实际怎么用的：**

1. **架构设计阶段用 AI 对话**：我在设计批量写入方案时，先把问题描述清楚（现象、数据量、现有代码），让 AI 列出几种方案（单条串行 / 批量缓冲 / 异步 channel），然后我自己评估每种的 trade-off，最终选了 channel + ticker 双触发方案。AI 给我省了大量查文档和搜博客的时间，但最终决策是我做的。

2. **写重复性代码用 AI**：比如 Protobuf 的 RPC 服务注册代码、Docker-compose 的 healthcheck 配置，这些有固定模式的代码直接让 AI 生成，我 review 后直接用。

3. **排查 bug 时用 AI 作为第二视角**：把 error log 和相关代码片段贴给 AI，它经常能快速指出我没注意到的问题。比如那个 ACK 乱序问题，我描述了现象后 AI 直接提示了 `multiple=true` 的潜在问题，我才去看 RabbitMQ 文档确认的。

4. **AI 不能替代的地方**：业务逻辑的正确性判断、性能测试数据的解读、线上问题的根因分析——这些都需要我自己去做，AI 给的答案必须验证，不能直接信。

**关于 AI 写的代码 review 的习惯**：
- 每一行都要理解，不懂的不用
- 对于有副作用的操作（DELETE/UPDATE/重要配置），必须自己写或逐字核对
- 用 AI 生成的测试用例是个很好的习惯，比自己想边界情况更全面

---

### Q32：如果让你来做飞机管控后端，设备数据落库，你会怎么设计？（场景题）

**答（话术）：**

这道题考的是我能不能把项目经验迁移到新场景。我会这样回答：

设备时序数据和 IM 消息有很多共同点：**写多读少、时间序列、需要快速查询某段时间内的数据**。

**表结构设计思路：**
```sql
-- 设备状态上报表（类比 messages 表）
CREATE TABLE device_telemetry (
    id          BIGINT PRIMARY KEY,     -- Snowflake 生成，有序且分布式唯一
    device_id   VARCHAR(64) NOT NULL,   -- 设备唯一标识
    device_type TINYINT NOT NULL,       -- 1: 无人机, 2: 传感器...
    metric      VARCHAR(32) NOT NULL,   -- 指标名：battery/altitude/speed
    value       DOUBLE,
    unit        VARCHAR(16),
    report_time DATETIME(3) NOT NULL,   -- 毫秒级精度，时序数据关键
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_device_time (device_id, report_time),  -- 核心查询索引
    INDEX idx_type_time   (device_type, report_time) -- 按类型查询
);
```

**写入优化**：和我 IM 里一样，用批量写入，设备上报频率高（比如 10Hz），不能每条数据一个 INSERT，攒 100 条或 100ms 批量写一次。

**查询优化**：查某台设备最近 1 小时的轨迹，走 `(device_id, report_time)` 联合索引；查所有设备某个时间点的状态，走 `(device_type, report_time)` 索引。

**扩展**：数据量极大时（亿级），可以按 `device_type` 分表，或者考虑 TDengine / TimescaleDB 这类专门的时序数据库，写入吞吐和时序查询效率远超 MySQL。

---

### Q33：如果出现服务 7×24 小时运行中断，你的应急处理流程是什么？

**答（话术）：**

我在自己项目联调中实践过，结合行业通用 SOP：

**第一步：确认影响范围（1 分钟内）**
- 先 `docker ps` / `ps aux` 确认哪个进程挂了
- 看最近的 error 日志，找最后一条正常日志和第一条异常日志之间的时间点
- 确认是单节点还是全量故障

**第二步：快速止损（3 分钟内）**
- 先重启服务（`docker-compose restart logic`），让服务先恢复，再查原因
- 如果重启失败，看具体报错——是配置问题、端口占用、还是依赖（MySQL/Redis）挂了

**第三步：根因分析（恢复后）**
- 看完整日志，用 `grep "FATAL\|panic" logs/xxx.log` 找 panic 栈
- Go 服务 panic 会打印 goroutine 栈，直接定位到哪一行代码

**第四步：复盘沉淀**
- 写时间线：几点发现、几点处理、几点恢复
- 分析根因和修复方案
- 补充监控告警（让下次能更快发现）

**我项目里的具体案例**：Logic 容器启动后 30 秒崩溃，重启后还是崩。看日志发现是 `Milvus 连接超时`，因为 Milvus 依赖 MinIO 和 etcd，这两个初始化完才能接受连接，但 Logic 容器在 Milvus 还没准备好时就尝试连接了。解决方案是在 docker-compose 里给 Milvus 加 `depends_on: etcd, minio`，Logic 加 `depends_on: milvus`，解决了启动时序问题。

---

### Q34：快问快答（可能被追问的高频小问题）

**Q: Redis 和 MySQL 数据不一致了怎么处理？**  
A: 我的路由 key（`route:user:xxx`）是 30 秒 TTL 的软状态，不做双写一致。过期了下行推送失败，消息已在 MySQL，等用户下次上线 SyncUnread 就拉到了。对于强一致要求的数据（比如用户信息），先写 DB 再删 Redis，不更新缓存，让下次查询时回源填充。

**Q: MySQL 连接池怎么配？**  
A: GORM 的 `SetMaxOpenConns(100)` / `SetMaxIdleConns(10)` / `SetConnMaxLifetime(1 * time.Hour)`。最大连接数根据 MySQL 的 `max_connections`（默认 151）来，留一些给运维操作，不能满打满算。

**Q: 你了解分库分表吗？**  
A: 了解基本思路。垂直分库按业务拆（把消息库和用户库分开），水平分表按 hash 或 range（messages 表按 `user_id % N` 分片）。我的项目规模不需要，但如果消息量上了 10 亿，需要按 `user_id` 分片，同时 msg_id 用 Snowflake 保证跨分片唯一。分库分表引入的最大问题是跨分片查询（join、排序）需要业务层处理，不能依赖 DB。

**Q: 接口幂等怎么保证？**  
A: 我的方案是在 MySQL `msg_id` 上建唯一索引，重复提交时第二次 INSERT 返回 1062，业务层识别后视为成功。更通用的方案是用 Redis SETNX 做幂等 token：客户端带一个唯一 request_id，服务端先 SETNX，成功才执行业务，防止网络重试导致的重复。

**Q: 你的系统能撑多少并发？**  
A: 我用 `ws_true_qps.js` 脚本做了真实双人互发压测：100 对用户（200 账号）同时运行，每对每 100ms 发一条消息，实测吞吐 990 msg/s，端到端 P95 延迟 131ms，消息零丢失。这是在 Windows + Docker Desktop 单机环境下的数字，如果换裸机 Linux 预估可以提升 3~5 倍。瓶颈在 MySQL 批量写入，Logic 多实例竞争消费可以线性扩容。

---

### Q35：你怎么证明压测数据是可信的？脚本是怎么设计的？

**答（话术，主动讲清楚，防止面试官觉得数据是编的）：**

**脚本架构（ws_true_qps.js）：**

```
VUS=100 → setup 阶段预先登录 100 对账号（200 个账号，完全隔离）

账号分配：
  第 i 对：user_{1000+i*2+1}（Sender）↔ user_{1000+i*2+2}（Receiver）
  奇数 VU → Sender 角色，偶数 VU → Receiver 角色

Sender 逻辑：
  连接 WebSocket 后，每隔 SEND_INTERVAL=100ms 发一条消息
  消息体格式：msgTs:{发送时间戳}:rid:{本轮唯一RUN_ID}:p{对号}:s{序号}

Receiver 逻辑：
  连接 WebSocket 后持续监听
  收到消息后提取发送时间戳：latency = Date.now() - sentAt
  只统计包含本轮 RUN_ID 的消息（过滤掉 SyncUnread 的历史积压）
```

**为什么要设计 RUN_ID 过滤？**

这是一个我踩过的坑：早期版本没有过滤，测出来的延迟经常有 30~60 秒的异常高值。排查发现是 Receiver 重连时，Gateway 会推送 `SyncUnread`（离线消息同步），把上一轮测试遗留的消息全部推过来——这些消息的「发送时间」是几分钟前，用「当前时间 - 几分钟前」算出来的延迟当然离谱。

解决方法：每轮测试生成唯一 6 位 RUN_ID 嵌入消息体，Receiver 用正则过滤，只计算本轮消息的延迟：
```javascript
const match = msg.match(MSG_PATTERN); // /msgTs:(\d+):rid:{runId}:/
if (!match) { msgSkipped.add(1); return; } // 历史消息直接跳过
```

**关键指标的来源：**

| 指标 | 对应 k6 自定义指标 | 计算方式 |
|------|-----------------|---------|
| 990 msg/s | `true_msg_sent` counter / duration | 发出总数 ÷ 测试时长 |
| P95 = 131ms | `true_e2e_ms` trend p(95) | Receiver 收到时刻 - 消息体时间戳 |
| 成功率 100% | `true_recv_ok` rate | 收到消息数 ÷ 发出消息数 |

**环境说明（主动交代，不藏着）：**

测试环境是 Windows 笔记本 + Docker Desktop，所有服务都在同一台机器上的容器里跑，k6 也在同一台机器上。这个环境的瓶颈：
- Docker Desktop on Windows = Hyper-V 虚拟机，Host↔容器网络有 NAT 额外延迟
- CPU 被 k6 + 三个微服务 + 基础设施容器共享
- 单机部署，没有做读写分离

**这组数据证明的是系统稳定性**——100 对用户并发、60 秒连续压测、消息零丢失、延迟可控，而不是极限吞吐。裸机 Linux 多实例部署预估能到 3000~5000 msg/s。