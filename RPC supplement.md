# 面试补充方向 —— 针对得物「中间件 AI 开发工程师（Golang 方向）」岗位

> **项目组合**：NexusGo（高并发分布式 IM + AI Agent）× GrowRPC（自研 RPC 框架）
>
> 面试主线是 NexusGo，GrowRPC 是其底层 RPC 基础设施，两者结合可以展现"从业务系统到基础设施"的完整技术栈深度。

---

## 一、两个项目的定位与联动关系

```
NexusGo 全局视角
┌─────────────────────────────────────────────────────┐
│  Gateway (WS 连接管理)                               │
│       │                                             │
│       │  ← GrowRPC (TCP + Protobuf，一致性哈希)     │
│       ▼                                             │
│  Logic (业务落库 + 消息路由)                         │
│       │                                             │
│       └── RabbitMQ → Agent (AI 推理) → SSE → Gateway│
└─────────────────────────────────────────────────────┘
```

GrowRPC 在 NexusGo 中的**具体价值**：
- Gateway 需要将用户消息路由到正确的 Logic 实例（多实例部署），GrowRPC 提供了带**一致性哈希负载均衡**的 RPC 调用，同一 UserID 的消息始终命中同一 Logic 实例，避免跨实例查 Redis 路由表的额外开销
- 客户端连接池避免频繁建连，配合 NexusGo 990 msg/s 的吞吐量，单连接多路复用确保 Gateway→Logic 的 RPC 链路不成为瓶颈
- Protobuf 编码在消息量大时压缩率显著优于 JSON，降低内网带宽占用

**面试时如何引出 GrowRPC**：
> "Gateway 和 Logic 之间我没有用 gRPC，而是用了自己实现的 GrowRPC。这样做一方面是为了深入理解 RPC 框架的底层原理，另一方面也因为自研框架可以更灵活地裁剪，比如去掉 gRPC 的 HTTP/2 开销，直接用 TCP + Protobuf，在内网短路场景下延迟更低..."

---

## 二、JD 核心能力对照（整合两个项目）

| JD 要求 | NexusGo 覆盖 | GrowRPC 覆盖 | Gap 程度 | 优先级 |
|---------|-------------|-------------|---------|--------|
| Golang 开发 | ✅ 完整微服务，goroutine/channel 深度使用 | ✅ 泛型/sync/net 底层 | 无 Gap | — |
| RPC 框架 | ✅ 实际使用 GrowRPC | ✅ 从零实现，含粘包/泛型/连接池 | 无 Gap | — |
| 消息队列 | ✅ RabbitMQ 手动 ACK + DLX + 幂等 | — | 无 Gap | — |
| 分布式缓存 | ✅ Redis 路由表 + Lua 原子摘要替换 + 滑窗 | — | 无 Gap | — |
| AI/LLM 实践 | ✅ Eino ReAct Agent + 三层记忆 + RAG | — | 无 Gap | — |
| MCP 协议 | ⚠️ 了解概念，无实际代码 | — | 中 Gap | ⭐⭐⭐ |
| AI Gateway | ⚠️ 有 SSE 转发，无完整 Gateway 设计 | — | 中 Gap | ⭐⭐ |
| Service Mesh | ❌ 无实践 | — | 高 Gap | ⭐⭐ |
| etcd 注册中心 | ⚠️ 用 etcd 做 Milvus 元数据，未做服务发现 | ⚠️ HTTP 心跳注册，非 etcd | 中 Gap | ⭐⭐ |
| 分布式一致性 | ⚠️ Snowflake 幂等，无 Raft 认知 | — | 中 Gap | ⭐⭐ |

---

## 三、GrowRPC 现有内容的深挖建议（结合 NexusGo 场景）

### 3.1 粘包与协议设计 → 结合 NexusGo 场景强化

**你可以这样说**：
> "在 NexusGo 里，Gateway 和 Logic 之间的 GrowRPC 连接走的是 Protobuf 编码，但服务端握手阶段用的是 JSON 的 Option 协商。高并发下 Gateway 给 Logic 发 100ms 内的批量消息时，JSON 握手包和 Protobuf 数据包会合并在同一个 TCP 段里。正是在这个场景下我发现了 json.Decoder 的 4KB 预读缓冲会吞掉后续 Protobuf 字节，导致服务端解出 160MB 的荒谬帧长并死锁..."

这样的叙述把"技术 Bug"和"真实业务场景"绑定了，说服力远强于孤立讲框架。

**追问准备**：
- `bufio.Reader.ReadBytes` 为什么不会丢失后续数据？（br 内部维护缓冲，未消费的字节留在 br 里，通过 safeBufferConn 无损传递）
- NexusGo 里 Gateway 建立多少条 GrowRPC 连接？（连接池 maxActive 配置，一致性哈希决定每个 Logic 实例分配的连接数）

---

### 3.2 连接池 → 结合 NexusGo 990 msg/s 吞吐量

**你可以这样说**：
> "NexusGo 压测时 990 msg/s，每条消息 Gateway 都要通过 GrowRPC 调用 Logic。如果每次都新建连接，握手 + TLS + TCP 慢启动加起来至少几十毫秒，根本撑不住这个吞吐量。连接池的价值在这里体现得很清晰——用 LIFO 栈优先复用热连接，配合 maxActive 防止连接数爆炸，cleaner goroutine 清理过期连接..."

**追问准备**：
- 连接池 maxActive 怎么设定的？（Logic 实例数 × 期望每实例并发度，NexusGo 场景下可以根据 batch worker 数来估算）
- 如果一个 Logic 实例宕机，连接池怎么感知？（IsAvailable 健康检查，Get 时发现不健康则关闭并重新创建，不会把"死连接"借出去）

---

### 3.3 一致性哈希 → 结合 NexusGo 跨实例路由问题

**你可以这样说**：
> "NexusGo 里 Logic 是多实例部署的。如果同一个用户 A 发给 B 的消息，第一条路由到 Logic-1，第二条路由到 Logic-2，Logic-2 就需要跨实例查 Redis 路由表来找 B 在哪个 Gateway 上，多了一次网络 RTT。用一致性哈希按 UserID 做路由，同一对话的所有消息都命中同一 Logic 实例，路由表查询变成本地内存操作..."

**追问准备**：
- 一致性哈希的"雪崩"问题是什么？如何用虚拟节点解决？（节点分布不均，虚拟节点让哈希环更均匀，GrowRPC 实现了 replica 参数控制虚拟节点数）
- Logic 实例扩容时，一致性哈希会影响多少在途请求？（约 k/n 的 key 需要重新路由，GrowRPC 的客户端重新拉取注册中心列表后自动生效）

---

### 3.4 泛型零反射路由 → 结合性能数字

**追问准备**：
- 你说 handler 调用开销从 471ns 降到 10ns，在 NexusGo 的 990 msg/s 场景下这意味着什么？（990 msg/s × 461ns 节省 ≈ 约 0.046% 的 CPU 时间，说明在 IO 密集型场景下 CPU 优化作用有限，但在未来 CPU 密集型场景如批量压缩、加密时收益会放大）
- 为什么泛型闭包能捕获类型参数？（Go 泛型实例化时为每个具体类型生成或共享代码，闭包捕获的是实例化后的具体函数，运行时无需反射）

---

## 四、高优先级补充方向（面试前必须了解）

### 4.1 ⭐⭐⭐ MCP 协议（JD 明确加分项）

**为什么重要**：JD 写明"熟悉 MCP 等 AI 工具协议者优先"，且你的 NexusGo 已经有完整的 Agent 工具调用体系（HITL、工具分类、ApprovalMiddleware），向 MCP 标准靠拢是自然的延伸。

**MCP 核心要点**：
- **定位**：AI Agent 与外部工具之间的标准通信协议，底层是 JSON-RPC 2.0
- **三类能力**：Tools（可调用函数）、Resources（可读数据源）、Prompts（提示模板）
- **连接方式**：stdio（本地进程通信）或 SSE/HTTP（远程服务）

**和你项目的连接点**：
> "NexusGo 里我已经实现了工具安全两级分类和 HITL 审批，这其实和 MCP 的 Tool 调用语义高度对齐——MCP 的 tools/call 就是标准化的工具触发协议，我的 ApprovalMiddleware 相当于在工具调用链上插了一个审批拦截器。如果把 NexusGo 的 Agent 工具改造成 MCP Server，只需要把 tool handler 包一层 JSON-RPC 2.0 的 schema 即可，架构完全兼容。"
>
> "另外，GrowRPC 和 MCP 底层都是 RPC 协议，GrowRPC 是自定义 TLV + Protobuf，MCP 是 JSON-RPC 2.0，两者的服务注册、请求路由、序列化机制是同一抽象层级的设计，只是应用场景不同。"

**快速上手**（半天）：用 `github.com/mark3labs/mcp-go` 把 NexusGo 的 `search_chat_history` 工具包装成一个 MCP Server，用 Claude Desktop 或自写 Agent 调用它。这样面试时可以说"我把 NexusGo 的工具改造为 MCP Server 做过验证"。

---

### 4.2 ⭐⭐ etcd 服务发现（取代 HTTP 心跳注册中心）

GrowRPC 目前用的是 HTTP 心跳方案，NexusGo 里 etcd 只用于 Milvus 元数据，两者都没有真正用 etcd 做 RPC 服务发现。

**需要掌握**：
- etcd Lease（租约）机制：服务启动时创建 TTL=10s 的 Lease，把服务地址 put 到对应 key，定期 KeepAlive 续期。宕机后 Lease 自动过期，key 被删除
- Watch 机制：客户端 Watch /services/logic/ 前缀，节点上下线时实时收到 Event，无需轮询
- 对比你的方案：HTTP 心跳有轮询延迟（服务下线最长等一个 TTL 周期），etcd Watch 是推送模型，延迟约 100ms 级

**快速 Demo**（2 小时）：
```go
// 用 etcd Go SDK 替换 GrowRPC 的 HTTP 心跳注册
cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
lease, _ := cli.Grant(ctx, 10)
cli.Put(ctx, "/services/logic/"+addr, addr, clientv3.WithLease(lease.ID))
cli.KeepAlive(ctx, lease.ID)
```

**面试话术**：
> "GrowRPC 目前的注册中心是 HTTP 心跳方案，它足够简单，适合单机开发验证。但在 NexusGo 多实例部署场景下，HTTP 心跳的最大问题是延迟——Logic 实例宕机后，Gateway 要等最多一个 TTL 周期（比如 10s）才能感知，这 10s 内所有路由到这个实例的请求都会失败。etcd Watch 模型能把这个感知延迟降到 100ms 级，这是我下一步要改造的方向。"

---

### 4.3 ⭐⭐ LLM Gateway / AI Gateway 设计认知

JD 明确提到"大模型 API Gateway"，你的 NexusGo 里有 Agent 服务，但没有完整的 Gateway 层设计。

**需要了解的 LLM Gateway 核心功能**：
- **多模型路由**：根据请求参数（model 字段、cost 预算）路由到不同 Provider（OpenAI/Claude/Qwen）
- **限流**：按 APIKey 做 token bucket 限流，防止单用户打爆配额
- **负载均衡**：多个同模型 Provider 实例之间轮询/权重分发
- **流式代理**：SSE 流的透明转发，不能把整个响应 buffer 住再返回（延迟爆炸）
- **可观测性**：记录每次调用的 prompt tokens、completion tokens、首字延迟（TTFT）

**和你项目的连接点**：
> "NexusGo 里 Agent 服务和 Gateway 之间的 SSE 流转发，其实就是一个简化版的 LLM 流式代理——Gateway 收到 AI 请求后，保持一个 HTTP 长连接到 Agent，逐 chunk 读取 SSE 流，再通过 WebSocket 推送给客户端。这个架构和 LLM Gateway 的流式代理是同一个模式。下一步我会在 Agent 服务前加一层 Gateway，做多模型路由和 APIKey 限流。"

---

### 4.4 ⭐ Service Mesh 基础概念

**需要了解（能讲清楚概念即可，不需要实操）**：
- 数据面（Envoy/MOSN Sidecar）vs 控制面（Istio）的职责边界
- 为什么用 Service Mesh：把限流/熔断/链路追踪从业务代码剥离，下沉到基础设施层
- 和 SDK RPC 框架（GrowRPC）的关系：SDK 在应用层做路由和序列化，Mesh 在网络层做流量治理，两者可以共存，职责不重叠

**面试话术**：
> "GrowRPC 是 SDK 形式的 RPC 框架，流量治理逻辑（限流、熔断）写在业务进程里。Service Mesh 把这部分逻辑下沉到 Sidecar Proxy（如 Envoy），业务进程不感知治理策略的变化，配置通过控制面统一下发。在 NexusGo 的 Docker Compose 单机部署阶段用 GrowRPC 就足够了，但如果上 K8s 多集群，引入 Istio 可以统一管理跨服务的流量治理策略，这是架构演进的自然路径。"

---

## 五、面试前 1 周行动清单

| 优先级 | 任务 | 预计耗时 |
|--------|------|---------|
| ⭐⭐⭐ | 用 mcp-go 把 NexusGo 的 search_chat_history 工具包装成 MCP Server | 4h |
| ⭐⭐⭐ | 背熟 NexusGo 消息可靠性矩阵（6 个故障场景 × 处理方式）| 1h |
| ⭐⭐⭐ | 准备 GrowRPC 的 160MB 死锁故事（能流畅口述两次踩坑过程）| 0.5h |
| ⭐⭐ | 用 etcd Go SDK 跑通服务注册发现 demo（替代 HTTP 心跳）| 2h |
| ⭐⭐ | 能解释 RabbitMQ 手动 ACK vs 自动 ACK 的区别及死信队列原理 | 1h |
| ⭐⭐ | 能讲清楚 NexusGo 三层记忆体系（短期/中期/长期）的触发时机 | 0.5h |
| ⭐⭐ | 能讲清楚 LLM Gateway 的核心功能（限流/多模型路由/流式代理）| 0.5h |
| ⭐ | 了解 Service Mesh 数据面 vs 控制面概念 | 0.5h |

---

## 六、联合项目的核心话术（面试开场 2 分钟版）

> 我有两个核心项目，一个是**业务层**，一个是**基础设施层**，两者是上下游关系。
>
> 业务层是 **NexusGo**，一套分布式 IM + AI Agent 系统。核心难点有三个：第一，消息可靠性——通过 RabbitMQ 手动 ACK + 死信队列 + Snowflake 幂等，100 人压测消息投递成功率 100%，P95 延迟 131ms；第二，AI Agent 工程化——基于 Eino 框架实现了完整的 ReAct 循环、HITL 审批中断、工具安全分类和自动续写，以及短期/中期/长期三层记忆 + 双路 RAG 召回；第三，SSE 流式推送，AI 首字响应延迟 483ms。
>
> 基础设施层是 **GrowRPC**，NexusGo 里 Gateway 和 Logic 之间的 RPC 通信用的就是这个框架，承载了一致性哈希路由和连接池复用。自己实现的原因是深入理解 RPC 框架底层，过程中踩了两个系统级 Bug，排查和修复让我对 TCP 流、并发模型和协议边界有了非常深入的认知，这也是我认为中间件工程师最核心的能力之一。
