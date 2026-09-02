# Agent 项目面试话术

> 面向 Agent 开发实习岗位，侧重展示设计思考、AI 开发理解和工程深度。

---

## 一、项目背景：为什么做这个项目？

### 起因

我见过不少项目把 AI 当成"API 调一下"，用户发消息 → 调 LLM → 返回结果。这种用法最大的问题是：**LLM 只是一个外挂，不是系统的一部分**。真正的 AI Agent 应该能像人一样使用工具、记住上下文、在多个步骤间推理。

所以我决定做 NexusGo——不是因为市面上没有 IM，而是因为**市面上缺少一个把 AI Agent 深度融入即时通讯的参考实现**。

### 我想要验证的核心问题

| 问题 | 为什么重要 |
|------|-----------|
| AI 推理和消息路由怎样才能互不阻塞？ | 如果 LLM 调用 3 秒，期间 WebSocket 连接不能卡死 |
| Agent 多轮对话怎么记住上下文不丢？ | 用户说"接着上次的继续"，AI 不能一脸茫然 |
| 怎么让 Agent 安全地执行"操作"？ | "帮我给张三发消息"和"帮我搜聊天记录"，风险完全不同 |
| RAG 怎么在 IM 场景落地？ | 用户搜"吃饭"，原文是"聚餐"，关键词搜索会漏 |
| MCP 怎么让 Agent 的能力可扩展？ | 不能每次加功能都要改 Agent 代码 |

### 借鉴与差异

- **设计思路**参考了 LangChain 的 Agent 抽象（感知→推理→行动循环）、RAGFlow 的检索增强理念
- **框架选型**用了字节跳动的 Eino 而不是直接调 OpenAI API——Eino 封装了 ReAct 循环、工具管理、流式输出、中断恢复
- **代码完全自己写的**，只是在架构设计上向成熟方案对齐，理解为什么这样设计之后才动手

---

## 二、Agent 架构设计（最可能被深挖）

### 为什么拆成独立微服务，而不是一个函数调用？

面试官可能会这样问，我的回答逻辑线：

**第一步：说现象**

最早我的 Agent 是嵌入在 Logic 服务里的一个 goroutine。用户发 AI 消息 → Logic 调 LLM → 流式返回。这种设计在工作初期没有任何问题——直到我开始做压测。

**第二步：说问题**

50 个并发 WS 连接中，如果同时来了 10 条 AI 请求，Logic 的 goroutine 全被 LLM 调用占满（一次调用 3-5 秒）。**这 10 个 AI 请求占用的 goroutine 当时正在等待网络 IO，看起来不消耗 CPU——但实际上每个 goroutine 都绑着一个 delivery tag，MQ 认为这些消息没被 ACK，不会给 Logic 投新消息。**

也就是说，**AI 的 IO 等待间接阻塞了消息落库。**

**第三步：说决策**

两个方案摆在面前：
- 方案 A：给 AI 请求单独开一个更大的 goroutine 池（治标不治本——池再大也会被 LLM 延迟占满）
- 方案 B：把 AI 拆成独立服务（AI 崩了不影响消息；扩容策略也不同——Logic 是 CPU 密集型要堆实例，Agent 是 IO 等待型要调并发数而不是实例数）

我选了 B。拆完之后 AI 和消息两个域完全隔离，各自出问题不会互相拖垮。

**第四步：说架构收益**

```go
// Agent 只提供一个 SSE 端点，无状态（session 存 Redis 中）
r.GET("/agent/chat/sse", handler.ChatSSE)

// Gateway 通过 HTTP GET 拉 Agent 的 SSE 流，逐 chunk 推 WS 给用户
// Agent 不需要知道 MQ、不需要知道消息路由，边界清晰
```

### 为什么用 SSE 而不是 WebSocket 直连 Agent？

面试官可能问："Agent 和 Gateway 之间为什么用 HTTP SSE，而不是开一个 WebSocket？"

我的回答：

1. **架构更干净**。Gateway 已经管了所有客户端的 WS 连接。如果 Agent 也开 WS，一条 AI 请求变成两条 WS（客户端→Gateway + Gateway→Agent），增加连接管理的复杂度。
2. **SSE 天然单向**。Agent 只需要向 Gateway 推流，不需要接收 Gateway 的推送——SSE 的语义就是单向流，比 WebSocket 更贴合这个场景。
3. **之前用的是 Redis PubSub**，但 PubSub 是 fire-and-forget 的——如果 Gateway 重启,订阅中断期间的消息会丢失。换成 SSE 之后，Agent 直接推给 Gateway，Gateway 不在了就报错，不存在丢消息问题。

---

## 三、AI 开发使用的理解（Tool/Function Call/MCP/RAG/Prompt）

### Tool Call（Function Calling）

面试官可能问："你是怎么理解 Function Calling 的？你项目里怎么用的？"

我的理解：

**Function Calling 不是 LLM 执行函数，而是 LLM 说"我建议调用这个函数"**。LLM 只返回一个 JSON（工具名 + 参数），真正执行的是我们写的 Go 代码。执行完结果再喂回 LLM，形成 ReAct 循环。

我项目里的实际运用：

```go
// Eino 框架的 InferTool 自动从 Go struct 生成 function schema
// LLM 看到的 schema:
{
  "name": "search_chat_history",
  "description": "查询与特定用户的历史聊天记录，支持时间范围和关键词过滤",
  "parameters": {
    "target_user": {"type": "string", "description": "单聊对象用户名"},
    "keywords": {"type": "string", "description": "关键词，空格分隔"},
    ...
  }
}

// 用户说"搜我和张三关于数据库的聊天"
// LLM 自动填充: target_user="张三", keywords="数据库"
// 我的代码执行: FindUserByName → SQL FULLTEXT 搜索 → 返回结果给 LLM
```

关键设计点：**我让 LLM 只填参数，不让它建议 SQL**。LLM 对数据库结构的理解是不可靠的，让它生成 SQL 会引入幻觉。正确的做法是 LLM 填业务参数（用户名、关键词），Go 代码构造安全的参数化查询。

### MCP（Model Context Protocol）

面试官可能问："你项目里的 MCP 是怎么用的？解决了什么问题？"

我的理解：

MCP 的核心价值是**工具解耦**。传统 Agent 的开发模式是：Agent 代码中硬编码工具（import → 注册 → 调用）。加一个新工具需要改 Agent 代码、测试、重新部署。

MCP 把工具变成独立进程（MCP Server），通过标准化 JSON-RPC 协议暴露。Agent（MCP Client）通过 `tools/list` 自动发现可用工具，通过 `tools/call` 调用，完全不需要重新部署。

我项目里的实际运用：

```
IM Agent（你的项目）
    │
    │ MCP Client 连接
    │
    ├── 内部 MCP Server（IM 工具集）
    │   ├── search_chat_history  → MCP Tool
    │   └── schedule_message     → MCP Tool
    │
    └── 外部 MCP Server（第三方工具）
        ├── 天气查询
        └── 联网搜索
```

设计思考：**为什么内部工具也要走 MCP？**

代码直接调 `dao.SearchHistoryMessages()` 更快,为什么还要包一层 MCP？三个原因：

1. **协议标准化**。如果未来要接一个 Python 写的网页端 AI 助手，它通过标准 MCP 协议就能调用 Go 写的 IM 工具，不需要重写
2. **工具发现是动态的**。MCP Server 启动时 Agent 通过 `tools/list` 自动发现，不需要硬编码 tool name
3. **安全边界**。MCP 进程可以限制权限，即使 Agent 被越权调用了也不怕

### RAG（检索增强生成）

面试官可能问："你的 RAG 为什么设计成双路（关键词 + 语义）？"

我的理解：

**RAG 的核心矛盾是精确度 vs 召回率的取舍。**

- **关键词搜索（FULLTEXT）**：精确匹配，用户说"数据库"就找"数据库"，不会跑偏。但用户说"崩溃"，原文是"挂了"就找不到
- **语义搜索（Milvus 向量）**：语义相似，"崩溃"能找到"挂了"、"报错"、"闪退"。但可能返回不相关的结果（"数据库"搜出"数据结构"，不是用户要的）

**我的设计是串行降级，不是并行融合**：

```
关键词搜索（FULLTEXT Boolean 模式）
    │
    ├── 有结果 → 直接返回          （精确度高，优先）
    │
    └── 无结果 → 语义降级
         ↓
    Embedding(关键词) → Milvus 向量检索
         ↓
    msg_id 列表 → 回表 MySQL 取真实文本
         ↓
    LLM 总结返回
```

选择串行的原因（面试官追问时答）：

- 关键词匹配结果质量更高，有结果时语义召回是冗余的
- 并行搜索需要融合两路排名（RRF 算法），增加复杂度和延迟
- Milvus 搜索有 Embedding API 调用成本，能省则省

### Prompt 工程

面试官可能问："你的 Agent 用了 Prompt 工程吗？"

我的回答：

我项目里 Prompt 工程体现在两个层面：

**1. Tool Schema 是隐式的 Prompt**

Go struct 的 `jsonschema` tag：

```go
type SearHistoryReq struct {
    TargetUser  string `json:"target_user" jsonschema:"description=单聊对象用户名, 与target_group至少填一个"`
    Keywords    string `json:"keywords" jsonschema:"description=关键词,用空格分隔,例如'数据库 密码'"`
    ...
}
```

这句话不是给代码看的，是给 LLM 看的。LLM 通过 function calling schema 理解工具的用法。写得好不好直接影响填参准确率。

**2. 中间件本质上是 Prompt 的前后处理**

`TrimResult` 中间件把工具返回结果裁剪为 `"[result omitted] (~5000 chars)"`，这不是给用户看的——是**给 LLM 看的**。目的是在不丢失"这里有一个工具调用"的提示的前提下，节省 Context token。

**3. 我刻意保持 System Prompt 极简**

```go
instruction := "You are a helpful assistant."
```

不做复杂的角色扮演 Prompt，理由：
- 工具描述（Tool Schema）已经告诉 LLM 它能做什么
- ReAct 循环本身会从上下文推导意图
- Prompt 越长越容易产生幻觉和意外行为

### 对 Agent 产品的看法

面试官可能问："你觉得一个好的 Agent 产品应该是什么样的？"

我的看法：

**首先是可靠性 > 酷炫度。** 很多 Agent Demo 视频里看起来很强，一上线就挂——因为幻觉导致的错误操作没有防护。我项目里给每个破坏性工具都接了 HITL 审批，这不是技术秀，是**对用户负责**。

**其次是增量交付。** 不是一开始做全自动 Agent，而是逐步放宽权限：
- Stage 1：只读工具（搜索），不需审批
- Stage 2：写入工具（发消息），需用户确认
- Stage 3：根据用户历史行为自动判断是否需要确认（Trust Level）

**最后是可观测性。** Agent 内部发生了什么，必须可追踪。每次 ToolCall 的参数、执行结果、LLM 的思考过程都要打日志——不看日志你永远不知道 Agent 为什么做错了。

---

## 四、Agent 开发中的踩坑（最能展示思考深度）

### 踩坑 1：从"写死回调"到"中间件 Pipeline"

最早我在每个工具函数里硬编码"执行前检查权限"、"执行后打日志"。

问题暴露：加一个新中间件（比如限流）要改所有工具函数。

**设计演进**：用 Eino 的中间件机制，把横切关注点抽象为独立中间件，声明式注入：

```
请求进入 → RateLimitMiddlware → ApprovalMiddleware → 工具执行
                                                        ↓
                                                   SafeMiddleware（兜底不崩溃）
```

现在加一个新能力（如 Token 统计），只需注册一个中间件，不改任何工具代码。

### 踩坑 2：Multi-step Tool Call 的 Context 爆炸

多轮 ReAct 中，每轮工具返回的内容都追加到 Context。3 轮下来 Context 可能从 500 token 膨胀到 5000 token。

**我的解法**：不保存工具结果的全文，只保存一个 `"[result omitted]"` 标记。这样 LLM 知道"发生过这件事"，但不消耗大量 Token 去看全文。这个设计借鉴了 LangChain 的 `MessagesPlaceholder` 理念。

### 踩坑 3：Milvus 和 MySQL 的双写一致性问题

每条消息要同时写 MySQL（持久化）和 Milvus（向量检索）。如果同步写，每消息多 50ms 延迟。

**我的解法**：不追求强一致。MySQL 写完后异步发到 MQ `im.vector.index` 队列，独立消费者批量写入 Milvus。MySQL 中的数据是最小可用集——即使 Milvus 挂了，关键词搜索兜底。

---

## 五、如果面试官问"你有什么想做的 Agent 功能？"

结合群聊场景，3 个有深度且易于实现的方向：

1. **Agent 群协作**：多个 Agent 各司其职——一个负责会议纪要，一个负责待办提醒，通过群聊协作。比单 Agent 更贴近真实办公场景

2. **Agent 记忆的用户画像**：Agent 记住每个用户的偏好（"张三喜欢简洁回答"），主动调整回复风格

3. **Agent 独立评估自己的回答质量**：每次回复后自我评分，低分的自动重试或提醒用户请其他人帮忙

---

## 六、项目链接与展示注意

面试官如果有技术背景，重点展示：

- Agent 的 ReAct 循环图和中间件 Pipeline
- 压测数据和优化前后对比
- RAG 双路检索的架构图
- HITL 中断审批的完整时序

面试官如果是产品/设计背景，重点展示：

- 用户场景（群里@AI 搜聊天记录、定时提醒）
- 中间件为什么必要（安全性、可靠性）
- Agent 和用户之间的交互契约（什么操作需确认、什么时候放心自动执行）

---

## 七、押题：最可能被问到的几个问题及应对

**Q：你为什么要手写 Agent 而不是用 LangChain/CrewAI？**

A：两个原因。一是对学习而言，用框架会屏蔽底层细节——ReAct 循环到底是怎么走的、工具是怎么被 LLM 填充参数的，不自己写过一遍永远不知道。二是 Go 生态里目前缺少成熟的 Agent 框架，我用 Eino 是因为它本身就是字节在生产的框架，比 LangChain Go 更稳定。

**Q：你的 Agent 和 ChatGPT 的插件有什么区别？**

A：ChatGPT 的插件是中心化的——OpenAI 审核、上线、管理。我的 Agent 工具是自己定义的，融合了 IM 的数据（聊天记录、群成员），LLM 不能直接访问这些数据，必须通过 Tool 间接操作。权限控制也自己做——哪些能自动执行、哪些需要用户确认。

**Q：你觉得你的 Agent 最薄弱的地方是什么？**

A：评测。我目前靠人工测试——跑一轮对话看看结果对不对。没有自动化评测 pipeline（比如用另一 LLM 判断回答质量）。这是下一步要补的——否则每次改 Prompt 或换模型都不知道有没有倒退。

**Q：如果让你用 AI 重新设计这个项目，你会怎么改进？**

A：对话流程——设计和决策的过程仍然需要人去思考和权衡，但代码实现效率确实比没有 AI 辅助的时候高很多。有了 AI 辅助之后，我能够把更多时间花在思考"为什么这样设计"和"可能出现什么问题"上，而不是花在查 API 文档或手写重复代码上。所以如果再设计一次，瓶颈不在代码实现，而是对每个设计决策的 trade-off 思考是否够深——AI 帮你跑代码，但架构判断必须自己做。
