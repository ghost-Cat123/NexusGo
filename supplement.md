# 补充知识点 —— 针对得物「中间件 AI 开发工程师」岗位

> 结合项目已有内容，补充 JD 中涉及但项目暂未覆盖的关键概念，务必在面试前理解并能用自己的话表达。

---

## 一、MCP 协议（Model Context Protocol）

### 是什么？

MCP 是 Anthropic 于 2024 年 11 月开源的**标准化协议**，解决的是：AI 模型如何以统一方式连接各种外部工具和数据源。

可以把它理解为 **AI 世界的 USB-C 接口**：不同的工具（MCP Server）和不同的 AI 模型（MCP Client）只要遵循同一套协议，就能互通，无需为每对组合单独开发适配代码。

### 架构模型

```
┌─────────────┐      JSON-RPC / stdio / SSE      ┌──────────────────┐
│  MCP Client │ ◀──────────────────────────────▶ │   MCP Server     │
│  (AI模型侧)  │                                   │  (工具/数据源侧)  │
│             │                                   │                  │
│  Claude     │                                   │  search_tool     │
│  GPT        │                                   │  database_tool   │
│  你的 Agent  │                                   │  file_system     │
└─────────────┘                                   └──────────────────┘
```

### 核心能力（三类原语）

| 原语 | 说明 | 你项目对应 |
|-----|------|----------|
| **Tools** | 可被 AI 调用的函数（有副作用） | `schedule_message` |
| **Resources** | 只读数据源（文件、DB 记录等） | `search_chat_history` 返回值 |
| **Prompts** | 预制的 Prompt 模板 | System Prompt（暂无）|

### MCP vs 你项目的 Tool 调用

| 对比维度 | 你的项目 | MCP |
|---------|---------|-----|
| 工具定义方式 | Go 函数 + `InferTool` 泛型 | JSON Schema 标准描述 |
| 工具进程位置 | 同进程（Go 函数） | 独立进程（任意语言） |
| 调用协议 | Eino 内部调用 | JSON-RPC 2.0 over stdio/HTTP/SSE |
| 跨语言支持 | 不支持（只能 Go） | 支持（Python/Node/Go 等任意语言） |
| 安全隔离 | 无（同进程崩溃影响 Agent） | 有（独立进程，崩溃不影响 Agent）|

### 在 Eino 框架下使用 MCP

Eino 官方支持 MCP：`eino-ext/components/tool/mcp`

```go
import "github.com/cloudwego/eino-ext/components/tool/mcp"

// 连接 MCP Server
client := mcp.NewClient("http://localhost:3000/mcp")

// 发现 MCP Server 提供的工具
tools, _ := client.ListTools(ctx)

// 将 MCP 工具注入 Eino Agent（与本地工具完全一致）
agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
    ToolsConfig: adk.ToolsConfig{
        ToolsNodeConfig: compose.ToolsNodeConfig{
            Tools: tools,  // MCP 工具和本地工具混用
        },
    },
})
```

**面试回答要点**：MCP 让工具变成独立服务，Agent 通过标准协议调用，实现工具的语言无关和进程隔离。你的项目已经有工具安全分类的设计思想（readOnly/destructive），和 MCP 的能力声明理念一致，未来可以很自然地迁移到 MCP 架构。

---

## 二、Eino 框架核心概念

### 框架定位

CloudWeGo Eino 是字节跳动开源的**企业级 AI 应用开发框架**，核心解决的是：
1. AI 组件（LLM/Tool/Retriever/Embedder）的标准化抽象和互换
2. 复杂 AI 流程（ReAct/Chain/DAG）的编排
3. AI 应用的可观测性（Callback 体系）

### 核心组件体系

```
compose (图编排)
├── Graph         - 有向无环图，节点 + 边
├── Chain         - 线性 Pipeline
└── Branch        - 条件分支

adk (Agent 开发套件)
├── Runner        - Agent 执行引擎（管理状态机 + 中断恢复）
├── ChatModelAgent - ReAct 循环
└── Middleware     - 前/后处理钩子（你的 ApprovalMiddleware 就实现了这个）

components (标准组件接口)
├── model.ChatModel       - LLM 抽象（DeepSeek/GPT/Claude 等实现）
├── tool.BaseTool         - 工具抽象（InferTool 从函数反射生成）
├── retriever.Retriever   - 向量检索抽象（Milvus/ES 等实现）
└── embedding.Embedder    - Embedding 抽象
```

### Graph 编排（你项目待实现的 G1-G4）

```go
graph := compose.NewGraph[InType, OutType]()

// 添加节点
graph.AddChatModelNode("llm", chatModel)
graph.AddLambdaNode("router", routerFn)
graph.AddToolsNode("tools", toolsNode)

// 添加边
graph.AddEdge(compose.START, "llm")
graph.AddEdge("llm", "router")

// 条件分支
graph.AddBranch("router", &compose.GraphBranch{
    Condition: func(ctx context.Context, out string) (string, error) {
        if out == "chat" { return "llm", nil }
        return "tools", nil
    },
    EndNodes: map[string]bool{"llm": true, "tools": true},
})

compiled, _ := graph.Compile(ctx)
```

### Skill 技能包

Eino 里的 **Skill** 是比 Tool 更高层的概念：
- **Tool**：单个函数，LLM 直接调用
- **Skill**：可能包含多个 Tool + 内部 Graph 编排的复合能力

比如你的 `search_chat_history` 是一个 Tool，而「完整的搜索体验」（先关键词搜 → 失败则语义搜 → LLM 总结）可以封装为一个 Skill。

在 Eino 里，Skill 本质上是一个实现了 `tool.BaseTool` 接口但内部包含 Graph 的复合工具。

---

## 三、向量数据库原理

### 为什么需要向量数据库？

传统数据库存储结构化数据（字符串、数字），适合精确匹配（`WHERE name = '张三'`）。

语义搜索需要「相似度」而非「相等」——「数据库优化」和「MySQL 性能调优」语义相似但字符串完全不同，传统 DB 找不到关联，而向量数据库可以。

### Embedding（向量化）

把文本转成高维向量（通常 768 或 1536 维），语义相似的文本向量在空间中距离近。

```
"数据库优化" → [0.12, -0.45, 0.78, ...] (1536维)
"MySQL调优"  → [0.11, -0.43, 0.80, ...] (1536维)  ← 距离很近
"今天天气好" → [-0.89, 0.32, -0.11, ...] (1536维) ← 距离很远
```

### ANN 索引算法

**问题**：暴力计算每个向量与查询向量的距离，O(n) 复杂度，百万级向量时太慢。

**HNSW（Hierarchical Navigable Small World）** — 最常用：
```
第3层（稀疏）: 少量节点，快速定位大致区域
     │
第2层（中等）: 中等节点，缩小范围
     │
第1层（稠密）: 所有节点，精确搜索
```
搜索时从顶层出发，贪心地走向更近的节点，层层下沉，复杂度约 O(log n)。

**IVF_FLAT（倒排文件 + 暴力搜索）**：
- 先 K-Means 把向量聚成 K 个簇
- 搜索时只在最近的几个簇内暴力搜索
- 速度快但精度稍低（近似搜索）

### Milvus 架构

```
┌─────────────────────────────────────────┐
│               Milvus Standalone          │
│  ┌────────┐  ┌──────────┐  ┌─────────┐  │
│  │ Proxy  │  │ Root     │  │ Query   │  │
│  │(接入层) │  │ Coord   │  │ Node    │  │
│  └────────┘  └──────────┘  └─────────┘  │
│                   │                      │
│              etcd (元数据)                │
│              MinIO (向量数据存储)          │
└─────────────────────────────────────────┘
```

这就是为什么你的 docker-compose 里 Milvus 依赖 etcd 和 MinIO。

### 标量过滤（Hybrid Search）

Milvus 支持在向量检索时附加标量条件：

```go
// 你项目里的实际代码
filterExpr := fmt.Sprintf("conv_id == \"%s\"", convID)
retriever.Retrieve(ctx, keywords, milvusret.WithFilter(filterExpr))
```

执行顺序：先标量过滤缩小候选集，再向量近似搜索，兼顾精度和权限控制。

---

## 四、大模型工作原理（以 Claude 为例）

### Transformer 基础

所有主流 LLM 都基于 Transformer 架构，核心是 **Self-Attention（自注意力机制）**：

模型处理「我喜欢吃苹果，它很甜」时：
- 「它」要理解是「苹果」而非「我」
- Self-Attention 计算每个 token 与其他 token 的相关性权重
- 「它」会对「苹果」分配高权重，从而正确理解指代

### 自回归生成（为什么是流式）

LLM 每次只生成一个 token（大约一个词），然后把这个 token 拼到输入里，再生成下一个。

```
输入: "今天天气"
生成: "很" → 输入: "今天天气很"
生成: "好" → 输入: "今天天气很好"
生成: "。" → 停止
```

这就是为什么 SSE 流式输出是 LLM 的天然形态，而不是一个优化——本来就是一个词一个词生成的。

### Claude 的核心设计理念

Claude 是 Anthropic 开发的，有几个关键特点：

1. **Constitutional AI（宪法 AI）**：
   - 训练时给模型一套「宪法」（原则列表），让模型自我批评和修正
   - 比 RLHF（人类反馈强化学习）更可扩展，减少对大量人工标注的依赖

2. **超长上下文**：
   - Claude 3 支持 200K token 上下文（约 15 万汉字）
   - 实现方式：改进的位置编码（不用原版 Transformer 的固定位置编码）

3. **工具调用（Function Calling）**：
   - Claude 遇到需要外部信息的问题，输出结构化的工具调用请求
   - 用户/框架（如 Eino）执行工具，把结果塞回上下文，Claude 继续生成
   - 这就是你项目里 ReAct 的底层机制

4. **Context Window 与 KV Cache**：
   - 每次生成都要重新计算所有历史 token 的注意力，成本 O(n²)
   - KV Cache：缓存历史 token 的 Key-Value 矩阵，只计算新 token 的注意力
   - 你的 `TrimResultMiddleWare` 本质上是在控制 Context 长度，降低 KV Cache 内存占用和计算成本

### 为什么 LLM 会"幻觉"？你的 RAG 是怎么缓解的？

**幻觉根因**：LLM 是概率模型，生成的是「最可能的下一个 token」，不是「真实信息」。当知识截止日期后的问题、私有数据问题出现时，模型倾向于「编造」看起来合理的答案。

**你的 RAG 缓解方案**：
```
System Prompt 里显式告知模型：
"以下是从数据库检索到的真实消息记录，请仅基于这些信息回答，
 如果记录中没有相关内容，直接说不知道，不要编造。"

+ 真实检索到的消息文本
```

把「我不知道」的正确行为通过 Prompt 明确约束，加上检索到的真实 grounding 信息，可以大幅降低幻觉率。

---

## 五、得物中间件岗位重点关注方向

### 5.1 你有的（重点深挖）

- **自建 RPC**：讲清楚一致性哈希、服务发现、Protobuf 序列化
- **消息队列**：MQ 可靠性链路、死信队列、批量消费策略
- **向量数据库**：Milvus 接入、RAG 双路召回、标量过滤
- **Agent 工程化**：HITL 中断、中间件 Pipeline、三层记忆

### 5.2 你没有但 JD 提到的（了解原理，表达兴趣）

| JD 关键词 | 你需要了解的 |
|----------|-----------|
| **AI Workflow 编排** | 你已有 Eino Graph，说明理解 DAG 编排；可展开讲 G1-G4 计划 |
| **LLM 网关** | 理解其职责（路由/限流/可观测），你的 Agent 服务承担了部分功能 |
| **MCP 协议** | 见上方详解，表达愿意将工具迁移到 MCP 标准 |
| **Service Mesh** | 理解 Sidecar 模式，你的 geeRPC 是胖客户端模式，对比说明 |
| **热点探测** | 本质是频率统计 + 阈值报警，可联系 Redis 热 Key 问题 |
| **SLA 保障** | 你有死信队列、手动 ACK、幂等去重，是 SLA 保障的具体实践 |
| **K8s 容器化** | 你会 Docker Compose，K8s 是下一步；理解 Pod/Service/Deployment 概念 |

### 5.3 面试中的差异化亮点

**绝大多数实习生的项目**：CRUD + Redis 缓存 + 简单 MQ，可能有个简单 LLM 调用

**你的差异点**（按稀缺度排序）：
1. 🔥 **HITL 审批中断**：用 `StatefulInterrupt` 实现真正的人机协作，市面上极少有人做到这层
2. 🔥 **三层记忆 + Milvus 长期记忆**：不只是 Redis 存历史，有系统性的记忆压缩和向量化
3. **双路 RAG**：关键词 + 语义降级，有权限控制，不是简单的向量搜索
4. **完整中间件 Pipeline**：续写/裁剪/兜底/审批，有工程化思维
5. **自建 RPC + 一致性哈希**：理解分布式系统底层，不只是调库

**面试开场建议**：重点介绍 HITL 和三层记忆，这两个是最稀缺的，面试官大概率会顺着往下问，你有完整的代码细节可以展开。
