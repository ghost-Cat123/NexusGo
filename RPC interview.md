# GrowRPC 面试题目与答案

> 针对得物「中间件 AI 开发工程师（Golang 方向）」岗位，结合 GrowRPC 项目内容，整理高频考点与参考答案。

---

## 一、项目整体理解类

### Q1：简单介绍一下你这个 RPC 框架，为什么要自己实现？

**参考答案**：

GrowRPC 是我从零实现的 Go 语言 RPC 框架。选择自己实现的原因是：直接用 gRPC 是"黑盒"，我希望真正理解 RPC 框架内部的协议设计、流量控制、并发模型。项目实现了完整的生命周期：从 TCP 连接建立、协议握手、多路复用、请求路由、业务执行、响应返回，以及上层的连接池、注册中心、负载均衡等。更重要的是，在实现过程中深度踩了两类系统级 Bug——多协议切换时的预读缓冲越界死锁，以及泛型路由引入后的 TCP 流交错，并将排查和修复过程完整地做了工程化总结。

---

### Q2：你的 RPC 框架和 gRPC 相比有什么优势和差距？

**参考答案**：

**优势**：
- 纯路由分发层：泛型闭包工厂实现 handler dispatch 仅需 34 ns / 2 allocs，相较 gRPC 的 HTTP/2 帧处理路径（~200ns+）在极低延迟场景有竞争力
- 堆内存分配极少：完整路由只有 2 次 alloc，GC 压力低
- 协议更简洁：自定义 TLV 二进制协议，在小包场景下 parsing overhead 更低

**差距**：
- 没有 HTTP/2 的流量控制（Stream Window）和优先级调度，高并发混合负载下稳定性不如 gRPC
- 缺少 TLS、服务端反射、retry policy 等生产级特性
- 注册中心是自研 HTTP 心跳方案，不具备 etcd 的 Raft 一致性保证

这种差距是有意识的取舍，GrowRPC 的目标是在深入理解原理的同时，在纯路由层做到极致性能，而非完整替代 gRPC。

---

## 二、网络协议与粘包类

### Q3：什么是 TCP 粘包？你是如何解决的？

**参考答案**：

TCP 是流式协议，不保证消息边界。发送方多次 write 的数据可能被 Nagle 合并成一个 TCP 包，接收方一次 read 可能收到多条消息（粘包），也可能只收到半条消息（半包）。

常规解法是 TLV（Type-Length-Value）格式：先发 4 字节长度，接收方用 `io.ReadFull` 读满 N 字节。GrowRPC 的 Protobuf 编解码就是这样做的：
```
[ 4字节 Header 长度 ][ Header bytes ][ 4字节 Body 长度 ][ Body bytes ]
```

但 GrowRPC 还遇到了一个更高阶的问题：**多协议切换时的预读缓冲越界**。握手阶段用 JSON，之后切换 Protobuf，`json.Decoder` 会贪婪预读 4KB，把后续 Protobuf 字节吸入私有缓冲。这导致后续读取的 4 字节长度第一个字节是 JSON 末尾的换行符 `\n`（0x0a），大端序解析出 160MB，服务端死等 160MB 数据，双向死锁。

最终解法是用 `bufio.Reader.ReadBytes('\n')` 精确截取 JSON 行，再通过 `safeBufferConn` 将持有 Protobuf 残余数据的 `br` 无损传递给后续 Codec。

---

### Q4：bufio.Reader 的 ReadBytes('\n') 是怎么工作的？为什么能解决这个问题？

**参考答案**：

`bufio.Reader` 内部维护一个字节缓冲区（默认 4KB）和当前读指针。`ReadBytes('\n')` 会从缓冲区中一直读取，直到找到 `\n` 为止，返回包含 `\n` 的完整切片。如果缓冲区里没有 `\n`，它会继续从底层 `conn` 补充数据到缓冲区，直到找到为止。

关键点是：读完一行之后，`br` 的内部缓冲区中**剩余的字节（即 Protobuf 数据）仍然被 br 保管**，不会丢失。我们把 `br` 包装进 `safeBufferConn`，重写 `Read` 方法指向 `br.Read`，后续 ProtobufCodec 的所有读操作都经过 `br`，因此能连续正确地读取到 Protobuf 数据，而不是直接读裸 conn 导致跳过了 br 内残余的数据。

---

### Q5：你说的"主协程串行读取 Header 和 Body"为什么能防止流交错？如果是 JSON/Gob 编码会有区别吗？

**参考答案**：

一个 TCP 连接上同一时刻只有一个字节序列，如果两个 goroutine 同时对这个 conn 调用 `Read`，OS 内核只会把每一段字节给其中一个调用者，另一个会读到"乱序"的残片。在多路复用 RPC 场景下，同一连接上有多个并发请求，每个请求的 Header 和 Body 紧密相邻但不能被打散。

如果主循环读完 Header 就把 Codec 句柄交给异步 goroutine 去读 Body，主循环同时去读下一个请求的 Header，两个 goroutine 就在同一个字节流上竞争，必然流交错。

让主循环同步读完 Header+Body（原子性完成一个帧的提取），再把已经解码的内存数据交给 goroutine 处理，就彻底隔离了 IO 层和计算层的并发。

JSON/Gob 和 Protobuf 在这个问题上完全一样，都是流式读取，都会产生流交错，解法是一致的。

---

## 三、Go 并发与泛型类

### Q6：你是怎么用泛型消除反射的？注册时做了什么？

**参考答案**：

传统反射方案：注册服务时用 `reflect.TypeOf` 保存参数类型蓝图，每次请求时用 `reflect.New(reqType)` 创建实例，性能差（471 ns / 6 allocs per call）。

泛型方案的核心思路是**利用闭包在泛型函数的作用域内"冻结"具体类型**：

```go
func RegisterMethod[Req any, Resp any](server *Server, serviceMethod string,
    handler func(ctx context.Context, req *Req, resp *Resp) error) {
    
    entry := &handlerEntry{
        // 此时 Req 类型已知，闭包捕获它，返回时不需要任何反射
        newReq: func() interface{} { return new(Req) },
        handler: func(ctx context.Context, req interface{}) (interface{}, error) {
            resp := new(Resp)
            err := handler(ctx, req.(*Req), resp) // 类型断言，0 反射
            return resp, err
        },
    }
    server.serviceMap.Store(serviceMethod, entry)
}
```

主循环拿到 `handlerEntry` 后，盲调 `entry.newReq()` 就能得到正确类型的空实例，无需知道具体类型，完全零反射。这样做了到了两个目标：编译期类型安全，运行时零反射损耗。

---

### Q7：你的连接池是怎么避免 goroutine 泄漏的？

**参考答案**：

主要有两处设计：

1. **waitCh 是有缓冲 channel（容量 maxActive）**：当达到连接上限时，Get 在 `waitCh` 上阻塞。但每次 Put 都会往 `waitCh` 发一个非阻塞通知，Close 时调用 `close(waitCh)`，所有阻塞在 waitCh 的 Get 会立即被唤醒并返回 `ErrPoolClosed`，不会永久 hang 住。

2. **handleRequest 中 called/sent 是容量 1 的 buffered channel**：超时时外层 select 命中 `time.After` 直接返回，但内部业务 goroutine 还在运行。等业务完成时，它往 `called <- struct{}{}` 写，因为 channel 有 1 个缓冲，即使外层没有接收者，写入也不会阻塞，goroutine 正常退出，无泄漏。

---

### Q8：pending map 是怎么实现多路复用的？为什么用 map 而不是 channel？

**参考答案**：

客户端维护一个 `pending map[uint64]*Call`，key 是递增的请求序号 Seq，value 是对应的 Call（含 Reply 指针和 Done channel）。

发送时：拿 sending.Lock()，给 call 分配 Seq，存入 pending，写入 TCP 流，解锁。

接收时：独立的 `receive()` goroutine 持续从 TCP 流读 Header，拿到 Seq，从 pending 取出对应 Call，读取 Body 到 call.Reply，调用 `call.Done <- call` 通知调用方。

为什么用 map？因为 RPC 响应不保证按发送顺序返回（服务端可能并发处理），必须能根据 Seq 随机定位到对应的等待者。channel 是 FIFO 的，无法做到 O(1) 随机访问。map 的 O(1) 查找正好契合这个语义。这和 HTTP/2 的 stream ID 机制原理完全一样。

---

## 四、分布式与中间件类

### Q9：你的服务注册中心是怎么工作的？有什么缺陷？

**参考答案**：

GrowRPC 实现了一个基于 HTTP 的轻量心跳注册中心：
- 服务启动时向注册中心 HTTP POST 注册自己的地址
- 定期（如 5s）发送心跳 HTTP 请求续期
- 注册中心维护一个 TTL，超过心跳超时（如 10s）没有续期的服务会被剔除
- 客户端定期 GET 拉取服务列表，配合负载均衡策略选择目标实例

**缺陷**：
1. **可用性**：注册中心本身是单点，没有高可用保障
2. **一致性**：轮询拉取模型有延迟，注册中心重启会导致短暂全量丢失
3. **事件通知**：客户端只能定期轮询，无法像 etcd Watch 那样实时感知服务变更

生产环境应接入 etcd/Consul，利用 Raft 保证注册中心本身的一致性，用 Watch 机制实现实时事件推送。

---

### Q10：一致性哈希的原理是什么？你是怎么实现的？什么时候用它？

**参考答案**：

一致性哈希的核心思想是把哈希空间构造成一个虚拟环（0 到 2^32-1），每个服务节点映射到环上多个虚拟节点（Virtual Nodes，也叫 replicas），请求按 key 的哈希顺时针找到第一个虚拟节点，就是目标服务节点。

实现上用 `sort.Search` 做二分查找：
```go
// 用 key 的哈希值在已排序的虚拟节点列表中二分查找
idx := sort.Search(len(m.keys), func(i int) bool {
    return m.keys[i] >= hash
})
return m.hashMap[m.keys[idx % len(m.keys)]]
```

**优点**：节点增删时只有相邻区间的 key 需要重新路由（约 k/n 的请求受影响，k 为 key 数量，n 为节点数），普通哈希则是全量重新分配。

**适用场景**：需要会话亲和性（同一个 user 始终路由到同一个节点，如有状态缓存）；或者节点频繁扩缩容，希望最小化缓存失效率（如分布式缓存 sharding）。

---

## 五、AI/大模型方向延伸类（针对 JD）

### Q11：你了解 LLM Gateway 吗？如果让你用 RPC 框架搭一个 LLM API Gateway 你会怎么做？

**参考答案**：

LLM Gateway 是一个位于应用层和 LLM 服务之间的代理，核心功能包括：请求路由、负载均衡（多模型/多 Provider）、流量控制（Rate Limiting）、认证鉴权、可观测性（延迟/Token 统计）、流式响应（SSE/Streaming）转发。

如果用 GrowRPC 的能力延伸：
- **路由层**：`RegisterMethod` 为每个模型（GPT-4/Claude/Qwen）注册 handler，根据请求 Header 中的 model 字段路由到对应 Provider 的 HTTP 客户端
- **限流**：在洋葱拦截器中插入 `rate.Limiter`，按 APIKey 做 token bucket 限流
- **负载均衡**：多个同模型 Provider 实例之间用轮询/一致性哈希
- **流式响应**：LLM 的 SSE 流转发需要将 TCP 层的 streaming 暴露给上层，这部分需要在 RPC 协议层扩展双向流 或直接 HTTP 透传
- **可观测性**：拦截器中嵌入 OpenTelemetry Span，记录每次 LLM 调用的 prompt tokens、completion tokens、延迟

实际生产中会直接用 gRPC + Envoy/MOSN 做 Service Mesh，Gateway 侧用 Golang 写控制面逻辑。

---

### Q12：什么是 MCP？和 RPC 有什么关系？

**参考答案**：

MCP（Model Context Protocol）是 Anthropic 提出的一个开放协议，目标是让 AI 模型（如 Claude）能够标准化地调用外部工具（数据库、文件系统、API 等）。核心思想是：模型作为 Client，工具服务作为 Server，通过 JSON-RPC 2.0 协议通信，定义了 `tools/list`、`tools/call`、`resources/read` 等标准方法。

和 RPC 的关系：MCP 本质上就是一套特化的 RPC 协议，针对 AI Agent 的工具调用场景做了语义层的标准化。如果让我用 GrowRPC 实现一个 MCP Server，可以将每个 Tool 用 `RegisterMethod` 注册，toolName 作为 serviceMethod，输入/输出 schema 用 Protobuf 定义，上层 Agent 通过标准 RPC 调用触发工具执行。GrowRPC 的洋葱拦截器天然适合在 tool call 前后注入权限检查、参数校验、调用日志等横切逻辑。

---

## 六、Go 语言底层类

### Q13：你的框架中 sync.Map 和普通 map+RWMutex 分别用在哪里？为什么这样选择？

**参考答案**：

- **serviceMap 用 `sync.Map`**：注册服务（写）只发生在启动阶段，之后几乎全是并发读（路由查找）。`sync.Map` 针对"多读少写"场景做了优化，通过 read map + dirty map 的双层结构，读操作无锁（atomic.Load），性能极好。

- **pending map 用普通 map + sync.Mutex**：pending 的读写非常频繁且均衡（每次请求都要 registerCall + removeCall），`sync.Map` 的 dirty 晋升机制在频繁写场景下反而有额外开销。用 `sync.Mutex` + 普通 map 更直接高效。

- **Pool 的 idle 用 `sync.Mutex` + slice**：连接池需要维护严格的计数一致性和精准的 active 状态，必须在同一把锁内完成读-判断-写的原子操作，RWMutex 在这里帮助不大，简单的 Mutex 反而语义更清晰。

---

### Q14：Go 的 goroutine 和 OS thread 有什么区别？GMP 模型是什么？

**参考答案**：

OS thread 是内核调度的，栈固定（通常 1~8MB），创建/切换需要系统调用（~1μs 量级）。

goroutine 是 Go runtime 调度的用户态轻量线程，初始栈只有 2KB，可动态扩缩（最大 1GB），创建/切换在用户态完成（~100ns 量级），Go runtime 维护了一个调度器（GMP 模型）将 goroutine 映射到 OS thread 上。

GMP 模型：
- **G（Goroutine）**：待运行的协程
- **M（Machine）**：OS 线程，实际执行代码
- **P（Processor）**：调度上下文，持有 goroutine 本地运行队列（Local Run Queue）；GOMAXPROCS 控制 P 的数量，通常设为 CPU 核数

G 通过 P 调度到 M 上运行。当 G 发生系统调用阻塞时，M 会和 P 解绑，让 P 继续调度其他 G 到新的 M 上，实现 IO 密集型高并发而不阻塞 CPU。

在 GrowRPC 的场景中，`receive()` goroutine 阻塞在 TCP Read 时，底层是 Go 的 netpoll（epoll/IOCP），G 被 park 挂起，M/P 被释放去跑其他 G，IO 就绪时再被 runtime 唤醒，这就是为什么单连接能高效多路复用的底层原理。

---

### Q15：GrowRPC 的拦截器（洋葱模型）是怎么实现的？

**参考答案**：

洋葱模型通过函数包装实现调用链：

```go
type HandlerFunc func(info *CallInfo) error
type Interceptor func(next HandlerFunc) HandlerFunc
```

注册时，拦截器链按逆序依次包装核心 handler：

```go
var handler HandlerFunc = func(i *CallInfo) error {
    resp, err := req.entry.handler(i.Ctx, req.decoded)
    respData = resp
    return err
}
for i := len(server.interceptors) - 1; i >= 0; i-- {
    handler = server.interceptors[i](handler)
}
err := handler(info)
```

最外层的拦截器最先执行前置逻辑，最后执行后置逻辑，形成"洋葱"结构。这和 Gin 的 middleware、gRPC 的 interceptor 原理完全一致。

优点：零侵入业务代码，可组合，每个拦截器只负责一件事（日志、限流、recover 等），符合单一职责原则。
