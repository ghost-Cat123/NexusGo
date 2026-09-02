# GrowRPC 项目介绍

---

## 简洁版（面试开场 / 简历概述，约 200 字）

GrowRPC 是一个从零实现的高性能 Go 语言 RPC 框架，核心目标是在深度理解 gRPC/net/rpc 底层原理的基础上，进行有取舍的自主设计与工程实现。

框架主要包含以下模块：**高性能 RPC 核心**（单连接多路复用、Gob/JSON/Protobuf 多协议）、**客户端连接池**（LIFO 栈 + 精准 active 计数 + 后台清理）、**Context 级联超时**（Deadline 跨网络透传）、**泛型服务路由**（Go 1.18 泛型消除反射，路由 dispatch 降至 34ns/2alloc）、**洋葱模型拦截器链**、**服务注册发现与负载均衡**（随机/轮询/一致性哈希）。

在工程实践中深度排查并解决了两类系统级难题：其一，`json.Decoder` 预读缓冲越界导致 Protobuf 流 160MB 错位死锁；其二，高并发下异步读 Body 引发的 TCP 流交错粘包。通过 `bufio.Reader` 精准接管缓冲与主循环同步按帧解包彻底修复，极限压测下 0 报错。

---

## 详细版（面试深问 / 技术陈述，含全部技术难点）

### 1. 项目概述与目标

GrowRPC 定位为**工业级 Go RPC 框架的学习型完整实现**，参考 gRPC、net/rpc 等成熟框架，独立设计并实现了：

- 自定义二进制协议（TLV 帧格式）
- 多编解码协议适配层（Codec 接口抽象）
- 单连接多路复用（pending map + Seq 分发）
- 高可用客户端连接池
- 全链路 Context 级联超时控制
- 泛型 + 闭包工厂驱动的零反射服务路由
- 洋葱模型拦截器（Interceptor Chain）
- HTTP Hijack 实现单端口多协议
- 基于 HTTP 心跳的轻量级服务注册发现
- 随机/轮询/一致性哈希负载均衡

---

### 2. 核心技术难点

#### 难点一：多协议握手切换引发的深度粘包

**背景**：客户端连接后先以 JSON 格式发送协议协商 Option（含 \n 结尾），随后立即切换为 Protobuf 二进制流。高并发下，JSON 协商包与首批 Protobuf 请求包会因 TCP Nagle 合并到同一个网络数据段。

**踩坑过程（两次演进）**：

- **第一次踩坑（数据永久丢失）**：直接用 `json.NewDecoder(conn).Decode(&opt)`。`json.Decoder` 内置 4KB 缓冲区，它不仅读走了 JSON，还把紧随其后的 Protobuf 字节一并吸入私有内存，这部分数据随 decoder 变量被 GC 回收，**永久丢失**。后续 ProtobufCodec 去 conn 读数据时读到错位的截断内容，解码失败。

- **第二次踩坑（160MB 死锁）**：用 `io.MultiReader(decoder.Buffered(), conn)` 把残余数据拼接回连接。这次数据没丢，但忽略了 `json.Encoder` 在 JSON 末尾自动追加的换行符 `\n`（ASCII 0x0a）。Protobuf 读取 4 字节大端序长度时，第一个字节是 0x0a，凑足 4 字节后解析出约 **167,772,160 字节（160MB）**。服务端阻塞等待 160MB 数据永不到来，与客户端双向死锁，极限压测下 100% 复现。

**最终解法**：

```go
br := bufio.NewReader(conn)
// 精确读取一整行 JSON（含 \n），绝对不会越界到后续 Protobuf 流
jsonLine, _ := br.ReadBytes('\n')
json.Unmarshal(jsonLine, &opt)
// 将持有残余 Protobuf 数据的 br 通过 safeBufferConn 无损传递给 Codec
bc := &safeBufferConn{Conn: conn, r: br}
server.serveCodec(f(bc), &opt)
```

`safeBufferConn` 继承 `net.Conn` 并重写 `Read` 方法为 `br.Read`，后续 Codec 对流的任何读取都经由 br，保证了 JSON 解析与 Protobuf 解码之间的字节流完全连续且无缝衔接。

---

#### 难点二：泛型路由与流交错（并发多路复用粘包）

**三阶段演进**：

**阶段一（反射时代）**：服务注册时存储 `reflect.Type` 蓝图，主循环用 `reflect.New(reqType)` 创建请求实例，在主协程同步读取 Header 和 Body，完整 req 交给子协程处理。安全，但 reflect 开销大。

**阶段二（泛型引入后的错误实现）**：注册时换用 `RegisterMethod[T, R]` 泛型，但 `sync.Map` 只能存 `interface{}`，无法携带编译期类型参数 T。主循环不知道 T 是什么，无法创建实例，于是妥协将 Body 读取推迟到异步 `go handleRequest` 子协程中。**结果**：主协程读下一个请求的 Header 与子协程读当前请求的 Body 并发竞争同一个 TCP 字节流，高并发下必然流交错乱序，解码崩溃。

**阶段三（最终方案）**：利用**闭包捕获泛型类型参数**，在注册时额外存储一个"实例工厂"：

```go
type handlerEntry struct {
    newReq  func() interface{}  // 闭包：return new(T)，0 反射
    handler MethodHandler
}
// 注册时，此时在泛型函数作用域内 T 已知
entry.newReq = func() interface{} { return new(T) }
```

主循环调用 `entry.newReq()` 即可无需反射获得正确类型的实例，再同步完成 `cc.ReadBody(reqVal)`。Header 和 Body **全部在同一主协程串行读取**，彻底消除流交错。

**Benchmark 对比结果**（Go 1.24, Intel i5-13500H）：

| 场景 | 泛型方案 | 反射方案 | 提升 |
|------|---------|---------|------|
| handler 调用开销 | 10 ns / 1 alloc | 471 ns / 6 allocs | **47x** |
| 实例创建开销 | 12 ns / 1 alloc | 18 ns / 1 alloc | 1.5x |
| 完整路由分发 | 34 ns / 2 allocs | 41 ns / 2 allocs | 1.2x |

---

#### 难点三：连接池并发安全设计

**早期 channel 方案的问题**：
1. `active` 计数与 idle 复用路径存在竞态，计数可能变负
2. `maxActive` 限制完全失效，可能创建远超预期的连接
3. `Put` 到满 channel 时走 default 分支直接关闭有效连接
4. `cleanIdle` 用瞬时 len 作为扫描次数，遍历中 Get/Put 导致扫描不完整

**最终设计（sync.Mutex + slice 栈）**：
- **LIFO 栈**：热连接（最近归还）优先复用，降低连接冷启动损耗
- **借出预占**：在锁内先 `p.active++` 预占，工厂函数在锁外执行，失败时回退，彻底消除超创建
- **优雅阻塞**：达到 maxActive 后通过有缓冲的 `waitCh` 阻塞等待；`Put` 时发通知，`Close` 时 `close(waitCh)` 唤醒全部等待者
- **cleanIdle**：持锁做完整 in-place 过滤，收集待关闭列表后在锁外关闭，避免持锁阻塞 IO
- **双重过期检查**：区分 `idleTimeout`（空闲超时）与 `maxLifetime`（连接最大存活）

---

#### 难点四：全链路 Context 超时透传

客户端调用时通过 `ctx.Deadline()` 提取绝对时间戳（毫秒）存入请求 Metadata，跨网络传递给服务端。服务端重建 `context.WithDeadline`，将精确剩余时间传给业务 handler。如果请求已消耗大部分超时时间，服务端业务层会立即触发 `context.DeadlineExceeded`，提前中止 DB 查询等下游调用，**杜绝服务端在已超时请求上的算力空转，防止级联雪崩**。

超时并发安全：`called` 与 `sent` 均为容量 1 的 buffered channel，保证即使外层 select 命中 `time.After` 先返回，内部业务 goroutine 最终完成时向 channel 写入不会阻塞，**无 goroutine 泄漏**。

---

#### 难点五：HTTP Hijack 单端口多协议

服务端实现 `ServeHTTP`，检测到 HTTP `CONNECT` 方法后调用 `hijacker.Hijack()` 夺回连接控制权，直接降级为 TCP 进行 RPC 通信。客户端对应实现 `NewHTTPClient`，先以 HTTP/1.0 发起 CONNECT 握手，再切换为 RPC 协议。**同一个端口，同一个 Listener，同时服务 HTTP 请求和 RPC 长连接**。

---

### 3. 架构总览

```
Client                               Server
  |                                    |
  |-- Dial(opt) -- JSON握手 ---------->|-- ServeConn
  |   |-- json.Encoder(bufio buffered) |   |-- bufio.Reader.ReadBytes('\n')
  |                                    |       |-- safeBufferConn 无损接管流
  |-- send(call) -- Protobuf TLV ---->|-- serveCodec (主循环, 单协程串行读)
  |   |-- sending.Lock()细粒度写锁    |   |-- readRequestHeader (同步)
  |                                    |   |-- readRequestBody   (同步)
  |-- receive() [独立goroutine]        |   |-- go handleRequest  (异步业务)
  |   |-- pending map[Seq]*Call        |       |-- 重建 Context Deadline
  |       |-- ReadHeader               |       |-- interceptor chain (洋葱)
  |       |-- ReadBody(call.Reply)     |       |-- entry.handler(ctx, req)
  |                                    |       |-- sendResponse + Lock
  |-- Pool (可选)                      |
      |-- LIFO idle stack              |-- serviceMap (sync.Map)
      |-- active 精准计数              |       |-- handlerEntry{newReq, handler}
      |-- waitCh 优雅阻塞
      |-- cleaner goroutine
```

---

### 4. 核心数据

| 指标 | 数值 |
|------|------|
| 泛型 handler 调用层 | **10 ns / 1 alloc/op**（vs 反射 471 ns / 6 allocs，提升 47x） |
| 完整路由分发 | **34 ns / 2 allocs/op** |
| Protobuf 并发压测 | 0 error，0 超时（400 req, c=100） |
| 连接池并发压测 | active 计数精准，无泄漏，无超创建 |
