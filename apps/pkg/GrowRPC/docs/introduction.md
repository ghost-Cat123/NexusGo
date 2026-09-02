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

### 3.1 完整请求/响应生命周期（四阶段）

**阶段一：握手与流接管（连接建立时执行一次）**

客户端用 `json.Encoder(conn).Encode(opt)` 发送协议协商包（含 `\n`）。服务端用 `br.ReadBytes('\n')` 精确截取这一行，`\n` 之后的所有字节（即紧随其后的 Protobuf 数据）仍然安全地保存在 `br` 的内部缓冲区中。随后把 `br` 包装进 `safeBufferConn`（重写 `Read` 指向 `br.Read`），后续 Codec 的所有读操作都经过 `br`，**残余 Protobuf 数据一个字节不丢**。根据 `opt.CodecType` 找到工厂函数 `f`，调用 `f(bc)` 构造出 `ProtobufCodec`，传入 `serveCodec` 开始事件循环。

**阶段二：客户端发送请求（并发多路复用）**

N 个 goroutine 并发调用 `Call()`，各自经过 `registerCall()` 拿到唯一递增的 `Seq`，存入 `pending[Seq]=call`，然后竞争 `sending.Lock()` 排队写 TCP 流。`sending` 只在 `cc.Write()` 时持有，写完即释放，确保 TLV 帧完整性，goroutine 写完后立即返回，通过 `call.Done` channel 异步等待响应。

**阶段三：服务端接收与分发（主循环串行读 + 异步业务）**

`serveCodec` 主循环（单 goroutine）依次 `ReadHeader` → `findEntry` → `entry.newReq()` 创建实例 → `ReadBody` 完成完整帧的原子读取。组装好 `Request{header, entry, decoded}` 后，派发给 `go handleRequest` 异步处理，主循环立即回到顶部读下一帧，实现读取串行、业务并发。

`handleRequest` 内部：从 `Metadata["deadline"]` 重建 `context.WithDeadline`，经洋葱拦截器链调用 `entry.handler(ctx, req)`，完成后通过 `sending.Lock()` 保护写回响应。

**阶段四：客户端接收响应（receive goroutine 统一分发）**

独立的 `receive()` goroutine 持续从 TCP 流读响应 Header，取 `Seq` 从 `pending` 取出并删除对应 `Call`，再 `ReadBody` 到 `call.Reply`，最后 `call.Done <- call` 通知调用方。调用方 `select` 同时监听 `ctx.Done()`（超时/取消）和 `call.Done`（正常完成），任意一个先触发则返回。

---

### 3.2 多路复用机制详解

#### Seq 序号 + pending map：HTTP/2 Stream ID 同款原理

```
pending map[uint64]*Call

发送时：seq++ → pending[seq]=call → cc.Write(header{Seq}, body)
接收时：ReadHeader → seq → call=pending[seq] → delete(pending[seq])
                          → ReadBody(call.Reply) → call.Done <- call
```

TCP 流上的请求有序写入，但**响应可以乱序返回**（服务端多个 handleRequest goroutine 并发执行，先完成的先发响应）。pending map 实现了 O(1) 的「响应到 Call 的映射」，这与 HTTP/2 的 Stream ID 机制原理完全一致。

#### sending 细粒度锁：防帧交织保证 TLV 完整性

```
// 无锁时（错误场景）：
goroutine-1 写：[hdr1.......][body1.......]
goroutine-2 写：   [hdr2.......][body2.......]
TCP 流：[hdr1][hdr2][body2][body1]  <- 帧交织，接收端 TLV 解析错误

// sending 锁保护后（正确场景）：
goroutine-1：Lock -> [hdr1][body1] -> Unlock
goroutine-2：                         Lock -> [hdr2][body2] -> Unlock
TCP 流：[hdr1][body1][hdr2][body2]   <- 帧完整，TLV 正确解析
```

`sending` 只在 `cc.Write()` 时持有，写完即释放，粒度极细。注意 `mu`（client 锁）和 `sending`（写流锁）职责严格分离：`mu` 只保护 pending map 和 seq 的读写，`sending` 只保护实际 IO 写入，两把锁互不阻塞，支撑了高并发下的双向并发写入。

#### 双 channel 防止超时场景下的 goroutine 泄漏与重复响应

```go
called := make(chan struct{}, 1)  // 容量 1，关键！
sent   := make(chan struct{}, 1)  // 容量 1，关键！

// 内部业务 goroutine：
go func() {
    resp := entry.handler(ctx, req)  // 业务执行
    called <- struct{}{}             // 信号 1：业务完成
    sendResponse(resp, sending)      // 加锁发送响应
    sent <- struct{}{}               // 信号 2：响应已发
}()

// 外层超时监听：
select {
case <-time.After(timeout):
    // 超时路径：外层发超时错误响应
    sendResponse(error_resp, sending)
    // 内部 goroutine 最终完成时，called/sent 因有 1 缓冲不会阻塞，goroutine 正常退出
    // 客户端侧：收到超时响应时已 removeCall(Seq)，
    // 内部 goroutine 后发的真实响应到达时，pending[Seq] 已空，receive() 直接丢弃
    // => 两次响应不会被同时处理 (不重复)
case <-called:
    <-sent  // 正常路径：等内部 goroutine 的 sendResponse 也完成
}
```

**为什么 called/sent 必须有缓冲？**
超时时外层 return 后不再读 called/sent。若 channel 无缓冲，内部 goroutine 写 `called <-` 将**永久阻塞，goroutine 泄漏**。容量为 1 的缓冲保证内部 goroutine 能顺利写入并自然退出，彻底杜绝泄漏。

---

### 4. 核心数据

| 指标 | 数值 |
|------|------|
| 泛型 handler 调用层 | **10 ns / 1 alloc/op**（vs 反射 471 ns / 6 allocs，提升 47x） |
| 完整路由分发 | **34 ns / 2 allocs/op** |
| Protobuf 并发压测 | 0 error，0 超时（400 req, c=100） |
| 连接池并发压测 | active 计数精准，无泄漏，无超创建 |
