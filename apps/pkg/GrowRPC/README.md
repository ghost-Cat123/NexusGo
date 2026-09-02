# GrowRPC

GrowRPC 是一个轻量级、高性能的 Go 语言 RPC 框架，提供了完整的服务端与客户端通信能力，支持服务注册与发现、负载均衡、中间件等特性。

## 功能特性

- **完整的 RPC 通信机制**：支持多种编码格式（Gob/JSON/Protobuf），实现了请求序列化与反序列化、方法调用、错误处理等核心功能
- **自定义协议头**：设计了自定义协议头，避免 TCP 流式处理产生的粘包问题，确保数据传输的完整性
- **HTTP 协议支持**：通过 HTTP CONNECT 方法实现 RPC 通信，兼容现有 HTTP 基础设施
- **服务注册与发现**：基于 HTTP 的服务注册中心，支持服务心跳检测、超时剔除
- **负载均衡**：支持多种负载均衡策略（随机/轮询/一致性哈希）

- **中间件系统**：可插拔的中间件机制，支持请求拦截、日志记录、异常恢复等功能
- **完善的超时与错误处理**：实现了连接超时、处理超时等机制，提高系统稳定性
- **级联超时控制 (Context 透传)**：支持将客户端上下文中的 `Deadline` 穿越网络传递至服务端，实现跨服务级联超时中断，避免服务端空转浪费资源
- **泛型无反射调用**：基于 Go 1.18+ 泛型重构底层服务路由，废弃 `reflect`，实现编译期严格类型检查和运行期零反射损耗的极致性能


## 演进路线 (TODO List)

为了打造工业级的现代微服务框架，GrowRPC 正在执行以下升级演进路线：

- [x] **Context 链路透传机制**：利用自定义协议头，实现超时时间和元数据跨网络级联透传与取消，防止服务端空转。
- [x] **防粘包与泛型改造**：结合自定义 TLV 二进制包与 Protobuf 编解码，摒弃性能低下的运行时 `reflect` 反射，基于 Go 1.18 泛型闭包实现零装箱损耗的方法注册与动态路由。
- [x] **客户端连接池设计**：维护多路复用连接池，彻底解决单条长连接遇到网络抖动时的 TCP 队头阻塞 (Head-of-Line Blocking) 问题。
- [ ] **云原生注册中心升级**：抛弃简单的内存服务发现，接入 **ETCD** 作为注册中心，基于 Watch 机制实现服务动态发现，并引入 Raft 算法和租约机制解决分布式一致性与选主问题。
- [ ] **Reactor 事件驱动网络层**：重构底层传输层，将阻塞型 `net` 库替换为类似 `gnet` 或 `Netpoll` 等基于 epoll (Multi-Reactors) 的高效事件驱动网络库，冲击海量并发的极限性能。

## 目录结构

```
GrowRPC/
├── codec/           # 编解码器实现
│   ├── pb/          # Protobuf 相关文件
│   ├── codec.go     # 编解码器接口
│   ├── gob.go       # Gob 编解码器
│   ├── json.go      # JSON 编解码器
│   └── protobuf.go  # Protobuf 编解码器
├── main/            # 示例代码
│   └── main.go      # 主示例
├── midware/         # 中间件实现
│   └── interceptor.go # 中间件接口和实现

├── registry/        # 服务注册中心
│   └── registry.go  # 注册中心实现
├── xclient/         # 扩展客户端
│   ├── consistent_hash.go # 一致性哈希实现
│   ├── discovery.go # 服务发现接口
│   ├── discovery_grow.go # 基于 GrowRegistry 的服务发现
│   └── xclient.go   # 支持负载均衡的客户端
├── client.go        # 基础客户端实现
├── client_test.go   # 客户端测试
├── debug.go         # 调试工具
├── go.mod           # Go 模块定义
├── go.sum           # 依赖校验和
├── server.go        # 服务端实现
└── service.go       # 服务定义和注册
```

## 安装

```bash
go get -u github.com/ghost-Cat123/GrowRPC
```

## 负载均衡策略

GrowRPC 支持以下负载均衡策略：

- **RandomSelect**：随机选择服务实例
- **RoundRobin**：轮询选择服务实例
- **ConsistentHash**：基于一致性哈希选择服务实例

## 中间件

GrowRPC 内置了以下中间件：

- **LoggerInterceptor**：记录请求日志和执行时间
- **RecoveryInterceptor**：捕获并恢复 panic，防止服务崩溃

## 编码格式

GrowRPC 支持以下编码格式：

- **Gob**：Go 语言特有的编码格式，性能优异
- **JSON**：通用性好，便于调试
- **Protobuf**：高效的二进制编码格式，适合跨语言场景

## 性能特性

- **并发处理**：服务端采用 goroutine 处理每个连接，支持高并发
- **轻量级锁机制**：发送请求时使用轻量级写锁，确保并发安全的同时最小化性能开销
- **多路复用**：类似 Epoll 机制，通过维护 pending map 和后台 receive 协程，实现单连接多路复用
- **高效编码**：支持多种编码格式，可根据场景选择最优方案
- **负载均衡**：通过多种负载均衡策略，提高系统整体性能

## 贡献

欢迎提交 Issue 和 Pull Request 来帮助改进 GrowRPC！
