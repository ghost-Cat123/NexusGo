package main

import (
	"GrowRPC"
	"GrowRPC/midware"
	"context"
	"log"
	"net"
	"time"
)

// =======================================================
// 1. 准备测试业务代码
// =======================================================

type MathArgs struct {
	A, B int
}

type MathReply struct {
	Result int
}

type MathService struct{}

// Add 正常的方法
func (m *MathService) Add(_ context.Context, args *MathArgs, reply *MathReply) error {
	// 模拟一点业务耗时，让 Logger 拦截器能打印出时间
	time.Sleep(time.Millisecond * 50)
	reply.Result = args.A + args.B
	return nil
}

// Divide 故意引发 Panic 的方法（测试 Recovery 拦截器）
func (m *MathService) Divide(_ context.Context, args *MathArgs, reply *MathReply) error {
	if args.B == 0 {
		// 这里故意触发 panic，而不是 return error
		panic("divide by zero error triggered intentionally!")
	}
	reply.Result = args.A / args.B
	return nil
}

// =======================================================
// 3. 启动服务端
// =======================================================

func startServer(addr chan string) {
	// 【核心：注册中间件】
	// 注意顺序：最外层是 Logger，往里一层是 Recovery。
	// 洋葱模型执行顺序：Logger进 -> Recovery进 -> 核心逻辑 -> Recovery出 -> Logger出
	GrowRPC.Use(midware.LoggerInterceptor, midware.RecoveryInterceptor)

	// 注册服务
	mathService := new(MathService)
	GrowRPC.RegisterMethod[MathArgs, MathReply](GrowRPC.DefaultServer, "MathService.Add", mathService.Add)
	GrowRPC.RegisterMethod[MathArgs, MathReply](GrowRPC.DefaultServer, "MathService.Divide", mathService.Divide)

	// 监听端口
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("RPC 服务端启动成功，地址:", l.Addr().String())
	addr <- l.Addr().String()

	// 接收请求
	GrowRPC.Accept(l)
}

// =======================================================
// 4. 客户端测试逻辑
// =======================================================

func main() {
	// 取消日志的时间前缀，让输出更干净，方便我们观察
	log.SetFlags(0)

	addr := make(chan string)
	go startServer(addr)

	// 获取服务端动态端口
	serverAddr := <-addr

	// 创建客户端
	client, err := GrowRPC.Dial("tcp", serverAddr, GrowRPC.DefaultOption) // 如果你测了 Protobuf，把 Option 改一下即可
	if err != nil {
		log.Fatal("dial error:", err)
	}
	defer client.Close()

	time.Sleep(time.Second) // 等待服务端就绪

	log.Println("\n--- 测试案例 1：正常的 RPC 调用 ---")
	args := &MathArgs{A: 10, B: 20}
	reply := &MathReply{}
	err = client.Call(context.Background(), "MathService.Add", args, reply)
	if err != nil {
		log.Printf("客户端收到错误: %v", err)
	} else {
		log.Printf("客户端收到结果: %d + %d = %d", args.A, args.B, reply.Result)
	}

	time.Sleep(time.Second)

	log.Println("\n--- 测试案例 2：故意触发 Panic 的 RPC 调用 ---")
	// 除数设为 0，触发业务代码里的 panic
	panicArgs := &MathArgs{A: 10, B: 0}
	panicReply := &MathReply{}
	err = client.Call(context.Background(), "MathService.Divide", panicArgs, panicReply)

	// 预期：服务端不会崩溃，且客户端能优雅地收到 err
	if err != nil {
		log.Printf("客户端成功收到服务端兜底错误: %v", err)
	} else {
		log.Printf("客户端收到结果 (不应该走到这里): %v", panicReply.Result)
	}

	time.Sleep(time.Second)
	log.Println("\n测试完成！服务端没有因为 Panic 退出，中间件工作正常！")
}
