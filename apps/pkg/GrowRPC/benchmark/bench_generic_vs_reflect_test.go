package benchmark

import (
	"GrowRPC"
	"context"
	"reflect"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// 共用的业务类型与处理函数（复用 service_test.go 中定义的 Args/Foo）
// ─────────────────────────────────────────────────────────────────────────────

// reflectEntry 模拟旧版反射时代在 serviceMap 中存储的结构
type reflectEntry struct {
	reqType reflect.Type                                                    // reflect.TypeOf(Args{})
	handler func(ctx context.Context, req interface{}) (interface{}, error) // 用反射调用
}

func newReflectEntry() *reflectEntry {
	var foo GrowRPC.Foo
	reqType := reflect.TypeOf(GrowRPC.Args{}) // 保存类型蓝图

	return &reflectEntry{
		reqType: reqType,
		handler: func(ctx context.Context, req interface{}) (interface{}, error) {
			// 旧版：reflect.New 创建 resp，reflect.ValueOf 取地址调用方法
			respVal := reflect.New(reflect.TypeOf(0)) // new(int)
			err := foo.Sum(ctx, req.(*GrowRPC.Args), respVal.Interface().(*int))
			return respVal.Interface(), err
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 1: 实例创建开销（newReq 工厂 vs reflect.New）
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkNew_Generic 泛型闭包工厂创建请求实例
func BenchmarkNew_Generic(b *testing.B) {
	server := GrowRPC.NewServer()
	var foo GrowRPC.Foo
	GrowRPC.RegisterMethod[GrowRPC.Args, int](server, "Foo.Sum", foo.Sum)
	entryI, _ := server.serviceMap.Load("Foo.Sum")
	entry := entryI.(*GrowRPC.handlerEntry)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = entry.newReq()
	}
}

// BenchmarkNew_Reflect 旧版反射方式创建请求实例
func BenchmarkNew_Reflect(b *testing.B) {
	re := newReflectEntry()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = reflect.New(re.reqType).Interface()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 2: handler 调用开销（type assertion vs reflect.Call）
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkCall_Generic 泛型闭包：interface 类型断言 + 直接调用
func BenchmarkCall_Generic(b *testing.B) {
	server := GrowRPC.NewServer()
	var foo GrowRPC.Foo
	GrowRPC.RegisterMethod[GrowRPC.Args, int](server, "Foo.Sum", foo.Sum)
	entryI, _ := server.serviceMap.Load("Foo.Sum")
	entry := entryI.(*GrowRPC.handlerEntry)
	ctx := context.Background()
	req := &GrowRPC.Args{Num1: 10, Num2: 20}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = entry.handler(ctx, req)
	}
}

// BenchmarkCall_Reflect 旧版反射方式调用业务函数
func BenchmarkCall_Reflect(b *testing.B) {
	var foo GrowRPC.Foo
	ctx := context.Background()

	// 旧版：通过 reflect.Value 调用方法
	fooVal := reflect.ValueOf(&foo)
	method := fooVal.MethodByName("Sum")
	argType := reflect.TypeOf(GrowRPC.Args{})
	replyType := reflect.TypeOf(0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// reflect.New 创建参数
		reqVal := reflect.New(argType)
		reqVal.Elem().Field(0).SetInt(10)
		reqVal.Elem().Field(1).SetInt(20)
		replyVal := reflect.New(replyType)

		// reflect.Call 调用
		method.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reqVal,
			replyVal,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 3: 完整路由 + 实例创建 + 调用一体（模拟 readRequest → handleRequest）
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkFullDispatch_Generic 完整路由流程（泛型）
func BenchmarkFullDispatch_Generic(b *testing.B) {
	server := GrowRPC.NewServer()
	var foo GrowRPC.Foo
	GrowRPC.RegisterMethod[GrowRPC.Args, int](server, "Foo.Sum", foo.Sum)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Step1: 主循环 findEntry（模拟 readRequest）
		entryI, _ := server.serviceMap.Load("Foo.Sum")
		entry := entryI.(*GrowRPC.handlerEntry)

		// Step2: 工厂创建实例（主循环同步）
		reqVal := entry.newReq()
		args := reqVal.(*GrowRPC.Args)
		args.Num1 = i
		args.Num2 = i + 1

		// Step3: 业务调用（模拟 handleRequest goroutine 内）
		_, _ = entry.handler(ctx, reqVal)
	}
}

// BenchmarkFullDispatch_Reflect 完整路由流程（反射）
func BenchmarkFullDispatch_Reflect(b *testing.B) {
	re := newReflectEntry()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Step1: 反射创建实例
		reqVal := reflect.New(re.reqType).Interface()
		args := reqVal.(*GrowRPC.Args)
		args.Num1 = i
		args.Num2 = i + 1

		// Step2: 反射业务调用
		_, _ = re.handler(ctx, reqVal)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 4: 并发场景（模拟多 goroutine 同时分发 RPC 请求）
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkParallel_Generic 并发泛型调用
func BenchmarkParallel_Generic(b *testing.B) {
	server := GrowRPC.NewServer()
	var foo GrowRPC.Foo
	GrowRPC.RegisterMethod[GrowRPC.Args, int](server, "Foo.Sum", foo.Sum)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entryI, _ := server.serviceMap.Load("Foo.Sum")
			entry := entryI.(*GrowRPC.handlerEntry)
			reqVal := entry.newReq()
			args := reqVal.(*GrowRPC.Args)
			args.Num1 = 1
			args.Num2 = 2
			_, _ = entry.handler(ctx, reqVal)
		}
	})
}

// BenchmarkParallel_Reflect 并发反射调用
func BenchmarkParallel_Reflect(b *testing.B) {
	re := newReflectEntry()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqVal := reflect.New(re.reqType).Interface()
			args := reqVal.(*GrowRPC.Args)
			args.Num1 = 1
			args.Num2 = 2
			_, _ = re.handler(ctx, reqVal)
		}
	})
}
