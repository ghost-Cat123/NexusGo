package GrowRPC

import (
	"context"
	"fmt"
)

// DecodeFunc 是主循环用来把流数据读入到 req 的函数
type DecodeFunc func(v interface{}) error

// ErrNotImplemented 导出变量
var ErrNotImplemented = fmt.Errorf("method not implemented")

// MethodHandler 定义底层的通用处理函数
// 调用前 Body 已经被主循环同步 decode 进 req（通过 reqFactory + DecodeFunc）
// handler 只负责：业务调用 + 返回 (resp, error)
type MethodHandler func(ctx context.Context, req interface{}) (interface{}, error)

// reqFactory 用于在主循环中为每个请求创建空的 req 实例
// 由 RegisterMethod 泛型闭包捕获类型参数，无需反射
type handlerEntry struct {
	newReq  func() interface{} // 工厂：创建 *Req 实例
	handler MethodHandler      // 业务处理函数
}

// RegisterMethod 泛型注册接口
// 依靠泛型实例化 Req 和 Resp，彻底消除 reflect.New 和 reflect.Call
func RegisterMethod[Req any, Resp any](
	server *Server,
	serviceMethod string,
	handler func(ctx context.Context, req *Req, resp *Resp) error,
) {
	entry := &handlerEntry{
		newReq: func() interface{} { return new(Req) },
		handler: func(ctx context.Context, req interface{}) (interface{}, error) {
			resp := new(Resp)
			err := handler(ctx, req.(*Req), resp)
			return resp, err
		},
	}
	server.serviceMap.Store(serviceMethod, entry)
}
