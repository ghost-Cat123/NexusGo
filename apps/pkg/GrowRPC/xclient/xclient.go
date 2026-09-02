package xclient

import (
	. "GrowRPC"
	"GrowRPC/pool"
	"context"
	"io"
	"reflect"
	"sync"
	"time"
)

// XClient 向用户暴露支持负载均衡的客户端
type XClient struct {
	// 服务发现实例
	d Discovery
	// 负载均衡选项
	mode SelectMode
	// 协议选项
	opt *Option
	mu  sync.Mutex
	// 复用创建好的Socket连接
	pools map[string]*pool.Pool
	// 连接池配置
	poolOpts []pool.Option
}

// 利用context上下文传递一致性哈希的key参数
// 1. 定义一个私有类型，防止 context 键名冲突
type routingKey string

// 2. 实例化一个固定的 Key
const contextKeyRoutingKey routingKey = "rpc_routing_key"

// WithRoutingKey 3. 提供一个对外暴露的辅助函数，方便用户往 ctx 里塞 Key
func WithRoutingKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, contextKeyRoutingKey, key)
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovery, mode SelectMode, opt *Option, poolOpts ...pool.Option) *XClient {
	return &XClient{d: d, mode: mode, opt: opt, pools: make(map[string]*pool.Pool), poolOpts: poolOpts}
}

// Close 关闭建立的连接
func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, p := range xc.pools {
		_ = p.Close()
		delete(xc.pools, key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*pool.Conn, error) {
	xc.mu.Lock()
	p, ok := xc.pools[rpcAddr]
	if !ok {
		factory := func() (*Client, error) {
			return XDial(rpcAddr, xc.opt)
		}
		p = pool.New(factory, xc.poolOpts...)
		xc.pools[rpcAddr] = p
	}
	xc.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.Get(ctx)
}

func (xc *XClient) putClient(rpcAddr string, conn *pool.Conn) {
	xc.mu.Lock()
	p, ok := xc.pools[rpcAddr]
	xc.mu.Unlock()
	if ok {
		p.Put(conn)
	} else {
		_ = conn.Client.Close()
	}
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	conn, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	defer xc.putClient(rpcAddr, conn)
	return conn.Client.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	var hashKey string
	// 用约定好的键查询rpc路由的key值
	if val := ctx.Value(contextKeyRoutingKey); val != nil {
		hashKey = val.(string)
	}
	// 如果是一致性哈希才会使用到hashKey
	rpcAddr, err := xc.d.Get(xc.mode, hashKey)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast 广播请求到所有服务 任意实例发生错误 则返回
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	// 有错误发生则失效
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			// 克隆 reply，避免多个 goroutine 同时修改同一个 reply
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			defer mu.Unlock()
			// 任意请求失败 则取消未完成的请求
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
