package GrowRPC

import (
	"GrowRPC/codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call 一次RPC调用所需信息
type Call struct {
	// 唯一标识当前RPC调用
	Seq uint64
	// 具体方法 目标服务+方法名
	ServiceMethod string
	// 调用方法的参数 客户端
	Args interface{}
	// 方法的返回结果 服务器端
	Reply interface{}
	// 调用过程中的错误信息
	Error error
	// 异步通知通道 调用完成触发客户端回调
	Done chan *Call
	// 透传的上下文数据
	Metadata map[string]string
}

// 异步调用 通知调用方
func (call *Call) done() {
	call.Done <- call
}

// Client 客户端核心
type Client struct {
	// 消息的编解码器 序列化+反序列化
	cc  codec.Codec
	opt *Option
	// 发送请求的互斥锁
	sending sync.Mutex
	// 请求消息头 只有发送时需要 可复用
	header codec.Header
	// Client自己的互斥锁
	mu sync.Mutex
	// 发送请求的编号
	seq uint64
	// 存储未处理的请求 seq: Call实例
	pending map[uint64]*Call
	// 用户关闭 任意一个为true表示不可用
	closing bool
	// 错误发生 时的关闭
	shutdown bool
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

var ErrShutdown = errors.New("connection is shut down")

var _ io.Closer = (*Client)(nil)

// Close 实现Closer接口 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 判断连接是否可用
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 将参数添加到pending中 并更新seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	// 请求序号
	call.Seq = client.seq
	// 加入map 代表未处理
	client.pending[call.Seq] = call
	// 接收下一个请求 不阻塞
	client.seq++
	return call.Seq, nil
}

// 根据seq 从pending中移除对应的call并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务器或客户端发生错误时调用 更改shutdown并通知其他未处理的call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 客户端接收
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		// 服务端处理正常 将请求状态设置为处理完
		call := client.removeCall(h.Seq)
		switch {
		// 1. call不存在
		case call == nil:
			err = client.cc.ReadBody(nil)
		// 2. call存在但服务端出错
		case h.Error != "":
			call.Error = errors.New(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		// 3. call存在 从body中读取返回值
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 有错误则中止
	client.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	// 有无该编码方式
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 发送option信息给服务端 完成协议交换
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		cc:  cc,
		opt: opt,
		// 序号从1开始 0表示不可用的call
		seq:     1,
		pending: make(map[uint64]*Call),
	}
	// 开多线程接收服务端响应
	go client.receive()
	return client
}

// 解析option参数 opts为可选参数
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 客户端连接的超时处理
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	// 解析参数
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 传入服务端地址 返回连接 包一层超时处理
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// 如果client为空则 关闭连接
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	// 创建一个通道
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		// 将client和err传入通道 客户端连接
		ch <- clientResult{client: client, err: err}
	}()
	// 没有超时重传
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	// 监听通道操作
	select {
	// 如果执行f超过超时时间 触发该分支
	case <-time.After(opt.ConnectTimeout):
		// 超时
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	// 在超时时间内完成
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial 便于用户传入服务端地址 创建Client实例
func Dial(netWork, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, netWork, address, opts...)
}

// 发送请求
func (client *Client) send(call *Call) {
	// 确保 Client 发送正确请求
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册请求 拿到序号
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""
	client.header.Metadata = call.Metadata

	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// 客户端已经处理 但依然为空
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步接口 返回call实例 (为了兼容上层调用保留原签名)
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	return client.goWithContext(context.Background(), serviceMethod, args, reply, done)
}

// 内部包含 context 透传的异步调用
func (client *Client) goWithContext(ctx context.Context, serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		// 创建一个channel 设置10的缓冲区
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		// 有done 但是缓冲区为0
		log.Panic("rpc client: done channel is unbuffered")
	}
	if client == nil {
		call := &Call{Error: errors.New("client instance is nil")}
		if done != nil {
			done <- call
		}
		return call
	}

	// 提取 Context 中的超时和元数据
	metadata := make(map[string]string)
	if deadline, ok := ctx.Deadline(); ok {
		metadata["deadline"] = fmt.Sprintf("%d", deadline.UnixMilli())
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
		Metadata:      metadata,
	}
	// 客户端发送请求
	client.send(call)
	// 返回请求实例
	return call
}

// Call 封装Go 同步接口 阻塞 等待响应返回 超时处理 ctx让调用者可以进行外部干预
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	if client == nil {
		return errors.New("client instance is nil")
	}
	// 异步RPC调用 阻塞Done
	call := client.goWithContext(ctx, serviceMethod, args, reply, make(chan *Call, 1))
	select {
	// RPC调用被外部中断
	case <-ctx.Done():
		// 清理未完成调用
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	// RPC调用正常完成或或异常失败
	case call := <-call.Done:
		return call.Error
	}
}

// NewHTTPClient 客户端实现HTTP协议
// 发起CONNECT请求 检查返回状态码 成功建立连接
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, err := io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\r\n\r\n", defaultRPCPath))
	if err != nil {
		return nil, err
	}

	// 给服务器发送请求
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})

	// 创建NewClient 用于后续通讯过程
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}

	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}

	return nil, err
}

// DialHTTP 监听默认HTTP RPC
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial 根据rpc地址相应不同函数
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// 其他传输协议
		return Dial(protocol, addr, opts...)
	}
}
