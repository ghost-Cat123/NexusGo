package GrowRPC

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"GrowRPC/codec"
)

const MagicNumber = 0x3bef5c

// DefaultOption 默认RPC选项
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

type Server struct {
	serviceMap   sync.Map
	interceptors []Interceptor
	mu           sync.Mutex
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (server *Server) Use(interceptors ...Interceptor) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.interceptors = append(server.interceptors, interceptors...)
}

func Use(interceptors ...Interceptor) {
	DefaultServer.Use(interceptors...)
}

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

type safeBufferConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *safeBufferConn) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (server *Server) ServeConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	// 1. 包装一个 bufio.Reader，提供缓冲但防止流交错丢失
	br := bufio.NewReader(conn)

	// 2. 精确读取一行 JSON Option（必定以 \n 结尾）
	jsonLine, err := br.ReadBytes('\n')
	if err != nil {
		log.Println("rpc server: options read error: ", err)
		return
	}

	// 3. 解析 JSON (Unmarshal 不受 \n 影响)
	var opt Option
	if err := json.Unmarshal(jsonLine, &opt); err != nil {
		log.Println("rpc server: options parse error: ", err)
		return
	}

	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	// 4. 将预读了底层数据的 br 一并传给后续流程
	bc := &safeBufferConn{
		Conn: conn,
		r:    br,
	}
	server.serveCodec(f(bc), &opt)
}

var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type Request struct {
	h       *codec.Header
	entry   *handlerEntry // 指向注册时的泛型闭包
	decoded interface{}   // 主循环同步 decode 完毕的 req 实例
}

// CallInfo 中间件相关
type CallInfo struct {
	Ctx           context.Context
	ServiceMethod string
	Header        *codec.Header
	ReqArgs       interface{}
}

// HandlerFunc 基本类型 只传请求
type HandlerFunc func(i *CallInfo) error

// Interceptor 中间件类型
type Interceptor func(next HandlerFunc) HandlerFunc

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*Request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &Request{h: h}
	entry, err := server.findEntry(h.ServiceMethod)
	if err != nil {
		// 找不到 handler，必须消费掉 Body 字节，否则下次 ReadHeader 会读到 Body 内容
		_ = cc.ReadBody(nil)
		return req, err
	}
	req.entry = entry

	// ─── 关键修复：在主循环（单协程）中同步读取 Body ───
	// 若推迟到 goroutine 里读，主循环会先开始读下一个请求的 Header，
	// 导致两个 goroutine 并发读同一条 JSON/Gob 流，高并发下必然流交错。
	// 解决方案：entry.newReq() 提供正确的目标类型实例，这里直接在主循环完成 decode。
	reqVal := entry.newReq()
	if err := cc.ReadBody(reqVal); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	req.decoded = reqVal
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *Request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	go func() {
		ctx := context.Background()
		var cancel context.CancelFunc
		if req.h.Metadata != nil {
			if deadlineStr, ok := req.h.Metadata["deadline"]; ok {
				if deadlineMs, err := strconv.ParseInt(deadlineStr, 10, 64); err == nil {
					ctx, cancel = context.WithDeadline(ctx, time.UnixMilli(deadlineMs))
				}
			}
		}
		if cancel == nil {
			ctx, cancel = context.WithCancel(ctx)
		}
		defer cancel()

		info := &CallInfo{
			Ctx:           ctx,
			ServiceMethod: req.h.ServiceMethod,
			Header:        req.h,
			ReqArgs:       req.decoded,
		}

		var respData interface{}
		var handler HandlerFunc = func(i *CallInfo) error {
			// req.decoded 已在主循环同步 decode，直接传给业务 handler
			resp, err := req.entry.handler(i.Ctx, req.decoded)
			respData = resp
			return err
		}

		for i := len(server.interceptors) - 1; i >= 0; i-- {
			handler = server.interceptors[i](handler)
		}

		err := handler(info)

		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, respData, sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (server *Server) findEntry(serviceMethod string) (*handlerEntry, error) {
	entryI, ok := server.serviceMap.Load(serviceMethod)
	if !ok {
		return nil, errors.New("rpc server: can't find service method " + serviceMethod)
	}
	return entryI.(*handlerEntry), nil
}

// 使服务端支持HTTP协议
const (
	connected        = "200 Connected to Gee RPC"
	defaultRPCPath   = "/_geerpc_"
	defaultDebugPath = "/debug/geerpc"
)

func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
