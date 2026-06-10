package mcp

import (
	"context"
	"github.com/mark3labs/mcp-go/server"
)

var sse *server.SSEServer // 持有引用供 Shutdown 使用

func Start(ctx context.Context, addr string) error {
	s := server.NewMCPServer(
		"IM-Agent-MCP",
		"1.0.0",
		server.WithToolCapabilities(true),
	)
	// 注册为MCP Tools
	RegisterSearchChatHistory(s)
	RegisterScheduleMessage(s)
	sse = server.NewSSEServer(s, server.WithBaseURL("http://localhost"+addr))

	// 监听 ctx 取消，异步触发 shutdown
	go func() {
		<-ctx.Done()
		err := sse.Shutdown(context.Background())
		if err != nil {
			return
		}
	}()

	return sse.Start(addr)
}
