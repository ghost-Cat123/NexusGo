package mcp

import (
	"context"
	emcp "github.com/cloudwego/eino-ext/components/tool/mcp"
	"github.com/cloudwego/eino/components/tool"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

// GetExternalTools MCP客户端
func GetExternalTools(ctx context.Context, serverURL string) ([]tool.BaseTool, error) {
	if serverURL == "" {
		return nil, nil
	}
	cli, err := client.NewSSEMCPClient(serverURL)
	if err != nil {
		return nil, err
	}
	if err = cli.Start(ctx); err != nil {
		return nil, err
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{
		Name:    "im-agent",
		Version: "1.0.0",
	}
	if _, err := cli.Initialize(ctx, initReq); err != nil {
		return nil, err
	}
	return emcp.GetTools(ctx, &emcp.Config{Cli: cli})
}
