package callback

import (
	"NexusGo/apps/pkg/config"
	"NexusGo/apps/pkg/logger"
	"context"
	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	"github.com/cloudwego/eino/callbacks"
	"github.com/coze-dev/cozeloop-go"
	"sync"
)

var (
	coozCli cozeloop.Client
	// 保证只注册一次
	once    sync.Once
	initErr error
	handler callbacks.Handler
)

func InitCozeLoop(agentConfig config.AgentConfig) error {
	once.Do(func() {
		coozCli, initErr = cozeloop.NewClient(
			cozeloop.WithAPIToken(agentConfig.Trace.APIToken),
			cozeloop.WithWorkspaceID(agentConfig.Trace.WorkSpaceId),
		)
		if initErr != nil {
			return
		} else {
			logger.Log.Infof("CoozLoop单例初始化成功！")
		}
		// 客户端就绪后才注册全局回调
		handler = clc.NewLoopHandler(coozCli)
		callbacks.AppendGlobalHandlers(handler)
	})
	return initErr
}

func CloseTrace() {
	coozCli.Close(context.Background())
}
