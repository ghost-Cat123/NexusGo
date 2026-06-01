package ws

import (
	"NexusGo/apps/pkg/cache"
	"context"
	"strconv"
	"sync"
)

// CliMap 维护全局的websocket连接
type CliMap struct {
	sync.RWMutex
	Clients map[string]*Client
}

// NewCliMap 构造函数
func NewCliMap() *CliMap {
	return &CliMap{Clients: make(map[string]*Client)}
}

func (m *CliMap) Register(uid int64, newClient *Client) {
	m.Lock()
	defer m.Unlock()

	// 1. 检查是否存在旧连接 多设备会下线，如果后期要多设备在线可以加设备号
	IdStr := strconv.FormatInt(uid, 10)
	if oldClient, exit := m.Clients[IdStr]; exit {
		// 发送下线通知给旧设备
		oldClient.SendMessage([]byte(`{"action": "kick", "reason": "logged_in_elsewhere"}`))
		// 关闭旧连接
		oldClient.Close()
	}
	// 覆盖为新连接
	m.Clients[IdStr] = newClient
}

func (m *CliMap) Get(key string) (*Client, bool) {
	m.RLock()
	defer m.RUnlock()
	client, exit := m.Clients[key]
	return client, exit
}

func (m *CliMap) Remove(key string, newClient *Client) {
	m.Lock()
	defer m.Unlock()
	// 仅当实例相同时才delete
	if m.Clients[key] == newClient {
		delete(m.Clients, key)
		// 同时清理路由表
		ctx := context.Background()
		redisKey := "route:user:" + key
		cache.GetCache().Del(ctx, redisKey)
	}
}

func (m *CliMap) Len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.Clients)
}
