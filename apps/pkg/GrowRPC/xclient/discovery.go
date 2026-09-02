package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// SelectMode 不同的负载均衡策略
type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
	ConsistentHash
)

// Discovery 服务发现基本接口
type Discovery interface {
	Refresh() error
	Update(servers []string) error
	// Get 传一个key值 当使用一致性哈希算法时使用
	Get(mode SelectMode, key string) (string, error)
	GetAll() ([]string, error)
}

// MultiServersDiscovery 服务列表由手工维护的服务发现结构体
type MultiServersDiscovery struct {
	// 产生随机数
	r       *rand.Rand
	mu      sync.RWMutex
	servers []string
	// 记录轮询到的位置
	index int
	// 一致性哈希使用的哈希环
	hashMap *Map
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		servers: servers,
	}
	// 避免每次都从0开始 随机初始化一个正值
	d.index = d.r.Intn(math.MaxInt32 - 1)
	// 构造hash环 默认一个节点会有50个虚拟节点 使用默认hash算法
	d.hashMap = New(50, nil)
	// 将服务节点插入哈希环
	d.hashMap.Add(servers...)
	return d
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// Refresh 手动控制 无需刷新
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	// 更新服务列表 重建哈希环
	d.hashMap = New(50, nil)
	d.hashMap.Add(servers...)
	return nil
}

func (d *MultiServersDiscovery) Get(mode SelectMode, key string) (string, error) {
	// 排他写锁
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	// 随机选择
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	// 轮询
	case RoundRobinSelect:
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	case ConsistentHash:
		if key == "" {
			return "", errors.New("rpc discovery: key is required for ConsistentHash")
		}
		// 选择相应服务节点
		return d.hashMap.Get(key), nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	// 共享读锁，多个goroutine不会互相阻塞
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	// 防止暴露底层数组
	copy(servers, d.servers)
	return servers, nil
}
