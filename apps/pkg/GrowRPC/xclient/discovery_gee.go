package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// GeeRegistryDiscovery 注册中心维护的服务发现结构体
type GeeRegistryDiscovery struct {
	// 原手工维护结构体
	*MultiServersDiscovery
	// 注册中心的地址
	registry string
	// 服务列表的过期时间
	timeout time.Duration
	// 最后从注册中心更新服务列表的时间
	lastUpdate time.Time
}

// 默认10s 超时需从注册中心更新新的列表
const defaultUpdateTimeout = time.Second * 10

func NewGeeRegistryDiscovery(registerAddr string, timeout time.Duration) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &GeeRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

// Update 更新服务器列表
func (d *GeeRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

// Refresh 超时重新获取
func (d *GeeRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	// 通过http头部获取服务地址
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	// 获取可用服务列表 都存在自定义字段中
	servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
	// 更新注册中心的服务器列表
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		// 过滤空格
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Get(mode SelectMode, key string) (string, error) {
	// 都需要先刷新保证服务列表没有过期
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode, key)
}

func (d *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
