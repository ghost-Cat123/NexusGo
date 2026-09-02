package xclient

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type EtcdDiscovery struct {
	// etcd配置
	client      *clientv3.Client
	serviceName string
	mu          sync.RWMutex
	servers     []string
	// 负载均衡 配置
	index   atomic.Int32
	r       *rand.Rand
	rMu     sync.Mutex
	hashMap *Map
	// 中断信号量
	stopCh chan struct{}
}

func NewEtcdDiscovery(client *clientv3.Client, serviceName string) *EtcdDiscovery {
	d := &EtcdDiscovery{
		client:      client,
		serviceName: serviceName,
		r:           rand.New(rand.NewSource(time.Now().UnixNano())),
		hashMap:     New(50, nil),
		stopCh:      make(chan struct{}),
	}
	d.index.Store(int32(d.r.Intn(math.MaxInt32 - 1)))
	go d.watch()
	return d
}

func (d *EtcdDiscovery) watch() {
	prefix := fmt.Sprintf("/services/%s/", d.serviceName)
	for {
		if d.isStopped() {
			return
		}

		resp, err := d.client.Get(context.Background(), prefix, clientv3.WithPrefix())
		if err != nil {
			log.Printf("etcd discovery: get error: %v, retrying in 3s...", err)
			if d.sleepOrStop(3 * time.Second) {
				return
			}
			continue
		}
		d.updateServersFromKVs(resp.Kvs)

		watchCh := d.client.Watch(context.Background(), prefix,
			clientv3.WithPrefix(),
			clientv3.WithRev(resp.Header.Revision+1))

		for wresp := range watchCh {
			if wresp.Err() != nil {
				log.Printf("etcd discovery: watch error: %v", wresp.Err())
				break
			}
			for _, ev := range wresp.Events {
				d.handleEvent(ev)
			}
		}

		if d.sleepOrStop(3 * time.Second) {
			return
		}
	}
}

func (d *EtcdDiscovery) isStopped() bool {
	select {
	case <-d.stopCh:
		return true
	default:
		return false
	}
}

func (d *EtcdDiscovery) sleepOrStop(dur time.Duration) bool {
	select {
	case <-d.stopCh:
		return true
	case <-time.After(dur):
		return false
	}
}

func (d *EtcdDiscovery) updateServersFromKVs(kvs []*mvccpb.KeyValue) {
	d.mu.Lock()
	defer d.mu.Unlock()

	addrs := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		addr := d.parseAddr(string(kv.Key))
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	d.servers = addrs
	d.hashMap = New(50, nil)
	d.hashMap.Add(addrs...)
}

func (d *EtcdDiscovery) handleEvent(ev *clientv3.Event) {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch ev.Type {
	case mvccpb.PUT:
		addr := d.parseAddr(string(ev.Kv.Key))
		if addr != "" && !d.contains(addr) {
			d.servers = append(d.servers, addr)
		}
	case mvccpb.DELETE:
		addr := d.parseAddr(string(ev.Kv.Key))
		d.remove(addr)
	}
	d.hashMap = New(50, nil)
	d.hashMap.Add(d.servers...)
}

func (d *EtcdDiscovery) contains(addr string) bool {
	for _, a := range d.servers {
		if a == addr {
			return true
		}
	}
	return false
}

func (d *EtcdDiscovery) remove(addr string) {
	for i, a := range d.servers {
		if a == addr {
			d.servers[i] = d.servers[len(d.servers)-1]
			d.servers = d.servers[:len(d.servers)-1]
			return
		}
	}
}

func (d *EtcdDiscovery) parseAddr(key string) string {
	prefix := fmt.Sprintf("/services/%s/", d.serviceName)
	return strings.TrimPrefix(key, prefix)
}

var _ Discovery = (*EtcdDiscovery)(nil)

func (d *EtcdDiscovery) Refresh() error {
	return nil
}

func (d *EtcdDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.hashMap = New(50, nil)
	d.hashMap.Add(servers...)
	return nil
}

func (d *EtcdDiscovery) Get(mode SelectMode, key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}

	switch mode {
	case RandomSelect:
		d.rMu.Lock()
		idx := d.r.Intn(n)
		d.rMu.Unlock()
		return d.servers[idx], nil
	case RoundRobinSelect:
		idx := int(d.index.Add(1)-1) % n
		return d.servers[idx], nil
	case ConsistentHash:
		if key == "" {
			return "", errors.New("rpc discovery: key is required for ConsistentHash")
		}
		return d.hashMap.Get(key), nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

func (d *EtcdDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

func (d *EtcdDiscovery) Close() error {
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
	return nil
}
