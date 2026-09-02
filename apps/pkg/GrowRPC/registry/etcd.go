package registry

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync/atomic"
	"time"
)

type EtcdRegistry struct {
	client   *clientv3.Client
	leaseID  clientv3.LeaseID
	leaseTTL int64
	key      string
	value    string
	closed   atomic.Bool
}

func NewEtcdRegistry(endpoints []string, ttl int64) (*EtcdRegistry, error) {
	if ttl <= 0 {
		ttl = 10
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdRegistry{
		client:   client,
		leaseTTL: ttl,
	}, nil
}

func (r *EtcdRegistry) Register(serviceName, addr string, meta map[string]string) error {
	resp, err := r.client.Grant(context.Background(), r.leaseTTL)
	if err != nil {
		return fmt.Errorf("etcd grant lease: %w", err)
	}
	r.leaseID = resp.ID

	r.key = fmt.Sprintf("/services/%s/%s", serviceName, addr)

	valMap := map[string]interface{}{"addr": addr}
	if meta != nil {
		for k, v := range meta {
			valMap[k] = v
		}
	}
	valByte, _ := json.Marshal(valMap)
	r.value = string(valByte)

	_, err = r.client.Put(context.Background(), r.key, r.value, clientv3.WithLease(r.leaseID))
	if err != nil {
		return fmt.Errorf("etcd put key: %w", err)
	}
	return nil
}

func (r *EtcdRegistry) KeepAlive(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ch, err := r.client.KeepAlive(ctx, r.leaseID)
		if err != nil {
			log.Printf("etcd keepalive: %v, reconnecting...", err)
			if err := r.reRegister(ctx); err != nil {
				log.Printf("etcd re-register failed: %v", err)
				if sleep(ctx, 3*time.Second) != nil {
					return ctx.Err()
				}
			}
			continue
		}

		for resp := range ch {
			if resp == nil {
				break
			}
		}

		log.Println("etcd keepalive channel closed, re-registering...")
		if err := r.reRegister(ctx); err != nil {
			log.Printf("etcd re-register failed: %v", err)
			if sleep(ctx, 3*time.Second) != nil {
				return ctx.Err()
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (r *EtcdRegistry) reRegister(ctx context.Context) error {
	if r.leaseID != 0 {
		_, revokeErr := r.client.Revoke(ctx, r.leaseID)
		if revokeErr != nil {
			log.Printf("etcd revoke old lease %x: %v (may already be expired)", r.leaseID, revokeErr)
		}
	}

	resp, err := r.client.Grant(ctx, r.leaseTTL)
	if err != nil {
		return err
	}
	r.leaseID = resp.ID

	_, err = r.client.Put(ctx, r.key, r.value, clientv3.WithLease(r.leaseID))
	return err
}

func (r *EtcdRegistry) Deregister() error {
	if r.leaseID != 0 {
		_, err := r.client.Revoke(context.Background(), r.leaseID)
		return err
	}
	return nil
}

func (r *EtcdRegistry) Close() error {
	r.closed.Store(true)
	return r.client.Close()
}
