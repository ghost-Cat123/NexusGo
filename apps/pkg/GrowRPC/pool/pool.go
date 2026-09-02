package pool

import (
	"GrowRPC"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrPoolClosed = errors.New("pool: connection pool closed")
)

// Conn 对外暴露的连接凭证（token），上层只持有此对象，不直接操作 poolConn
// 归还时将此 token 传回 Put，池内部通过它恢复 createAt
type Conn struct {
	Client *GrowRPC.Client
	pc     *poolConn // 指回内部包装，对外不可见
}

// poolConn 池内部的连接包装，生命周期完全由 Pool 控制
type poolConn struct {
	client   *GrowRPC.Client
	createAt time.Time // 仅在工厂创建时赋值，归还时不覆盖
	lastUsed time.Time // 借出时更新
}

type Pool struct {
	factory     func() (*GrowRPC.Client, error)
	maxIdle     int
	maxActive   int
	idleTimeout time.Duration
	maxLifetime time.Duration

	// 核心：所有状态统一在 mu 下操作，彻底消除 TOCTOU 竞争
	mu     sync.Mutex
	closed bool
	idle   []*poolConn // LIFO 栈，优先复用热连接
	active int         // 当前借出数（不含 idle 中的）

	// 仅用于阻塞通知：有连接归还时向此 chan 写一个信号
	// 容量为 maxActive，避免发送阻塞
	waitCh chan struct{}

	cleanerCh chan struct{}
}

type Option func(*Pool)

func WithMaxIdle(n int) Option               { return func(p *Pool) { p.maxIdle = n } }
func WithMaxActive(n int) Option             { return func(p *Pool) { p.maxActive = n } }
func WithIdleTimeout(d time.Duration) Option { return func(p *Pool) { p.idleTimeout = d } }
func WithMaxLifetime(d time.Duration) Option { return func(p *Pool) { p.maxLifetime = d } }

func New(factory func() (*GrowRPC.Client, error), opts ...Option) *Pool {
	p := &Pool{
		factory:     factory,
		maxIdle:     5,
		maxActive:   0,
		idleTimeout: 60 * time.Second,
		cleanerCh:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	waitCap := p.maxActive
	if waitCap <= 0 {
		waitCap = 64
	}
	p.waitCh = make(chan struct{}, waitCap)
	go p.cleaner()
	return p
}

// Get 借出一个连接，返回 *Conn（含 token）
// 调用者归还时必须将 *Conn 传回 Put，不可丢弃
func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	for {
		// --- 在锁内完成所有决策，消除竞争窗口 ---
		p.mu.Lock()

		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}

		// 1. 快速路径：从 idle 栈顶取（LIFO，热连接优先）
		for len(p.idle) > 0 {
			pc := p.idle[len(p.idle)-1]
			p.idle = p.idle[:len(p.idle)-1]

			// 检查生命周期和健康（在锁内判断时间，Close 前不会被竞态修改）
			if p.isExpired(pc) || !pc.client.IsAvailable() {
				p.mu.Unlock()
				_ = pc.client.Close()
				p.mu.Lock()
				continue
			}
			// 找到有效 idle 连接
			pc.lastUsed = time.Now()
			p.active++
			p.mu.Unlock()
			return &Conn{Client: pc.client, pc: pc}, nil
		}

		// 2. idle 为空，判断是否可以新建
		canCreate := p.maxActive == 0 || p.active < p.maxActive
		if canCreate {
			p.active++ // 预占
			p.mu.Unlock()
			client, err := p.factory()
			if err != nil {
				p.mu.Lock()
				p.active--
				p.mu.Unlock()
				return nil, err
			}
			pc := &poolConn{
				client:   client,
				createAt: time.Now(),
				lastUsed: time.Now(),
			}
			return &Conn{Client: client, pc: pc}, nil
		}

		// 3. 达到上限，释放锁后阻塞等待归还通知
		p.mu.Unlock()

		select {
		case <-p.waitCh:
			// 有连接被归还，重新竞争（回到 for 循环顶部）
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Put 归还连接
// conn 必须是 Get 返回的原始 *Conn，不可自行构造
func (p *Pool) Put(conn *Conn) {
	if conn == nil || conn.pc == nil {
		return
	}
	pc := conn.pc

	p.mu.Lock()
	defer p.mu.Unlock()

	// active 在确认借出时 +1，归还时 -1，精确对应
	p.active--

	// 连接池已关闭或连接不健康，直接丢弃
	if p.closed || !pc.client.IsAvailable() {
		// 注意：Close 后的归还不写 idle，由 Close 的 drain 负责清理已有 idle
		_ = pc.client.Close()
		// 通知可能正在等待的 Get（虽然池关了，让它们早点感知）
		p.notify()
		return
	}

	// idle 未满：放回栈顶
	if len(p.idle) < p.maxIdle {
		// 复用原 poolConn，只更新 lastUsed，保留 createAt
		pc.lastUsed = time.Now()
		p.idle = append(p.idle, pc)
		p.notify() // 通知等待者有连接可用
		return
	}

	// idle 已满：直接关闭，不做置换（置换逻辑复杂且收益低）
	_ = pc.client.Close()
	p.notify()
}

// notify 向 waitCh 发送一个非阻塞通知
// 调用方必须持有 mu
func (p *Pool) notify() {
	select {
	case p.waitCh <- struct{}{}:
	default:
	}
}

// isExpired 检查连接是否超过 idleTimeout 或 maxLifetime
// 调用方应持有 mu 或确保 pc 不被并发访问
func (p *Pool) isExpired(pc *poolConn) bool {
	if p.idleTimeout > 0 && time.Since(pc.lastUsed) > p.idleTimeout {
		return true
	}
	if p.maxLifetime > 0 && time.Since(pc.createAt) > p.maxLifetime {
		return true
	}
	return false
}

// cleaner 后台协程，定期清理 idle 中过期连接
func (p *Pool) cleaner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.cleanIdle()
		case <-p.cleanerCh:
			return
		}
	}
}

// cleanIdle 在 mu 下完整扫描 idle slice，无瞬时快照问题
// 清理后 idle 中保证没有过期或不健康的连接
func (p *Pool) cleanIdle() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	// 原地过滤：保留有效连接，收集待关闭连接
	var toClose []*poolConn
	kept := p.idle[:0]
	for _, pc := range p.idle {
		if p.isExpired(pc) || !pc.client.IsAvailable() {
			toClose = append(toClose, pc)
		} else {
			kept = append(kept, pc)
		}
	}
	p.idle = kept
	p.mu.Unlock()

	// 在锁外关闭，避免持锁时阻塞
	for _, pc := range toClose {
		_ = pc.client.Close()
	}
}

// Close 关闭连接池
// Close 后：新 Get 返回 ErrPoolClosed；正在借出的连接在 Put 时会被关闭
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	// 在锁内取出全部 idle，防止 Close 后 Put 再写入被遗漏
	toClose := p.idle
	p.idle = nil
	p.mu.Unlock()

	// 停止清理协程
	close(p.cleanerCh)

	// 锁外关闭 idle 连接
	for _, pc := range toClose {
		_ = pc.client.Close()
	}

	// 唤醒所有阻塞在 waitCh 的 Get，让它们感知 closed 并返回 ErrPoolClosed
	close(p.waitCh)
	return nil
}

// Stats 返回当前状态（近似值，仅供监控参考）
func (p *Pool) Stats() (idleCount, activeCount int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle), p.active
}
