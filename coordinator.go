package ratelimit

import (
	"fmt"
	"math"
	"time"
)

// Lease 表示协调器授予某个节点的一批令牌。
// 令牌在授予时即从全局桶中扣除；若节点在 ExpiresAt 之前未归还，
// 协调器会将其过期回收，重新计入全局可用池。
type Lease struct {
	ID        int64
	NodeID    int
	Tokens    float64
	GrantedAt time.Duration // 协调器时钟
	ExpiresAt time.Duration // 协调器时钟
	Returned  bool          // 节点已主动归还（剩余量可能为 0）
	Reclaimed bool          // 已被协调器过期回收
}

// Coordinator 持有全局令牌桶，负责向各节点发放租约、接收归还、过期回收。
// 它是限流决策的唯一权威：只有桶内有令牌才会发放租约。
type Coordinator struct {
	capacity float64 // 桶容量 C
	rate     float64 // 匀速补充速率 r（令牌/秒）
	tokens   float64 // 当前可用令牌
	last     time.Duration

	clock Clock
	ttl   time.Duration // 租约有效期

	leases     map[int64]*Lease
	nextLease  int64
	reclaimLog []ReclaimEvent

	// 统计
	TotalGranted   float64 // 累计发放（含被回收的部分）
	TotalReturned  float64 // 节点主动归还
	TotalReclaimed float64 // 过期回收
}

// ReclaimEvent 记录一次过期回收，用于报告与测试。
type ReclaimEvent struct {
	At       time.Duration
	LeaseID  int64
	NodeID   int
	Tokens   float64
	LeaseAge time.Duration
}

func NewCoordinator(clock Clock, capacity, ratePerSec float64, leaseTTL time.Duration) *Coordinator {
	return &Coordinator{
		capacity: capacity,
		rate:     ratePerSec,
		tokens:   capacity, // 启动时桶是满的
		clock:    clock,
		ttl:      leaseTTL,
		leases:   make(map[int64]*Lease),
	}
}

// refill 按协调器自己的时钟做匀速补充（token bucket 的标准实现）。
func (c *Coordinator) refill() {
	now := c.clock.Now()
	if now <= c.last {
		return
	}
	elapsed := now - c.last
	c.tokens += c.rate * elapsed.Seconds()
	if c.tokens > c.capacity {
		c.tokens = c.capacity
	}
	c.last = now
}

// RequestLease 节点申请一批令牌。want 为期望数量，返回实际授予的租约。
// 令牌按整数发放（一个请求消耗一个令牌）；桶内不足时按现有余额授予，
// 不足 1 个时返回 ok=false。
func (c *Coordinator) RequestLease(nodeID int, want float64) (Lease, bool) {
	c.refill()
	grant := math.Floor(want)
	if grant > c.tokens {
		grant = math.Floor(c.tokens)
	}
	if grant < 1 {
		return Lease{}, false
	}
	c.tokens -= grant
	c.nextLease++
	l := &Lease{
		ID:        c.nextLease,
		NodeID:    nodeID,
		Tokens:    grant,
		GrantedAt: c.clock.Now(),
		ExpiresAt: c.clock.Now() + c.ttl,
	}
	c.leases[l.ID] = l
	c.TotalGranted += grant
	return *l, true
}

// ReturnUnused 节点在换发新租约时归还旧租约的剩余令牌。
// 已过期被回收的租约的归还会被忽略（防止双重计入）。
func (c *Coordinator) ReturnUnused(leaseID int64, unused float64) {
	l, ok := c.leases[leaseID]
	if !ok || l.Returned || l.Reclaimed {
		return
	}
	if unused < 0 {
		unused = 0
	}
	if unused > l.Tokens {
		unused = l.Tokens
	}
	l.Returned = true
	c.refill()
	c.tokens += unused
	if c.tokens > c.capacity {
		c.tokens = c.capacity
	}
	c.TotalReturned += unused
}

// ExpireLeases 回收所有已过期且未归还的租约，令牌重新进入全局池。
// 模拟器会周期性调用；返回本次回收的事件列表。
func (c *Coordinator) ExpireLeases() []ReclaimEvent {
	now := c.clock.Now()
	var events []ReclaimEvent
	for _, l := range c.leases {
		if l.Returned || l.Reclaimed || now < l.ExpiresAt {
			continue
		}
		l.Reclaimed = true
		c.refill()
		c.tokens += l.Tokens
		if c.tokens > c.capacity {
			c.tokens = c.capacity
		}
		c.TotalReclaimed += l.Tokens
		ev := ReclaimEvent{
			At:       now,
			LeaseID:  l.ID,
			NodeID:   l.NodeID,
			Tokens:   l.Tokens,
			LeaseAge: now - l.GrantedAt,
		}
		c.reclaimLog = append(c.reclaimLog, ev)
		events = append(events, ev)
	}
	return events
}

// Available 当前全局池可用令牌（会先触发补充）。
func (c *Coordinator) Available() float64 {
	c.refill()
	return c.tokens
}

// Outstanding 当前所有未归还、未回收租约的令牌总量（被节点占用中）。
func (c *Coordinator) Outstanding() float64 {
	sum := 0.0
	for _, l := range c.leases {
		if !l.Returned && !l.Reclaimed {
			sum += l.Tokens
		}
	}
	return sum
}

// ReclaimLog 返回全部回收事件。
func (c *Coordinator) ReclaimLog() []ReclaimEvent { return c.reclaimLog }

func (c *Coordinator) String() string {
	return fmt.Sprintf("Coordinator{tokens=%.1f/%.1f rate=%.1f/s outstanding=%.1f}",
		c.tokens, c.capacity, c.rate, c.Outstanding())
}
