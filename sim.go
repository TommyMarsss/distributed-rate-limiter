package ratelimit

import (
	"container/heap"
	"math/rand"
	"time"
)

// event 是离散事件队列中的一个待执行动作。
type event struct {
	at  time.Duration
	seq int64
	fn  func()
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	return h[i].at < h[j].at || (h[i].at == h[j].at && h[i].seq < h[j].seq)
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// Sample 是某一采样时刻的系统状态快照。
type Sample struct {
	T            float64   `json:"t"`            // 秒
	Global       float64   `json:"global"`       // 全局池可用令牌
	Outstanding  float64   `json:"outstanding"`  // 被租约占用的令牌
	Local        []float64 `json:"local"`        // 各节点本地令牌
	AllowedTotal []int     `json:"allowedTotal"` // 各节点累计放行
	DeniedTotal  []int     `json:"deniedTotal"`  // 各节点累计拒绝
	Failed       []bool    `json:"failed"`       // 各节点故障状态
}

// SimEvent 是需要在报告里高亮的关键事件。
type SimEvent struct {
	T      float64 `json:"t"`
	Type   string  `json:"type"` // "failure" | "recovery" | "reclaim"
	NodeID int     `json:"node"`
	Detail string  `json:"detail"`
}

// Simulator 是离散事件模拟器：以确定性方式推进虚拟时间，
// 模拟网络延迟、节点故障、时钟偏移，并周期性采样系统状态。
type Simulator struct {
	now   time.Duration
	queue eventHeap
	seq   int64

	coord *Coordinator
	nodes []*Node

	linkLatency   time.Duration // 单向网络延迟
	leaseTTL      time.Duration
	reclaimPeriod time.Duration // 协调器扫描过期租约的周期
	samplePeriod  time.Duration

	samples []Sample
	events  []SimEvent

	rng *rand.Rand
}

// Config 汇总一次模拟所需的全部参数。
type Config struct {
	Capacity      float64       // 全局桶容量 C
	RatePerSec    float64       // 全局补充速率 r
	LeaseTTL      time.Duration // 租约有效期
	ReclaimPeriod time.Duration // 过期扫描周期
	LinkLatency   time.Duration // 单向网络延迟（模拟同步延迟）
	SamplePeriod  time.Duration // 状态采样周期
	Seed          int64
}

func NewSimulator(cfg Config) *Simulator {
	if cfg.SamplePeriod <= 0 {
		cfg.SamplePeriod = 50 * time.Millisecond
	}
	if cfg.ReclaimPeriod <= 0 {
		cfg.ReclaimPeriod = time.Second
	}
	s := &Simulator{
		linkLatency:   cfg.LinkLatency,
		leaseTTL:      cfg.LeaseTTL,
		reclaimPeriod: cfg.ReclaimPeriod,
		samplePeriod:  cfg.SamplePeriod,
		rng:           rand.New(rand.NewSource(cfg.Seed)),
	}
	s.coord = NewCoordinator(&simClock{sim: s}, cfg.Capacity, cfg.RatePerSec, cfg.LeaseTTL)
	return s
}

// AddNode 加入一个节点。clockOffset 模拟该节点本地时钟相对协调器的偏移。
func (s *Simulator) AddNode(batchSize float64, syncInterval, safetyMargin time.Duration, clockOffset time.Duration) *Node {
	n := newNode(s, len(s.nodes), &offsetClock{sim: s, offset: clockOffset}, batchSize, syncInterval, safetyMargin)
	s.nodes = append(s.nodes, n)
	return n
}

// schedule 在 delay 之后执行 fn（模拟网络延迟或定时任务）。
func (s *Simulator) schedule(delay time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.queue, event{at: s.now + delay, seq: s.seq, fn: fn})
}

// scheduleAt 在绝对模拟时刻 at 执行 fn。
func (s *Simulator) scheduleAt(at time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.queue, event{at: at, seq: s.seq, fn: fn})
}

// StartTraffic 为节点 n 启动泊松到达的请求流（平均速率 ratePerSec）。
// StartTrafficAt 可指定流量开始的模拟时刻（用于跳过启动预热期）。
// 节点故障期间请求直接丢弃（客户端超时），恢复后自动继续。
func (s *Simulator) StartTraffic(n *Node, ratePerSec float64) {
	s.StartTrafficAt(n, ratePerSec, 0)
}

func (s *Simulator) StartTrafficAt(n *Node, ratePerSec float64, at time.Duration) {
	var next func()
	next = func() {
		if !n.Failed {
			n.Allow()
		}
		// 指数分布间隔 → 泊松过程
		dt := time.Duration(s.rng.ExpFloat64() / ratePerSec * float64(time.Second))
		s.schedule(dt, next)
	}
	s.scheduleAt(at, next)
}

// StartBurst 在 at 时刻向节点 n 一次性打入 count 个请求（突发流量）。
func (s *Simulator) StartBurst(n *Node, at time.Duration, count int) {
	s.scheduleAt(at, func() {
		for i := 0; i < count; i++ {
			n.Allow()
		}
	})
}

// FailNode / RecoverNode 在指定时刻改变节点状态，并记录高亮事件。
func (s *Simulator) FailNode(n *Node, at time.Duration) {
	s.scheduleAt(at, func() {
		n.Fail()
		s.events = append(s.events, SimEvent{
			T: at.Seconds(), Type: "failure", NodeID: n.ID,
			Detail: "节点下线，持有未归还租约",
		})
	})
}

func (s *Simulator) RecoverNode(n *Node, at time.Duration) {
	s.scheduleAt(at, func() {
		n.Recover()
		s.events = append(s.events, SimEvent{
			T: at.Seconds(), Type: "recovery", NodeID: n.ID,
			Detail: "节点恢复，重新申请租约",
		})
	})
}

// Run 启动各节点的周期同步与协调器的过期扫描，推进模拟直到 duration。
func (s *Simulator) Run(duration time.Duration) {
	for _, n := range s.nodes {
		node := n
		// 周期同步循环
		var tick func()
		tick = func() {
			if !node.Failed {
				node.sync()
			}
			s.schedule(node.SyncInterval, tick)
		}
		s.schedule(0, tick)
	}
	// 协调器周期回收过期租约
	var reclaimTick func()
	reclaimTick = func() {
		for _, ev := range s.coord.ExpireLeases() {
			s.events = append(s.events, SimEvent{
				T: ev.At.Seconds(), Type: "reclaim", NodeID: ev.NodeID,
				Detail: "租约过期，回收令牌",
			})
		}
		s.schedule(s.reclaimPeriod, reclaimTick)
	}
	s.schedule(0, reclaimTick)
	// 周期采样
	var sampleTick func()
	sampleTick = func() {
		s.takeSample()
		s.schedule(s.samplePeriod, sampleTick)
	}
	s.schedule(0, sampleTick)

	end := s.now + duration
	for len(s.queue) > 0 {
		e := heap.Pop(&s.queue).(event)
		if e.at > end {
			break
		}
		s.now = e.at
		e.fn()
	}
	s.now = end
	s.takeSample()
}

func (s *Simulator) takeSample() {
	smp := Sample{
		T:           s.now.Seconds(),
		Global:      s.coord.Available(),
		Outstanding: s.coord.Outstanding(),
	}
	for _, n := range s.nodes {
		smp.Local = append(smp.Local, n.LocalTokens())
		smp.AllowedTotal = append(smp.AllowedTotal, n.Allowed)
		smp.DeniedTotal = append(smp.DeniedTotal, n.Denied)
		smp.Failed = append(smp.Failed, n.Failed)
	}
	s.samples = append(s.samples, smp)
}

// Now 返回当前模拟时间。
func (s *Simulator) Now() time.Duration { return s.now }

// Samples / Events 供报告与测试读取。
func (s *Simulator) Samples() []Sample   { return s.samples }
func (s *Simulator) Events() []SimEvent  { return s.events }
func (s *Simulator) Coord() *Coordinator { return s.coord }
func (s *Simulator) Nodes() []*Node      { return s.nodes }

// TotalAllowed 所有节点累计放行总数。
func (s *Simulator) TotalAllowed() int {
	sum := 0
	for _, n := range s.nodes {
		sum += n.Allowed
	}
	return sum
}
