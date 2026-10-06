package ratelimit

import "time"

// Node 是限流系统中的一个节点：持有少量本地令牌，消耗完后向协调器申请下一批。
//
// 配额协调协议（租约制）：
//   - 节点每隔 SyncInterval 向协调器申请一批 BatchSize 个令牌（一个租约）；
//   - 新租约到达前节点继续消耗旧租约，不存在令牌真空期；新租约到达时
//     旧租约的剩余令牌立即（单向消息）归还协调器；
//   - 本地令牌降至低水位 LowWatermark 时提前预取下一批，避免同步往返
//     期间令牌耗尽造成毛刺拒绝；
//   - 节点故障时租约不再归还，由协调器在 TTL 过期后回收（见 Coordinator）。
//
// 时钟偏移处理：节点不信任任何绝对时间戳。租约有效性完全用「本地时钟的相对
// 时长」判定——收到租约时记录 leaseValidUntil = 本地当前时间 + TTL - SafetyMargin。
// 只要各节点时钟速率一致（仅存在固定偏移），相对时长的判定结果与偏移无关；
// SafetyMargin 吸收网络延迟，保证节点不会在协调器回收租约后继续使用它。
type Node struct {
	ID    int
	clock Clock

	local           float64 // 本地剩余令牌
	leaseID         int64   // 当前持有的租约 ID（0 表示无）
	hasLease        bool
	leaseValidUntil time.Duration // 本地时钟口径的租约失效时间

	requestInFlight bool

	BatchSize    float64       // 每次申请的令牌数 B
	LowWatermark float64       // 低水位：本地令牌降至此值即预取
	SyncInterval time.Duration // 定期同步周期
	SafetyMargin time.Duration // 提前停止使用租约的安全余量

	Failed bool // 节点故障：停止消费与同步，租约等待协调器回收

	Allowed int // 放行请求数
	Denied  int // 拒绝请求数

	sim *Simulator
}

func newNode(sim *Simulator, id int, clock Clock, batchSize float64, syncInterval, safetyMargin time.Duration) *Node {
	low := batchSize * 0.5
	if low < 1 {
		low = 1
	}
	return &Node{
		ID:           id,
		clock:        clock,
		BatchSize:    batchSize,
		LowWatermark: low,
		SyncInterval: syncInterval,
		SafetyMargin: safetyMargin,
		sim:          sim,
	}
}

// Allow 处理一次到达的请求：本地有令牌且租约在本地口径下仍有效则放行。
func (n *Node) Allow() bool {
	if n.Failed {
		return false
	}
	if n.local >= 1 && n.hasLease && n.clock.Now() < n.leaseValidUntil {
		n.local--
		n.Allowed++
		// 低水位预取：在令牌耗尽前就开始下一轮同步。
		if n.local <= n.LowWatermark && !n.requestInFlight {
			n.sync()
		}
		return true
	}
	n.Denied++
	// 本地令牌耗尽，立即触发一次补货（若尚无在途请求）。
	if !n.requestInFlight {
		n.sync()
	}
	return false
}

// sync 向协调器申请新租约。网络往返各有一次延迟；
// 新租约到达时，旧租约剩余令牌立即归还协调器。
func (n *Node) sync() {
	if n.Failed || n.requestInFlight {
		return
	}
	n.requestInFlight = true

	n.sim.schedule(n.sim.linkLatency, func() {
		lease, ok := n.sim.coord.RequestLease(n.ID, n.BatchSize)
		n.sim.schedule(n.sim.linkLatency, func() {
			n.requestInFlight = false
			if n.Failed || !ok {
				return
			}
			// 旧租约剩余量立即归还（即使为 0 也要回报以关闭租约：
			// 否则协调器只能等 TTL 过期后按租约全额回收，把已被消费的
			// 令牌重复计入全局池）。
			if n.hasLease {
				oldID, oldRemaining := n.leaseID, n.local
				n.sim.schedule(n.sim.linkLatency, func() {
					n.sim.coord.ReturnUnused(oldID, oldRemaining)
				})
			}
			n.local = lease.Tokens
			n.leaseID = lease.ID
			n.hasLease = true
			// 关键：用本地时钟 + 相对时长判定租约有效期，规避时钟偏移。
			n.leaseValidUntil = n.clock.Now() + n.sim.leaseTTL - n.SafetyMargin
		})
	})
}

// Fail 模拟节点故障：不再消费、不再同步、不再归还。
func (n *Node) Fail() { n.Failed = true }

// Recover 节点恢复：重新进入同步循环（原租约可能已被回收，重新申请即可）。
func (n *Node) Recover() {
	n.Failed = false
	n.hasLease = false
	n.local = 0
	n.leaseID = 0
	n.sync()
}

// LocalTokens 当前本地剩余令牌（用于采样与测试）。
func (n *Node) LocalTokens() float64 { return n.local }
