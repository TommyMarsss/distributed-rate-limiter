package ratelimit

import (
	"math"
	"testing"
	"time"
)

// manualClock 是测试用的手动推进时钟。
type manualClock struct{ t time.Duration }

func (m *manualClock) Now() time.Duration { return m.t }

// ---------- 协调器单元测试 ----------

func TestCoordinatorGrantLimitedByBucket(t *testing.T) {
	clk := &manualClock{}
	c := NewCoordinator(clk, 100, 10, 2*time.Second)

	l, ok := c.RequestLease(0, 30)
	if !ok || l.Tokens != 30 {
		t.Fatalf("期望授予 30，得到 %+v ok=%v", l, ok)
	}
	if got := c.Available(); got != 70 {
		t.Fatalf("期望剩余 70，得到 %v", got)
	}
	// 申请超过余额时按余额授予
	l2, ok := c.RequestLease(1, 500)
	if !ok || l2.Tokens != 70 {
		t.Fatalf("期望授予 70，得到 %+v ok=%v", l2, ok)
	}
	// 桶空后无法再授予
	if _, ok := c.RequestLease(2, 10); ok {
		t.Fatal("桶已空，不应再授予")
	}
}

func TestCoordinatorRefillCappedAtCapacity(t *testing.T) {
	clk := &manualClock{}
	c := NewCoordinator(clk, 100, 10, 2*time.Second)
	if _, ok := c.RequestLease(0, 100); !ok {
		t.Fatal("应授予 100")
	}
	clk.t = 5 * time.Second // 补充 50
	if got := c.Available(); got != 50 {
		t.Fatalf("期望补充到 50，得到 %v", got)
	}
	clk.t = 100 * time.Second // 远超容量
	if got := c.Available(); got != 100 {
		t.Fatalf("补充不应超过容量 100，得到 %v", got)
	}
}

func TestCoordinatorReturnAndReclaim(t *testing.T) {
	clk := &manualClock{}
	c := NewCoordinator(clk, 100, 0, 2*time.Second)

	l, _ := c.RequestLease(0, 40)
	c.ReturnUnused(l.ID, 15)
	if got := c.Available(); got != 75 {
		t.Fatalf("归还后期望 75，得到 %v", got)
	}
	// 重复归还应被忽略
	c.ReturnUnused(l.ID, 15)
	if got := c.Available(); got != 75 {
		t.Fatalf("重复归还不应生效，得到 %v", got)
	}

	// 第二个租约过期回收
	l2, _ := c.RequestLease(1, 25)
	clk.t = 3 * time.Second // 超过 TTL
	events := c.ExpireLeases()
	if len(events) != 1 || events[0].LeaseID != l2.ID || events[0].Tokens != 25 {
		t.Fatalf("回收事件不符: %+v", events)
	}
	if got := c.Available(); got != 75 {
		t.Fatalf("回收后期望 75，得到 %v", got)
	}
	// 过期回收后再归还应被忽略（防止双重计入）
	c.ReturnUnused(l2.ID, 25)
	if got := c.Available(); got != 75 {
		t.Fatalf("已回收租约的归还不应生效，得到 %v", got)
	}
}

// ---------- 集成测试：多节点模拟 ----------

func newTestSim(seed int64) (*Simulator, Config) {
	cfg := Config{
		Capacity:      100,
		RatePerSec:    50,
		LeaseTTL:      2 * time.Second,
		ReclaimPeriod: 500 * time.Millisecond,
		LinkLatency:   80 * time.Millisecond,
		SamplePeriod:  100 * time.Millisecond,
		Seed:          seed,
	}
	return NewSimulator(cfg), cfg
}

// 需求一：多节点并发消费下，全局放行总量不超过理论边界 C + r·T + N·B。
func TestOvershootWithinTheoreticalBound(t *testing.T) {
	sim, cfg := newTestSim(7)
	const N = 4
	const B = 10.0
	const T = 15 * time.Second
	for i := 0; i < N; i++ {
		n := sim.AddNode(B, 800*time.Millisecond, 300*time.Millisecond, 0)
		sim.StartTraffic(n, 40) // 总需求 160/s，远超 r=50/s
	}
	sim.Run(T)

	allowed := float64(sim.TotalAllowed())
	bound := cfg.Capacity + cfg.RatePerSec*T.Seconds() + OvershootBound(N, B)
	if allowed > bound+1e-9 {
		t.Fatalf("放行 %v 超过理论上界 %v", allowed, bound)
	}
	// 利用率 sanity check：不应严重欠载
	if allowed < 0.6*(cfg.Capacity+cfg.RatePerSec*T.Seconds()) {
		t.Fatalf("放行 %v 过低，限流器疑似欠载", allowed)
	}
	t.Logf("放行 %.0f，理论上界 %.0f", allowed, bound)
}

// 需求二：节点故障后，其租约令牌应在 TTL + 回收周期内回到全局池。
func TestLeaseReclaimAfterNodeFailure(t *testing.T) {
	sim, cfg := newTestSim(3)
	const B = 10.0
	n0 := sim.AddNode(B, 800*time.Millisecond, 300*time.Millisecond, 0)
	n1 := sim.AddNode(B, 800*time.Millisecond, 300*time.Millisecond, 0)
	sim.StartTraffic(n0, 20)
	sim.StartTraffic(n1, 20)

	failAt := 4 * time.Second
	sim.FailNode(n0, failAt)
	sim.Run(12 * time.Second)

	// 故障节点确实持有过租约并被回收
	var reclaimed float64
	var reclaimAt time.Duration = -1
	for _, ev := range sim.Coord().ReclaimLog() {
		if ev.NodeID == n0.ID {
			reclaimed += ev.Tokens
			if reclaimAt < 0 || ev.At < reclaimAt {
				reclaimAt = ev.At
			}
		}
	}
	if reclaimed == 0 {
		t.Fatal("故障节点的租约未被回收")
	}
	// 回收应发生在 故障时刻 + TTL + 一个回收周期 之内
	deadline := failAt + cfg.LeaseTTL + cfg.ReclaimPeriod
	if reclaimAt > deadline {
		t.Fatalf("回收时刻 %v 晚于预期上界 %v", reclaimAt, deadline)
	}
	// 故障节点不再占用任何配额
	for _, smp := range sim.Samples() {
		if smp.T > (deadline+time.Second).Seconds() && smp.Outstanding > 2*B*1.01+1 {
			// 只剩 n1 一个活跃节点，在途租约不应显著超过 2B
			//（新旧租约交接的往返窗口内会短暂出现两份）
			t.Fatalf("t=%.2f 在途租约 %.1f 异常（故障节点疑似仍占用配额）", smp.T, smp.Outstanding)
		}
	}
	t.Logf("回收 %.0f 令牌，首次回收于 %v（上界 %v）", reclaimed, reclaimAt, deadline)
}

// 需求三：节点时钟存在偏移时，各节点放行比例保持公平。
func TestFairnessUnderClockSkew(t *testing.T) {
	sim, _ := newTestSim(11)
	const B = 10.0
	offsets := []time.Duration{0, 400 * time.Millisecond, -400 * time.Millisecond, 200 * time.Millisecond}
	nodes := make([]*Node, len(offsets))
	for i, off := range offsets {
		nodes[i] = sim.AddNode(B, 800*time.Millisecond, 500*time.Millisecond, off)
		sim.StartTrafficAt(nodes[i], 40, time.Second) // 需求相同且充足
	}
	sim.Run(25 * time.Second)

	total := 0
	minA, maxA := math.MaxInt, 0
	for _, n := range nodes {
		total += n.Allowed
		if n.Allowed < minA {
			minA = n.Allowed
		}
		if n.Allowed > maxA {
			maxA = n.Allowed
		}
	}
	if minA == 0 {
		t.Fatal("存在被饿死的节点")
	}
	if ratio := float64(maxA) / float64(minA); ratio > 1.5 {
		t.Fatalf("时钟偏移下公平性失守：max/min = %.2f（各节点 %d/%d）", ratio, maxA, minA)
	}
	// 任何节点的份额不应偏离均值的 ±50%
	for i, n := range nodes {
		share := float64(n.Allowed) / float64(total)
		if share < 0.125 || share > 0.375 {
			t.Fatalf("节点 %d 份额 %.2f 超出公平区间", i, share)
		}
	}
	t.Logf("各节点放行：%d/%d/%d/%d，max/min=%.2f",
		nodes[0].Allowed, nodes[1].Allowed, nodes[2].Allowed, nodes[3].Allowed,
		float64(maxA)/float64(minA))
}

// 需求五-a：单节点小流量（远低于补充速率）应全部放行。
func TestSingleNodeLowTraffic(t *testing.T) {
	sim, _ := newTestSim(5)
	n := sim.AddNode(10, 800*time.Millisecond, 300*time.Millisecond, 0)
	sim.StartTrafficAt(n, 5, time.Second) // 预热 1s 后开始，5/s ≪ 50/s
	sim.Run(11 * time.Second)

	if n.Denied != 0 {
		t.Fatalf("小流量不应有拒绝，实际拒绝 %d（放行 %d）", n.Denied, n.Allowed)
	}
	if n.Allowed < 30 || n.Allowed > 70 { // 10s × 5/s ≈ 50
		t.Fatalf("放行数 %d 与流量不符", n.Allowed)
	}
}

// 需求五-b：多节点突发流量下，放行受全局配额约束且不超过理论边界。
func TestMultiNodeBurst(t *testing.T) {
	sim, cfg := newTestSim(9)
	const N = 3
	const B = 10.0
	const T = 8 * time.Second
	nodes := make([]*Node, N)
	for i := 0; i < N; i++ {
		nodes[i] = sim.AddNode(B, 800*time.Millisecond, 300*time.Millisecond, 0)
		sim.StartBurst(nodes[i], 2*time.Second, 200) // 每节点突发 200，远超容量
	}
	sim.Run(T)

	allowed := float64(sim.TotalAllowed())
	bound := cfg.Capacity + cfg.RatePerSec*T.Seconds() + OvershootBound(N, B)
	if allowed > bound+1e-9 {
		t.Fatalf("突发下放行 %v 超过理论上界 %v", allowed, bound)
	}
	denied := 0
	for _, n := range nodes {
		denied += n.Denied
	}
	if denied == 0 {
		t.Fatal("突发流量应产生拒绝")
	}
	t.Logf("突发：放行 %.0f / 600，拒绝 %d，上界 %.0f", allowed, denied, bound)
}

// 故障节点恢复后应重新正常参与限流。
func TestNodeRecovery(t *testing.T) {
	sim, _ := newTestSim(13)
	n := sim.AddNode(10, 800*time.Millisecond, 300*time.Millisecond, 0)
	sim.StartTraffic(n, 20)
	sim.FailNode(n, 3*time.Second)
	sim.RecoverNode(n, 6*time.Second)
	sim.Run(10 * time.Second)

	// 恢复后（6s 之后）应有新的放行
	allowedAtEnd := n.Allowed
	if allowedAtEnd == 0 {
		t.Fatal("节点恢复后未放行任何请求")
	}
	// 故障期间（3s~6s）不应放行：检查采样中故障窗口内 Allowed 不增长
	var atFail, atRecover int
	for _, smp := range sim.Samples() {
		if smp.T <= 3.0 {
			atFail = smp.AllowedTotal[0]
		}
		if smp.T <= 6.0 {
			atRecover = smp.AllowedTotal[0]
		}
	}
	if atRecover != atFail {
		t.Fatalf("故障窗口内不应放行：故障时 %d，恢复时 %d", atFail, atRecover)
	}
}
