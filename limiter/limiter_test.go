package limiter

import (
	"math"
	"math/rand"
	"testing"
)

const eps = 1e-9

// poisson returns an Arrival function generating Poisson traffic at
// ratePerSec requests/second on every node, for a simulation tick of dt.
func poisson(ratePerSec, dt float64) func(int, float64, *rand.Rand) int {
	return func(_ int, _ float64, rng *rand.Rand) int {
		// Knuth Poisson, fine for the small means used here.
		l := math.Exp(-ratePerSec * dt)
		k, p := 0, 1.0
		for p > l {
			k++
			p *= rng.Float64()
		}
		return k - 1
	}
}

// --- Coordinator unit tests -------------------------------------------------

func TestCoordinatorGrantNeverExceedsPool(t *testing.T) {
	c := NewCoordinator(100, 10, 5)
	l := c.Sync(0, 0, 60, 0)
	if l.Remaining != 60 {
		t.Fatalf("grant = %v, want 60", l.Remaining)
	}
	if c.Pool != 40 {
		t.Fatalf("pool = %v, want 40", c.Pool)
	}
	// Second node asks for more than remains.
	l2 := c.Sync(1, 0, 80, 0)
	if l2.Remaining != 40 {
		t.Fatalf("grant = %v, want 40 (pool-limited)", l2.Remaining)
	}
	if c.Pool != 0 {
		t.Fatalf("pool = %v, want 0", c.Pool)
	}
	if got := c.Pool + c.Outstanding(); got > 100+eps {
		t.Fatalf("invariant violated: pool+outstanding = %v > capacity", got)
	}
}

func TestCoordinatorRefillCapsAtCapacityMinusOutstanding(t *testing.T) {
	c := NewCoordinator(100, 10, 100)
	c.Sync(0, 0, 70, 0) // pool=40, outstanding=70... ask 70 of 100
	// After 10s at 10 tok/s the raw refill would be +100, but 70 tokens are
	// leased out, so the pool may only rise to 100-70 = 30.
	c.Expire(10)
	if c.Pool > 30+eps {
		t.Fatalf("pool = %v, want <= 30 (capacity minus outstanding)", c.Pool)
	}
}

func TestCoordinatorSyncTopUpCarriesLeftover(t *testing.T) {
	c := NewCoordinator(100, 10, 100)
	c.Sync(0, 0, 50, 0) // pool=50, lease Remaining=50
	l := c.Sync(0, 20, 20, 1)
	// Consumed 20 of 50; leftover 30 is carried into the new lease and the
	// node is topped up by 20: pool = 50-20 = 30, new Remaining = 30+20.
	if l.Granted != 20 {
		t.Fatalf("granted = %v, want 20", l.Granted)
	}
	if l.Remaining != 50 {
		t.Fatalf("lease remaining = %v, want 50 (leftover 30 + grant 20)", l.Remaining)
	}
	if math.Abs(c.Pool-30) > eps {
		t.Fatalf("pool = %v, want 30", c.Pool)
	}
	if got := c.Pool + c.Outstanding(); got > 100+eps {
		t.Fatalf("invariant violated: pool+outstanding = %v > capacity", got)
	}
}

func TestCoordinatorLeaseExpiryReclaims(t *testing.T) {
	c := NewCoordinator(100, 0, 5) // no refill: isolate the reclaim
	l := c.Sync(0, 0, 40, 0)
	if c.Pool != 60 {
		t.Fatalf("pool = %v, want 60", c.Pool)
	}
	evs := c.Expire(l.ExpiresAt - 0.5)
	if len(evs) != 0 {
		t.Fatalf("lease reclaimed before TTL: %+v", evs)
	}
	evs = c.Expire(l.ExpiresAt)
	if len(evs) != 1 || evs[0].Amount != 40 {
		t.Fatalf("reclaim = %+v, want one event of 40", evs)
	}
	if c.Pool != 100 {
		t.Fatalf("pool = %v, want 100 after reclaim", c.Pool)
	}
}

func TestCoordinatorExpiredLeaseReportCountsOvershoot(t *testing.T) {
	c := NewCoordinator(100, 0, 5)
	c.Sync(0, 0, 40, 0)
	c.Expire(6) // reclaim all 40 (node never reported)
	// Node comes back and reports 15 consumed for the expired lease:
	// those tokens were reclaimed and possibly re-granted -> overshoot.
	c.Sync(0, 15, 10, 7)
	if c.Overshoot() != 15 {
		t.Fatalf("overshoot = %v, want 15", c.Overshoot())
	}
}

// --- Simulation tests -------------------------------------------------------

// TestGlobalBoundConcurrent: many nodes, sustained overload, random network
// delays, no failures. Total admissions must never exceed C + R*t.
func TestGlobalBoundConcurrent(t *testing.T) {
	cfg := Config{
		Duration: 30, DT: 0.002, SampleEvery: 0.05,
		NumNodes: 6, Capacity: 120, Rate: 100, TTL: 4,
		Batch: 15, SyncPeriod: 0.4,
		MinDelay: 0.01, MaxDelay: 0.15,
		ClockOffsets: []float64{-0.2, -0.1, 0, 0.1, 0.2, 0.3},
		Seed:         7,
	}
	cfg.Arrival = poisson(300, cfg.DT) // 300 req/s per node >> R
	res := Run(cfg)

	if res.MaxExcess > 1e-6 {
		t.Fatalf("bound violated: max excess = %v", res.MaxExcess)
	}
	for _, s := range res.Samples {
		if s.Pool < -eps {
			t.Fatalf("negative pool %.3f at t=%.2f", s.Pool, s.T)
		}
	}
	final := cfg.Bound(cfg.Duration, 0)
	if got := float64(res.TotalAdmitted()); got > final+1e-6 {
		t.Fatalf("admitted %v > bound %v", got, final)
	}
	// Sanity: under heavy load the limiter should actually be used.
	if res.TotalAdmitted() == 0 {
		t.Fatal("no admissions recorded")
	}
	t.Logf("admitted=%d bound=%.0f", res.TotalAdmitted(), final)
}

// TestFailureReclaim: a node crashes holding a leased batch; the tokens must
// return to the pool within the TTL, and the global bound with F=1 must hold.
func TestFailureReclaim(t *testing.T) {
	cfg := Config{
		Duration: 30, DT: 0.002, SampleEvery: 0.05,
		NumNodes: 3, Capacity: 120, Rate: 60, TTL: 3,
		Batch: 20, SyncPeriod: 0.5,
		MinDelay: 0.01, MaxDelay: 0.05,
		FailAt: map[int]float64{1: 10},
		Seed:   11,
	}
	cfg.Arrival = poisson(100, cfg.DT)
	res := Run(cfg)

	var reclaims, failures []Event
	for _, e := range res.Events {
		switch e.Kind {
		case EventReclaim:
			if e.NodeID == 1 {
				reclaims = append(reclaims, e)
			}
		case EventFailure:
			failures = append(failures, e)
		}
	}
	if len(failures) != 1 {
		t.Fatalf("failures = %d, want 1", len(failures))
	}
	if len(reclaims) == 0 {
		t.Fatal("no reclaim for the failed node")
	}
	for _, r := range reclaims {
		if r.T < 10 {
			t.Fatalf("reclaim before failure: t=%.2f", r.T)
		}
		// The node's last sync may have been delivered up to MaxDelay after
		// the crash; its lease must expire within TTL of that delivery
		// (plus one tick of scan granularity).
		deadline := 10 + cfg.MaxDelay + cfg.TTL + 2*cfg.DT
		if r.T > deadline {
			t.Fatalf("reclaim too late: t=%.2f, want <= %.2f", r.T, deadline)
		}
	}
	if res.Reclaimed <= 0 {
		t.Fatal("no tokens reclaimed")
	}
	// Bound with F=1 failed node must hold at every sample.
	if res.MaxExcess > 1e-6 {
		t.Fatalf("bound violated: max excess = %v", res.MaxExcess)
	}
	final := cfg.Bound(cfg.Duration, 1)
	if got := float64(res.TotalAdmitted()); got > final+1e-6 {
		t.Fatalf("admitted %v > bound %v (F=1)", got, final)
	}
	t.Logf("reclaimed=%.1f at t=%.2f.., admitted=%d bound(F=1)=%.0f",
		res.Reclaimed, reclaims[0].T, res.TotalAdmitted(), final)
}

// TestFailedNodeDoesNotStarveOthers: after the failed node's lease is
// reclaimed, the surviving nodes must regain roughly the full global rate.
func TestFailedNodeDoesNotStarveOthers(t *testing.T) {
	cfg := Config{
		Duration: 40, DT: 0.002, SampleEvery: 0.1,
		NumNodes: 3, Capacity: 100, Rate: 90, TTL: 3,
		Batch: 20, SyncPeriod: 0.5,
		MinDelay: 0.01, MaxDelay: 0.05,
		FailAt: map[int]float64{0: 5},
		Seed:   3,
	}
	cfg.Arrival = poisson(200, cfg.DT)
	res := Run(cfg)

	// Admissions in the last 10s (long after reclaim at ~t=8) should be
	// close to the full global rate: >= 80% of R*10.
	var before, after uint64
	for _, s := range res.Samples {
		if s.T >= 29.9 && s.T < 30 {
			before = s.Admitted
		}
		if s.T >= 39.9 {
			after = s.Admitted
			break
		}
	}
	got := float64(after - before)
	if want := 0.8 * cfg.Rate * 10; got < want {
		t.Fatalf("post-recovery rate too low: %.0f admissions in last 10s, want >= %.0f", got, want)
	}
}

// TestClockSkewFairness: nodes with skewed clocks and identical heavy demand
// must receive comparable shares — skew may shift sync phase but not window
// math, so per-node admissions differ by at most a couple of batches plus
// the tokens refilled during the maximum skew window.
func TestClockSkewFairness(t *testing.T) {
	const maxOff = 0.3
	cfg := Config{
		Duration: 30, DT: 0.002, SampleEvery: 0.1,
		NumNodes: 4, Capacity: 120, Rate: 100, TTL: 4,
		Batch: 15, SyncPeriod: 0.4,
		MinDelay: 0.01, MaxDelay: 0.05,
		ClockOffsets: []float64{-maxOff, -0.1, 0.1, maxOff},
		Seed:         5,
	}
	cfg.Arrival = poisson(200, cfg.DT)
	res := Run(cfg)

	var minA, maxA uint64 = math.MaxUint64, 0
	for _, n := range res.Nodes {
		if n.Admitted < minA {
			minA = n.Admitted
		}
		if n.Admitted > maxA {
			maxA = n.Admitted
		}
	}
	// Theoretical fairness slack: one batch per sync-phase difference plus
	// the refill that accrues during the worst-case skew+delay window.
	slack := 3*cfg.Batch + cfg.Rate*(2*maxOff+cfg.MaxDelay)
	if d := float64(maxA - minA); d > slack {
		t.Fatalf("unfair: max-min admitted = %v > slack %.0f (min=%d max=%d)", d, slack, minA, maxA)
	}
	if minA == 0 {
		t.Fatal("a node was starved entirely")
	}
	t.Logf("admitted per node: min=%d max=%d slack=%.0f", minA, maxA, slack)
}

// TestSingleNodeLowTraffic: a single node well under the global rate must
// admit (essentially) everything.
func TestSingleNodeLowTraffic(t *testing.T) {
	cfg := Config{
		Duration: 20, DT: 0.002, SampleEvery: 0.1,
		NumNodes: 1, Capacity: 100, Rate: 50, TTL: 4,
		Batch: 20, SyncPeriod: 0.5,
		MinDelay: 0.01, MaxDelay: 0.03,
		Seed: 1,
	}
	cfg.Arrival = poisson(15, cfg.DT) // 0.3 * R
	res := Run(cfg)

	n := res.Nodes[0]
	if n.Admitted == 0 {
		t.Fatal("nothing admitted")
	}
	// Rejections only possible in the brief window before the first grant.
	if n.Rejected > uint64(cfg.Batch) {
		t.Fatalf("rejected %d under light load, want <= %v", n.Rejected, cfg.Batch)
	}
	t.Logf("admitted=%d rejected=%d", n.Admitted, n.Rejected)
}

// TestSingleNodeOverload: one node far above the global rate must be capped
// at the single-bucket bound C + R*t.
func TestSingleNodeOverload(t *testing.T) {
	cfg := Config{
		Duration: 20, DT: 0.002, SampleEvery: 0.1,
		NumNodes: 1, Capacity: 100, Rate: 50, TTL: 4,
		Batch: 20, SyncPeriod: 0.5,
		MinDelay: 0.01, MaxDelay: 0.03,
		Seed: 2,
	}
	cfg.Arrival = poisson(500, cfg.DT)
	res := Run(cfg)

	final := cfg.Bound(cfg.Duration, 0)
	if got := float64(res.TotalAdmitted()); got > final+1e-6 {
		t.Fatalf("admitted %v > bound %v", got, final)
	}
	if res.MaxExcess > 1e-6 {
		t.Fatalf("max excess = %v", res.MaxExcess)
	}
}

// TestMultiNodeBurst: all nodes burst simultaneously; the global bound must
// hold and every node must get some share.
func TestMultiNodeBurst(t *testing.T) {
	cfg := Config{
		Duration: 20, DT: 0.001, SampleEvery: 0.05,
		NumNodes: 5, Capacity: 80, Rate: 60, TTL: 4,
		Batch: 12, SyncPeriod: 0.4,
		MinDelay: 0.01, MaxDelay: 0.06,
		Seed: 9,
	}
	// Idle until t=5, then a 5s simultaneous burst on all nodes.
	cfg.Arrival = func(id int, t float64, rng *rand.Rand) int {
		if t < 5 || t >= 10 {
			return 0
		}
		return poisson(400, cfg.DT)(id, t, rng)
	}
	res := Run(cfg)

	if res.MaxExcess > 1e-6 {
		t.Fatalf("bound violated during burst: max excess = %v", res.MaxExcess)
	}
	for _, s := range res.Samples {
		if s.Pool < -eps {
			t.Fatalf("negative pool %.3f at t=%.2f", s.Pool, s.T)
		}
	}
	for _, n := range res.Nodes {
		if n.Admitted == 0 {
			t.Fatalf("node %d got no tokens during the burst", n.ID)
		}
	}
	t.Logf("burst admitted per node: %v", res.Nodes)
}
