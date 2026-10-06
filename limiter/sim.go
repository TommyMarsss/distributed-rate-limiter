package limiter

import (
	"math/rand"
	"sort"
)

// message is an in-flight network message. Requests travel node ->
// coordinator, grants travel coordinator -> node, each leg delayed by a
// sampled network delay.
type message struct {
	deliverAt float64
	isGrant   bool
	nodeID    int
	consumed  float64 // request: tokens consumed since last sync
	want      float64 // request: tokens requested
	grant     float64 // grant: tokens granted
	leaseExp  float64 // grant: lease expiry (coordinator clock)
}

// Event kinds recorded during a simulation.
const (
	EventFailure = "failure"
	EventRecover = "recover"
	EventReclaim = "reclaim"
)

// Event is a notable occurrence in a simulation run.
type Event struct {
	T      float64 `json:"t"`
	Kind   string  `json:"kind"`
	NodeID int     `json:"node"`
	Amount float64 `json:"amount,omitempty"`
}

// Sample is one row of the recorded time series.
type Sample struct {
	T        float64   `json:"t"`
	Pool     float64   `json:"pool"`
	Local    []float64 `json:"local"`
	Admitted uint64    `json:"admitted"`
}

// NodeStats summarizes one node at the end of a run.
type NodeStats struct {
	ID          int     `json:"id"`
	ClockOffset float64 `json:"clockOffset"`
	Admitted    uint64  `json:"admitted"`
	Rejected    uint64  `json:"rejected"`
}

// Config describes a simulation scenario.
type Config struct {
	Duration    float64 // seconds (virtual)
	DT          float64 // tick size, seconds
	SampleEvery float64 // seconds between recorded samples

	NumNodes   int
	Capacity   float64 // global bucket capacity (tokens)
	Rate       float64 // global refill rate (tokens/s)
	TTL        float64 // lease TTL (s)
	Batch      float64 // tokens a node requests per sync
	SyncPeriod float64 // node periodic sync interval (s)

	MinDelay float64 // one-way network delay range (s)
	MaxDelay float64

	ClockOffsets []float64 // per-node clock offset (s); nil => zeros
	FailAt       map[int]float64
	RecoverAt    map[int]float64

	// Arrival returns how many requests arrive at node `id` in the tick
	// starting at virtual time t. Nil => no traffic.
	Arrival func(id int, t float64, rng *rand.Rand) int

	Seed int64
}

// Result holds the recorded run.
type Result struct {
	Config    Config      `json:"config"`
	Samples   []Sample    `json:"samples"`
	Events    []Event     `json:"events"`
	Nodes     []NodeStats `json:"nodes"`
	Overshoot float64     `json:"overshoot"` // double-spent tokens (expired-then-reported)
	MaxExcess float64     `json:"maxExcess"` // max(admitted - bound) over all samples, <= 0 means bound held
	Reclaimed float64     `json:"reclaimed"` // total tokens reclaimed via TTL expiry
}

// Bound returns the theoretical admission bound at time t given `failed`
// node failures so far: Capacity + Rate*t + failed*Batch.
func (c *Config) Bound(t float64, failed int) float64 {
	return c.Capacity + c.Rate*t + float64(failed)*c.Batch
}

// Run executes the simulation on a virtual clock.
func Run(cfg Config) *Result {
	if cfg.DT <= 0 {
		cfg.DT = 0.001
	}
	if cfg.SampleEvery <= 0 {
		cfg.SampleEvery = 0.1
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	coord := NewCoordinator(cfg.Capacity, cfg.Rate, cfg.TTL)

	nodes := make([]*Node, cfg.NumNodes)
	for i := range nodes {
		var off float64
		if i < len(cfg.ClockOffsets) {
			off = cfg.ClockOffsets[i]
		}
		// Stagger first syncs so nodes don't thundering-herd at t=period.
		first := cfg.SyncPeriod * float64(i+1) / float64(cfg.NumNodes+1)
		nodes[i] = NewNode(i, cfg.Batch, cfg.SyncPeriod, off, first)
	}

	delay := func() float64 {
		return cfg.MinDelay + rng.Float64()*(cfg.MaxDelay-cfg.MinDelay)
	}

	var queue, outbox []message
	// send buffers a message; buffered messages join the queue at the end
	// of the current tick (delivery may itself enqueue grant replies).
	send := func(m message) { outbox = append(outbox, m) }

	res := &Result{Config: cfg}
	failed := make(map[int]bool) // currently down
	firedF := make(map[int]bool) // failure event already fired
	firedR := make(map[int]bool) // recovery event already fired
	cumFailures := 0             // cumulative failures (for the F*Batch bound)
	var nextSample float64

	record := func(t float64) {
		s := Sample{T: t, Pool: coord.Pool, Local: make([]float64, len(nodes))}
		for i, n := range nodes {
			s.Local[i] = n.Local
			s.Admitted += n.Admitted
		}
		res.Samples = append(res.Samples, s)
		if ex := float64(s.Admitted) - cfg.Bound(t, cumFailures); ex > res.MaxExcess {
			res.MaxExcess = ex
		}
	}

	for t := 0.0; t <= cfg.Duration; t += cfg.DT {
		// 1. Deliver due network messages. Delivery may enqueue grant
		//    replies, so the surviving queue must not share backing
		//    storage with the one being ranged over.
		var rest []message
		for _, m := range queue {
			if m.deliverAt > t {
				rest = append(rest, m)
				continue
			}
			if m.isGrant {
				n := nodes[m.nodeID]
				n.inFlight = false
				n.Local += m.grant
				n.wantSync = false
			} else {
				lease := coord.Sync(m.nodeID, m.consumed, m.want, t)
				if lease.Granted > 0 {
					send(message{
						deliverAt: t + delay(),
						isGrant:   true,
						nodeID:    m.nodeID,
						grant:     lease.Granted,
						leaseExp:  lease.ExpiresAt,
					})
				} else {
					nodes[m.nodeID].inFlight = false
				}
			}
		}
		queue = rest

		// 2. Reclaim expired leases.
		for _, ev := range coord.Expire(t) {
			res.Events = append(res.Events, Event{T: ev.Time, Kind: EventReclaim, NodeID: ev.NodeID, Amount: ev.Amount})
			res.Reclaimed += ev.Amount
		}

		// 3. Node failures / recoveries scheduled for this tick (fire once).
		for id, ft := range cfg.FailAt {
			if !firedF[id] && t >= ft {
				firedF[id] = true
				failed[id] = true
				cumFailures++
				nodes[id].Fail()
				res.Events = append(res.Events, Event{T: t, Kind: EventFailure, NodeID: id})
			}
		}
		for id, rt := range cfg.RecoverAt {
			if !firedR[id] && failed[id] && t >= rt {
				firedR[id] = true
				delete(failed, id)
				nodes[id].Recover()
				res.Events = append(res.Events, Event{T: t, Kind: EventRecover, NodeID: id})
			}
		}

		// 4. Traffic arrivals and local admission.
		if cfg.Arrival != nil {
			for i, n := range nodes {
				if !n.Alive() {
					continue
				}
				for k := 0; k < cfg.Arrival(i, t, rng); k++ {
					n.Allow()
				}
			}
		}

		// 5. Nodes whose sync is due send a request: report consumption and
		//    ask for a top-up back to a full batch (the local leftover is
		//    kept, so the node is never tokenless during the RTT). Node
		//    clocks may be skewed; the coordinator uses its own clock on
		//    arrival.
		for _, n := range nodes {
			if !n.Due(t) {
				continue
			}
			local := t + n.ClockOffset
			for n.nextSync <= local {
				n.nextSync += n.SyncPeriod
			}
			want := n.Batch - n.Local
			if want < 0 {
				want = 0
			}
			send(message{
				deliverAt: t + delay(),
				nodeID:    n.ID,
				consumed:  n.unreported,
				want:      want,
			})
			n.unreported = 0
			n.inFlight = true
		}

		// 6. Record a sample when due.
		if t >= nextSample {
			record(t)
			nextSample += cfg.SampleEvery
		}

		// 7. Messages sent during this tick join the network queue.
		queue = append(queue, outbox...)
		outbox = outbox[:0]
	}

	for i, n := range nodes {
		res.Nodes = append(res.Nodes, NodeStats{
			ID:          i,
			ClockOffset: n.ClockOffset,
			Admitted:    n.Admitted,
			Rejected:    n.Rejected,
		})
	}
	res.Overshoot = coord.Overshoot()
	// Keep events in time order for the report.
	sort.SliceStable(res.Events, func(i, j int) bool { return res.Events[i].T < res.Events[j].T })
	return res
}

// TotalAdmitted sums admissions across nodes.
func (r *Result) TotalAdmitted() uint64 {
	var a uint64
	for _, n := range r.Nodes {
		a += n.Admitted
	}
	return a
}
