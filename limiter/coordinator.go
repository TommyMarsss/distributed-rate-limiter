// Package limiter implements a distributed token-bucket rate limiter.
//
// A central Coordinator owns the global token pool. Nodes periodically
// "lease" a small batch of tokens from the pool, consume them locally,
// and report consumption / request a fresh batch on the next sync.
// Leases carry a TTL so that tokens held by a failed node are reclaimed
// automatically. All refill and window arithmetic happens on the
// coordinator's clock, so node clock skew cannot shift window boundaries.
package limiter

// Lease is a batch of tokens granted to one node. Remaining is the
// coordinator's view of how many of the granted tokens the node has not
// yet reported as consumed.
type Lease struct {
	ID        int
	NodeID    int
	Granted   float64 // top-up amount granted by this sync
	Remaining float64
	ExpiresAt float64
	Expired   bool
}

// ReclaimEvent records tokens returning to the global pool when a lease
// expires (typically because its node failed).
type ReclaimEvent struct {
	Time   float64
	NodeID int
	Lease  int
	Amount float64
}

// Coordinator owns the global pool. It is the only component that reads
// a clock for refill/window decisions; methods take `now` explicitly so
// simulations and tests can use a virtual clock.
//
// Invariant maintained at all times:
//
//	Pool + sum(lease.Remaining) <= Capacity
//
// from which the global admission bound follows (see README):
//
//	admitted(t) <= Capacity + Rate*t + F*Batch
//
// where F is the number of nodes that failed while holding unreported
// consumed tokens (<= total nodes).
type Coordinator struct {
	Capacity float64 // max tokens in the system (pool + outstanding leases)
	Rate     float64 // global refill rate, tokens per second
	TTL      float64 // lease time-to-live, seconds

	Pool      float64
	lastRefil float64

	leases    map[int]*Lease
	nodeLease map[int]int // nodeID -> active lease ID
	nextID    int

	reported  float64 // total consumption reported by nodes
	overshoot float64 // consumption reported for already-expired leases

	Reclaims []ReclaimEvent
}

func NewCoordinator(capacity, rate, ttl float64) *Coordinator {
	return &Coordinator{
		Capacity:  capacity,
		Rate:      rate,
		TTL:       ttl,
		Pool:      capacity,
		leases:    make(map[int]*Lease),
		nodeLease: make(map[int]int),
		nextID:    1,
	}
}

// Outstanding returns the total tokens currently leased out (coordinator's
// view of unconsumed leased tokens).
func (c *Coordinator) Outstanding() float64 {
	var s float64
	for _, l := range c.leases {
		s += l.Remaining
	}
	return s
}

// Reported returns total consumption reported by nodes so far.
func (c *Coordinator) Reported() float64 { return c.reported }

// Overshoot returns consumption that was reported after its lease had
// already expired and been reclaimed — i.e. tokens that were effectively
// spent twice. Bounded by Batch per failed/desynced node.
func (c *Coordinator) Overshoot() float64 { return c.overshoot }

// refill advances the pool to time `now`. The refill is capped so that
// Pool + Outstanding never exceeds Capacity: leased-out tokens still
// count against the bucket capacity.
func (c *Coordinator) refill(now float64) {
	if now <= c.lastRefil {
		return
	}
	c.Pool += c.Rate * (now - c.lastRefil)
	if cap := c.Capacity - c.Outstanding(); c.Pool > cap {
		c.Pool = cap
	}
	c.lastRefil = now
}

// Sync processes one sync message from a node: the node reports how many
// tokens it consumed since its previous sync and asks for a top-up of up
// to `want` tokens (typically Batch minus its local leftover). The node's
// previous lease is reconciled: reported consumption is subtracted and
// the remaining leftover is carried over into the new lease, so the
// invariant Pool + Outstanding <= Capacity is preserved and the node
// keeps its leftover while the request was in flight. At most one lease
// per node is kept. Returns the new lease; the granted (top-up) amount is
// lease.Remaining minus the carried-over leftover — see Grant.
func (c *Coordinator) Sync(nodeID int, consumed, want, now float64) *Lease {
	c.refill(now)

	var leftover float64
	if lid, ok := c.nodeLease[nodeID]; ok {
		if l := c.leases[lid]; l != nil {
			// Live lease: subtract reported consumption; the leftover
			// stays with the node and rolls into the new lease.
			l.Remaining -= consumed
			if l.Remaining < 0 {
				l.Remaining = 0
			}
			leftover = l.Remaining
			delete(c.leases, lid)
		} else {
			// Lease already expired and was reclaimed: these consumed
			// tokens were returned to the pool once but really spent —
			// this is the (bounded) overshoot term.
			c.overshoot += consumed
		}
		delete(c.nodeLease, nodeID)
	}
	c.reported += consumed

	grant := want
	if grant > c.Pool {
		grant = c.Pool
	}
	if grant < 0 {
		grant = 0
	}
	c.Pool -= grant

	l := &Lease{ID: c.nextID, NodeID: nodeID, Granted: grant, Remaining: leftover + grant, ExpiresAt: now + c.TTL}
	c.nextID++
	if l.Remaining > 0 {
		c.leases[l.ID] = l
		c.nodeLease[nodeID] = l.ID
	}
	return l
}

// Expire reclaims all leases whose TTL has elapsed by `now`, returning
// their unconsumed tokens to the pool. Tokens a failed node consumed but
// never reported are reclaimed too — that is the F*Batch overshoot term,
// bounded by one batch per failed node.
func (c *Coordinator) Expire(now float64) []ReclaimEvent {
	c.refill(now)
	var evs []ReclaimEvent
	for id, l := range c.leases {
		if l.Expired || l.ExpiresAt > now {
			continue
		}
		l.Expired = true
		c.Pool += l.Remaining
		evs = append(evs, ReclaimEvent{Time: now, NodeID: l.NodeID, Lease: id, Amount: l.Remaining})
		delete(c.leases, id)
		// NB: nodeLease is kept so that a late sync from this node is
		// recognized as reporting against an expired lease (overshoot).
	}
	if len(evs) > 0 {
		c.Reclaims = append(c.Reclaims, evs...)
	}
	return evs
}
