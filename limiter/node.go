package limiter

// Node is one rate-limiting agent. It consumes tokens from a local batch
// leased from the coordinator and syncs periodically (or immediately when
// its batch runs out under demand). The node's own clock — which may be
// skewed by ClockOffset — is used ONLY to schedule its syncs; all token
// refill and window math happens on the coordinator's clock, so skew can
// shift a node's sync phase by at most |offset| but cannot move window
// boundaries or refill amounts.
type Node struct {
	ID          int
	Batch       float64 // tokens requested per sync
	SyncPeriod  float64 // seconds between periodic syncs (node-local clock)
	ClockOffset float64 // node clock = sim clock + ClockOffset

	Local      float64 // tokens currently held locally
	Admitted   uint64  // requests admitted locally
	Rejected   uint64  // requests rejected locally
	alive      bool
	inFlight   bool    // a sync request is currently in the network
	unreported float64 // tokens consumed since the last sync was sent
	nextSync   float64 // next periodic sync, node-local clock
	wantSync   bool    // ran dry under demand: sync ASAP
}

func NewNode(id int, batch, syncPeriod, clockOffset, firstSync float64) *Node {
	return &Node{
		ID:          id,
		Batch:       batch,
		SyncPeriod:  syncPeriod,
		ClockOffset: clockOffset,
		alive:       true,
		nextSync:    firstSync,
	}
}

func (n *Node) Alive() bool { return n.alive }

// Allow reports whether one request is admitted at this node.
func (n *Node) Allow() bool {
	if !n.alive {
		return false
	}
	if n.Local >= 1 {
		n.Local--
		n.unreported++
		n.Admitted++
		return true
	}
	n.Rejected++
	n.wantSync = true
	return false
}

// Due reports whether the node wants to send a sync at sim time `now`:
// either its periodic sync came due on its (possibly skewed) local clock,
// or it ran out of tokens under demand and has no request in flight.
func (n *Node) Due(now float64) bool {
	if !n.alive || n.inFlight {
		return false
	}
	if now+n.ClockOffset >= n.nextSync {
		return true
	}
	return n.wantSync && n.Local < 1
}

// Fail / Recover simulate node crash and rejoin. A crash loses all local
// state: tokens held but not yet reported as consumed are reclaimed by the
// coordinator via the lease TTL — that (bounded by one batch) is the only
// source of overshoot in the protocol.
func (n *Node) Fail() { n.alive = false }

func (n *Node) Recover() {
	n.alive = true
	n.Local = 0
	n.unreported = 0
	n.inFlight = false
	n.wantSync = false
}
