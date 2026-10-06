// Command simreport runs a multi-node simulation of the distributed token
// bucket and writes a single self-contained static HTML report.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/TommyMarsss/distributed-rate-limiter/limiter"
)

func main() {
	out := flag.String("out", "report.html", "output HTML file")
	seed := flag.Int64("seed", 42, "random seed")
	flag.Parse()

	cfg := limiter.Config{
		Duration:    60,
		DT:          0.002,
		SampleEvery: 0.1,

		NumNodes:   4,
		Capacity:   150, // global bucket capacity (tokens)
		Rate:       100, // global refill rate (tokens/s)
		TTL:        4,   // lease TTL (s)
		Batch:      20,  // tokens per sync request
		SyncPeriod: 0.5, // node sync interval (s)

		MinDelay: 0.02, // one-way network delay 20–120ms
		MaxDelay: 0.12,

		// Node clocks skewed by up to ±250ms.
		ClockOffsets: []float64{-0.25, -0.08, 0.12, 0.25},

		// Node 2 crashes at t=20s (holding a leased batch) and rejoins at 35s.
		FailAt:    map[int]float64{2: 20},
		RecoverAt: map[int]float64{2: 35},

		// Baseline 40 req/s per node (total 160 > R=100: sustained overload),
		// plus a 200 req/s burst on nodes 1 and 3 during t in [40, 45].
		Arrival: func(id int, t float64, rng *rand.Rand) int {
			lambda := 40.0
			if t >= 40 && t < 45 && (id == 1 || id == 3) {
				lambda = 200
			}
			n := 0
			p := lambda * 0.002 // DT
			// cheap Poisson: p small, allow rare double arrivals
			if rng.Float64() < p {
				n++
			}
			if p > 0.4 && rng.Float64() < p-0.4 {
				n++
			}
			return n
		},

		Seed: *seed,
	}

	res := limiter.Run(cfg)

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	defer f.Close()
	if err := limiter.WriteHTML(f, res); err != nil {
		fmt.Fprintln(os.Stderr, "render:", err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s\n", *out)
	fmt.Printf("  admitted total : %d\n", res.TotalAdmitted())
	fmt.Printf("  bound C+R*t+F*B: %.0f\n", cfg.Bound(cfg.Duration, 1))
	fmt.Printf("  max excess     : %.4f (<= 0 means the bound held)\n", res.MaxExcess)
	fmt.Printf("  overshoot      : %.1f tokens (double-spent via expired lease)\n", res.Overshoot)
	fmt.Printf("  reclaimed      : %.1f tokens\n", res.Reclaimed)
	for _, n := range res.Nodes {
		fmt.Printf("  node %d: admitted=%d rejected=%d clockOffset=%+.2fs\n",
			n.ID, n.Admitted, n.Rejected, n.ClockOffset)
	}
}
