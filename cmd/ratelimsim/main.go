// ratelimsim 运行一次多节点分布式限流模拟，并生成单文件 HTML 报告。
package main

import (
	"flag"
	"fmt"
	"time"

	ratelimit "github.com/TommyMarsss/distributed-rate-limiter"
)

func main() {
	out := flag.String("out", "report.html", "输出 HTML 报告路径")
	flag.Parse()

	cfg := ratelimit.Config{
		Capacity:      100,             // 全局桶容量 C
		RatePerSec:    50,              // 全局补充速率 r = 50 令牌/s
		LeaseTTL:      2 * time.Second, // 租约 2s 过期
		ReclaimPeriod: 500 * time.Millisecond,
		LinkLatency:   80 * time.Millisecond, // 模拟同步延迟（单向）
		SamplePeriod:  50 * time.Millisecond,
		Seed:          42,
	}
	sim := ratelimit.NewSimulator(cfg)

	const batch = 10.0 // 每个节点每次领取 B=10 个令牌
	// 4 个节点，各带不同的时钟偏移（-300ms ~ +250ms）
	offsets := []time.Duration{0, 250 * time.Millisecond, -300 * time.Millisecond, 120 * time.Millisecond}
	nodes := make([]*ratelimit.Node, len(offsets))
	for i, off := range offsets {
		nodes[i] = sim.AddNode(batch, 800*time.Millisecond, 300*time.Millisecond, off)
		sim.StartTraffic(nodes[i], 25) // 每节点 25 req/s，总需求 100 req/s > r
	}

	// 突发流量：t=6s 时节点 1 一次性涌入 60 个请求
	sim.StartBurst(nodes[1], 6*time.Second, 60)
	// 节点故障与恢复：t=4s 节点 2 下线（持有租约），t=9s 恢复
	sim.FailNode(nodes[2], 4*time.Second)
	sim.RecoverNode(nodes[2], 9*time.Second)

	sim.Run(14 * time.Second)

	rd := ratelimit.BuildReport("分布式令牌桶限流器 · 多节点模拟报告", sim, cfg, batch)
	ratelimit.MustWriteHTML(*out, rd)

	fmt.Printf("模拟完成：放行 %d，拒绝 %d，回收 %.1f 令牌\n报告已写入 %s\n",
		rd.Summary.TotalAllowed, rd.Summary.TotalDenied, rd.Summary.Reclaimed, *out)
}
