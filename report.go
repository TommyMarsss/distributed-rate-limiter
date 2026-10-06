package ratelimit

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// ReportData 是嵌入 HTML 的全部数据。
type ReportData struct {
	Title   string     `json:"title"`
	Config  ReportCfg  `json:"config"`
	Nodes   int        `json:"nodes"`
	Samples []Sample   `json:"samples"`
	Events  []SimEvent `json:"events"`
	Summary ReportSum  `json:"summary"`
}

type ReportCfg struct {
	Capacity   float64 `json:"capacity"`
	RatePerSec float64 `json:"ratePerSec"`
	LeaseTTLS  float64 `json:"leaseTTLs"`
	Bound      float64 `json:"bound"` // 理论超额上界 N*B
}

type ReportSum struct {
	TotalAllowed int     `json:"totalAllowed"`
	TotalDenied  int     `json:"totalDenied"`
	Granted      float64 `json:"granted"`
	Returned     float64 `json:"returned"`
	Reclaimed    float64 `json:"reclaimed"`
}

// OvershootBound 理论超额上界：任意时刻全部节点未消耗的在途租约令牌 ≤ N*B，
// 因此累计放行 ≤ C + r*T + N*B（详见 README 的推导）。
func OvershootBound(numNodes int, batchSize float64) float64 {
	return float64(numNodes) * batchSize
}

// BuildReport 汇总模拟结果。
func BuildReport(title string, sim *Simulator, cfg Config, batchSize float64) ReportData {
	rd := ReportData{
		Title:   title,
		Nodes:   len(sim.nodes),
		Samples: sim.samples,
		Events:  sim.events,
		Config: ReportCfg{
			Capacity:   cfg.Capacity,
			RatePerSec: cfg.RatePerSec,
			LeaseTTLS:  cfg.LeaseTTL.Seconds(),
			Bound:      OvershootBound(len(sim.nodes), batchSize),
		},
	}
	rd.Summary.TotalAllowed = sim.TotalAllowed()
	for _, n := range sim.nodes {
		rd.Summary.TotalDenied += n.Denied
	}
	rd.Summary.Granted = sim.coord.TotalGranted
	rd.Summary.Returned = sim.coord.TotalReturned
	rd.Summary.Reclaimed = sim.coord.TotalReclaimed
	return rd
}

// WriteHTML 生成单一静态 HTML 文件：数据与绘图图逻辑全部内嵌，
// 不依赖任何外部服务、框架或图形库。
func WriteHTML(path string, rd ReportData) error {
	data, err := json.Marshal(rd)
	if err != nil {
		return err
	}
	html := strings.Replace(reportTemplate, "/*__DATA__*/", string(data), 1)
	return os.WriteFile(path, []byte(html), 0o644)
}

// MustWriteHTML 同 WriteHTML，失败时 panic（供 main 使用）。
func MustWriteHTML(path string, rd ReportData) {
	if err := WriteHTML(path, rd); err != nil {
		panic(fmt.Sprintf("write report: %v", err))
	}
}
