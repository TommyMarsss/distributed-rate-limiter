package limiter

import (
	"encoding/json"
	"io"
	"strings"
)

// reportData is the JSON payload embedded in the HTML report.
type reportData struct {
	Capacity   float64     `json:"capacity"`
	Rate       float64     `json:"rate"`
	Batch      float64     `json:"batch"`
	TTL        float64     `json:"ttl"`
	SyncPeriod float64     `json:"syncPeriod"`
	Duration   float64     `json:"duration"`
	NumNodes   int         `json:"numNodes"`
	MinDelay   float64     `json:"minDelay"`
	MaxDelay   float64     `json:"maxDelay"`
	Samples    []Sample    `json:"samples"`
	Events     []Event     `json:"events"`
	Nodes      []NodeStats `json:"nodes"`
	Overshoot  float64     `json:"overshoot"`
	MaxExcess  float64     `json:"maxExcess"`
	Reclaimed  float64     `json:"reclaimed"`
	Admitted   uint64      `json:"admitted"`
	Rejected   uint64      `json:"rejected"`
}

// WriteHTML renders the run as a single self-contained static HTML file:
// all data and logic are inline, no frameworks, no external requests.
func WriteHTML(w io.Writer, res *Result) error {
	var rejected uint64
	for _, n := range res.Nodes {
		rejected += n.Rejected
	}
	d := reportData{
		Capacity:   res.Config.Capacity,
		Rate:       res.Config.Rate,
		Batch:      res.Config.Batch,
		TTL:        res.Config.TTL,
		SyncPeriod: res.Config.SyncPeriod,
		Duration:   res.Config.Duration,
		NumNodes:   res.Config.NumNodes,
		MinDelay:   res.Config.MinDelay,
		MaxDelay:   res.Config.MaxDelay,
		Samples:    res.Samples,
		Events:     res.Events,
		Nodes:      res.Nodes,
		Overshoot:  res.Overshoot,
		MaxExcess:  res.MaxExcess,
		Reclaimed:  res.Reclaimed,
		Admitted:   res.TotalAdmitted(),
		Rejected:   rejected,
	}
	js, err := json.Marshal(d)
	if err != nil {
		return err
	}
	// Data is numeric + fixed strings, but stay safe against "</script>".
	safe := strings.ReplaceAll(string(js), "<", "<")
	_, err = io.WriteString(w, strings.Replace(reportTemplate, "/*__DATA__*/null", safe, 1))
	return err
}

const reportTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>分布式令牌桶限流器 — 运行报告</title>
<style>
:root{
  --bg:#0f1420; --panel:#171e2e; --ink:#e8ecf4; --muted:#8b94a7;
  --grid:#2a3348; --axis:#3a4560; --failure:#ff5d5d; --reclaim:#ffb84d;
  --recover:#69db7c; --pool:#4fc3f7; --bound:#ff8a65; --admit:#81c784;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:14px/1.5 -apple-system,"Helvetica Neue","PingFang SC","Microsoft YaHei",sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:20px;margin:0 0 4px}
h2{font-size:15px;margin:28px 0 8px;color:var(--muted);font-weight:600}
.sub{color:var(--muted);font-size:13px;margin-bottom:16px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin:16px 0}
.card{background:var(--panel);border:1px solid var(--grid);border-radius:10px;padding:12px 14px}
.card .k{color:var(--muted);font-size:12px}
.card .v{font-size:20px;font-weight:700;margin-top:2px}
.card .v.ok{color:var(--admit)} .card .v.warn{color:var(--reclaim)}
.panel{background:var(--panel);border:1px solid var(--grid);border-radius:10px;padding:14px;margin-top:12px}
canvas{width:100%;display:block}
.legend{display:flex;flex-wrap:wrap;gap:12px;margin-top:8px;font-size:12px;color:var(--muted)}
.legend span{display:inline-flex;align-items:center;gap:6px}
.sw{width:14px;height:3px;border-radius:2px;display:inline-block}
table{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}
th,td{text-align:left;padding:6px 10px;border-bottom:1px solid var(--grid)}
th{color:var(--muted);font-weight:600}
.tag{display:inline-block;padding:1px 8px;border-radius:8px;font-size:12px}
.tag.failure{background:rgba(255,93,93,.15);color:var(--failure)}
.tag.reclaim{background:rgba(255,184,77,.15);color:var(--reclaim)}
.tag.recover{background:rgba(105,219,124,.15);color:var(--recover)}
.note{color:var(--muted);font-size:12px;margin-top:8px}
code{background:#0b0f1a;padding:1px 6px;border-radius:6px;font-size:12px}
</style>
</head>
<body>
<div class="wrap">
  <h1>分布式令牌桶限流器 — 多节点运行报告</h1>
  <div class="sub" id="subtitle"></div>

  <div class="cards" id="cards"></div>

  <h2>全局令牌池水位</h2>
  <div class="panel">
    <canvas id="chPool" height="260"></canvas>
    <div class="legend">
      <span><i class="sw" style="background:var(--pool)"></i>全局池可用令牌</span>
      <span><i class="sw" style="background:var(--bound)"></i>容量上限 C</span>
      <span><i class="sw" style="background:var(--failure)"></i>节点故障</span>
      <span><i class="sw" style="background:var(--reclaim)"></i>租约过期回收</span>
      <span><i class="sw" style="background:var(--recover)"></i>节点恢复</span>
    </div>
  </div>

  <h2>各节点本地配额</h2>
  <div class="panel">
    <canvas id="chLocal" height="260"></canvas>
    <div class="legend" id="lgLocal"></div>
  </div>

  <h2>累计放行 vs 理论上界</h2>
  <div class="panel">
    <canvas id="chAdmit" height="260"></canvas>
    <div class="legend">
      <span><i class="sw" style="background:var(--admit)"></i>累计放行请求数</span>
      <span><i class="sw" style="background:var(--bound)"></i>理论上界 C + R·t + F·B</span>
    </div>
    <div class="note">上界推导：任意时刻 池 + 在外租约 ≤ C，故累计放行 ≤ C + R·t + F·B，其中 F 为发生故障的节点数（每个故障节点最多带走一批 B 个未上报令牌）。</div>
  </div>

  <h2>事件时间线</h2>
  <div class="panel">
    <table id="tbl">
      <thead><tr><th>时间 (s)</th><th>事件</th><th>节点</th><th>令牌数</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <h2>节点统计</h2>
  <div class="panel">
    <table id="ntbl">
      <thead><tr><th>节点</th><th>时钟偏移 (s)</th><th>放行</th><th>拒绝</th><th>放行占比</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<script>
"use strict";
const DATA = /*__DATA__*/null;

const NODE_COLORS = ["#4fc3f7","#ba68c8","#81c784","#fff176","#f48fb1","#a1887f","#90a4ae","#e6ee9c"];
const EV_COLOR = {failure:"#ff5d5d", reclaim:"#ffb84d", recover:"#69db7c"};
const EV_NAME  = {failure:"节点故障", reclaim:"配额回收", recover:"节点恢复"};

function fmt(x, d){ return Number(x).toFixed(d === undefined ? 1 : d); }

// ---------- summary ----------
document.getElementById("subtitle").textContent =
  "容量 C=" + fmt(DATA.capacity,0) + " · 速率 R=" + fmt(DATA.rate,0) + " tok/s" +
  " · 批次 B=" + fmt(DATA.batch,0) + " · 租约 TTL=" + fmt(DATA.ttl,1) + "s" +
  " · 同步周期=" + fmt(DATA.syncPeriod,2) + "s · 网络延迟 " + fmt(DATA.minDelay*1000,0) + "–" + fmt(DATA.maxDelay*1000,0) + "ms" +
  " · 节点数=" + DATA.numNodes + " · 时长=" + fmt(DATA.duration,0) + "s";

const finalBound = DATA.capacity + DATA.rate*DATA.duration +
  DATA.events.filter(e => e.kind === "failure").length * DATA.batch;
const cards = [
  ["累计放行", DATA.admitted, DATA.admitted <= finalBound + 1e-9 ? "ok" : "warn"],
  ["理论上界 C+R·t+F·B", fmt(finalBound,0), ""],
  ["越界峰值 max(放行−上界)", fmt(DATA.maxExcess,3), DATA.maxExcess <= 1e-9 ? "ok" : "warn"],
  ["双花超额（过期后上报）", fmt(DATA.overshoot,1), DATA.overshoot <= DATA.batch + 1e-9 ? "ok" : "warn"],
  ["回收令牌总量", fmt(DATA.reclaimed,1), ""],
  ["累计拒绝", DATA.rejected, ""],
];
document.getElementById("cards").innerHTML = cards.map(c =>
  '<div class="card"><div class="k">' + c[0] + '</div><div class="v ' + c[2] + '">' + c[1] + '</div></div>'
).join("");

// ---------- chart helper ----------
function draw(canvasId, series, opts){
  const cv = document.getElementById(canvasId);
  const dpr = window.devicePixelRatio || 1;
  const W = cv.clientWidth || cv.parentElement.clientWidth - 28;
  const H = +cv.getAttribute("height");
  cv.width = W * dpr; cv.height = H * dpr;
  cv.style.height = H + "px";
  const ctx = cv.getContext("2d");
  ctx.scale(dpr, dpr);

  const padL = 56, padR = 14, padT = 12, padB = 28;
  const iw = W - padL - padR, ih = H - padT - padB;
  const tMax = DATA.duration;
  let yMax = 0;
  series.forEach(s => s.points.forEach(p => { if (p[1] > yMax) yMax = p[1]; }));
  (opts.hlines || []).forEach(h => { if (h.v > yMax) yMax = h.v; });
  yMax = yMax <= 0 ? 1 : yMax * 1.08;

  const X = t => padL + (t / tMax) * iw;
  const Y = v => padT + ih - (v / yMax) * ih;

  // grid + axes
  ctx.strokeStyle = "#2a3348"; ctx.fillStyle = "#8b94a7";
  ctx.font = "11px sans-serif"; ctx.lineWidth = 1;
  const yTicks = 5;
  for (let i = 0; i <= yTicks; i++){
    const v = yMax * i / yTicks, y = Y(v);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.textAlign = "right"; ctx.fillText(fmt(v, yMax > 50 ? 0 : 1), padL - 6, y + 3);
  }
  const xTicks = Math.min(12, Math.floor(tMax));
  ctx.textAlign = "center";
  for (let i = 0; i <= xTicks; i++){
    const t = tMax * i / xTicks, x = X(t);
    ctx.fillText(fmt(t,0) + "s", x, H - 8);
  }

  // event markers
  DATA.events.forEach(e => {
    const x = X(e.t);
    ctx.strokeStyle = EV_COLOR[e.kind] || "#fff";
    ctx.setLineDash([4,4]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, padT + ih); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = EV_COLOR[e.kind] || "#fff";
    ctx.beginPath(); ctx.arc(x, padT + 5, 3, 0, Math.PI*2); ctx.fill();
  });

  // horizontal reference lines
  (opts.hlines || []).forEach(h => {
    ctx.strokeStyle = h.color; ctx.setLineDash([6,4]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(padL, Y(h.v)); ctx.lineTo(W - padR, Y(h.v)); ctx.stroke();
    ctx.setLineDash([]);
  });

  // series
  series.forEach(s => {
    ctx.strokeStyle = s.color; ctx.lineWidth = s.width || 1.6;
    if (s.dash) ctx.setLineDash([6,4]);
    ctx.beginPath();
    s.points.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? ctx.lineTo(x,y) : ctx.moveTo(x,y); });
    ctx.stroke(); ctx.setLineDash([]);
  });
}

// ---------- chart 1: global pool ----------
draw("chPool",
  [{color:"#4fc3f7", points: DATA.samples.map(s => [s.t, s.pool])}],
  {hlines:[{v: DATA.capacity, color:"#ff8a65"}]});

// ---------- chart 2: per-node local quota ----------
const localSeries = [];
for (let n = 0; n < DATA.numNodes; n++){
  localSeries.push({
    color: NODE_COLORS[n % NODE_COLORS.length],
    points: DATA.samples.map(s => [s.t, s.local[n]]),
  });
}
draw("chLocal", localSeries, {hlines:[{v: DATA.batch, color:"#8b94a7"}]});
document.getElementById("lgLocal").innerHTML = localSeries.map((s, n) =>
  '<span><i class="sw" style="background:' + s.color + '"></i>节点 ' + n +
  '（偏移 ' + fmt(DATA.nodes[n].clockOffset, 2) + 's）</span>'
).join("") +
  '<span><i class="sw" style="background:#8b94a7"></i>批次大小 B</span>';

// ---------- chart 3: cumulative admitted vs bound ----------
let failures = 0, evIdx = 0;
const boundPts = DATA.samples.map(s => {
  while (evIdx < DATA.events.length && DATA.events[evIdx].t <= s.t){
    if (DATA.events[evIdx].kind === "failure") failures++;
    evIdx++;
  }
  return [s.t, DATA.capacity + DATA.rate * s.t + failures * DATA.batch];
});
draw("chAdmit",
  [{color:"#81c784", width:2, points: DATA.samples.map(s => [s.t, s.admitted])},
   {color:"#ff8a65", dash:true, points: boundPts}],
  {});

// ---------- tables ----------
document.querySelector("#tbl tbody").innerHTML = DATA.events.map(e =>
  "<tr><td>" + fmt(e.t, 2) + '</td><td><span class="tag ' + e.kind + '">' +
  (EV_NAME[e.kind] || e.kind) + "</span></td><td>节点 " + e.node + "</td><td>" +
  (e.amount !== undefined ? fmt(e.amount, 1) : "") + "</td></tr>"
).join("");

const totalAdm = DATA.nodes.reduce((a, n) => a + n.admitted, 0) || 1;
document.querySelector("#ntbl tbody").innerHTML = DATA.nodes.map(n =>
  "<tr><td>节点 " + n.id + "</td><td>" + fmt(n.clockOffset, 2) + "</td><td>" +
  n.admitted + "</td><td>" + n.rejected + "</td><td>" +
  fmt(100 * n.admitted / totalAdm, 1) + "%</td></tr>"
).join("");
</script>
</body>
</html>
`
