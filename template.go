package ratelimit

// reportTemplate 是单文件报告的 HTML 模板。数据以 JSON 形式替换 /*__DATA__*/ 注入。
// 只使用原生 HTML/CSS/JavaScript 与 Canvas 2D，无任何第三方依赖。
const reportTemplate = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>分布式令牌桶限流器模拟报告</title>
<style>
  :root {
    --bg: #0f1420; --panel: #171e2e; --border: #2a3550;
    --text: #dbe2f0; --muted: #8a96b0; --accent: #4f8cff;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 24px; background: var(--bg); color: var(--text);
         font-family: -apple-system, "PingFang SC", "Helvetica Neue", sans-serif; }
  h1 { font-size: 20px; margin: 0 0 4px; }
  h2 { font-size: 15px; margin: 0 0 8px; color: var(--muted); font-weight: 600; }
  .sub { color: var(--muted); font-size: 13px; margin-bottom: 20px; }
  .panel { background: var(--panel); border: 1px solid var(--border);
           border-radius: 10px; padding: 16px; margin-bottom: 20px; }
  canvas { width: 100%; display: block; }
  table { border-collapse: collapse; font-size: 13px; }
  td, th { padding: 4px 14px 4px 0; text-align: left; }
  th { color: var(--muted); font-weight: 600; }
  .legend { display: flex; flex-wrap: wrap; gap: 14px; font-size: 12px;
            color: var(--muted); margin-top: 8px; }
  .legend span::before { content: ""; display: inline-block; width: 14px; height: 3px;
            background: var(--c); margin-right: 5px; vertical-align: middle; border-radius: 2px; }
  .ev { font-size: 12px; color: var(--muted); margin-top: 8px; line-height: 1.7; }
  .ev b { font-weight: 600; }
  .tag { display: inline-block; padding: 0 6px; border-radius: 4px; font-size: 11px; margin-right: 4px; }
  .tag.failure { background: #4a1f24; color: #ff7a85; }
  .tag.reclaim { background: #4a3a1a; color: #ffc24b; }
  .tag.recovery { background: #1d4030; color: #4ade80; }
</style>
</head>
<body>
<h1 id="title"></h1>
<div class="sub" id="subtitle"></div>

<div class="panel">
  <h2>运行摘要</h2>
  <table id="summary"></table>
</div>

<div class="panel">
  <h2>全局配额池与被租约占用的令牌</h2>
  <canvas id="chartGlobal" height="220"></canvas>
  <div class="legend" id="legendGlobal"></div>
</div>

<div class="panel">
  <h2>各节点本地令牌</h2>
  <canvas id="chartLocal" height="220"></canvas>
  <div class="legend" id="legendLocal"></div>
</div>

<div class="panel">
  <h2>各节点累计放行请求数</h2>
  <canvas id="chartAllowed" height="220"></canvas>
  <div class="legend" id="legendAllowed"></div>
</div>

<div class="panel">
  <h2>事件时间线（故障 / 回收 / 恢复）</h2>
  <div class="ev" id="events"></div>
</div>

<script>
var DATA = /*__DATA__*/;

var PALETTE = ["#4f8cff","#ff7a85","#4ade80","#ffc24b","#c084fc","#22d3ee","#fb923c","#a3e635"];

document.getElementById("title").textContent = DATA.title;
document.getElementById("subtitle").textContent =
  "容量 C=" + DATA.config.capacity +
  " · 补充速率 r=" + DATA.config.ratePerSec + "/s" +
  " · 租约 TTL=" + DATA.config.leaseTTLs + "s" +
  " · 理论超额上界 N·B=" + DATA.config.bound;

// 摘要表
(function () {
  var s = DATA.summary;
  var rows = [
    ["总放行请求", s.totalAllowed],
    ["总拒绝请求", s.totalDenied],
    ["协调器累计发放令牌", s.granted.toFixed(1)],
    ["节点主动归还", s.returned.toFixed(1)],
    ["故障/过期回收", s.reclaimed.toFixed(1)]
  ];
  var html = "";
  for (var i = 0; i < rows.length; i++) {
    html += "<tr><th>" + rows[i][0] + "</th><td>" + rows[i][1] + "</td></tr>";
  }
  document.getElementById("summary").innerHTML = html;
})();

// 事件列表
(function () {
  var names = { failure: "节点故障", reclaim: "配额回收", recovery: "节点恢复" };
  var html = "";
  for (var i = 0; i < DATA.events.length; i++) {
    var e = DATA.events[i];
    html += "<div><span class='tag " + e.type + "'>" + names[e.type] + "</span>" +
            "<b>t=" + e.t.toFixed(2) + "s</b> · 节点 " + e.node + " · " + e.detail + "</div>";
  }
  if (!DATA.events.length) html = "<div>无事件</div>";
  document.getElementById("events").innerHTML = html;
})();

// 通用折线图：series = [{name, color, points:[[t,y],...], dash}]
function drawChart(canvasId, legendId, series, opts) {
  opts = opts || {};
  var canvas = document.getElementById(canvasId);
  var dpr = window.devicePixelRatio || 1;
  var cssW = canvas.clientWidth, cssH = 220;
  canvas.width = cssW * dpr; canvas.height = cssH * dpr;
  var ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  var padL = 52, padR = 12, padT = 10, padB = 26;
  var w = cssW - padL - padR, h = cssH - padT - padB;

  var tMin = Infinity, tMax = -Infinity, yMax = 0;
  series.forEach(function (s) {
    s.points.forEach(function (p) {
      if (p[0] < tMin) tMin = p[0];
      if (p[0] > tMax) tMax = p[0];
      if (p[1] > yMax) yMax = p[1];
    });
  });
  if (opts.yMinMax) yMax = Math.max(yMax, opts.yMinMax);
  yMax *= 1.08;
  if (yMax === 0) yMax = 1;
  if (tMax <= tMin) tMax = tMin + 1;

  function X(t) { return padL + (t - tMin) / (tMax - tMin) * w; }
  function Y(v) { return padT + h - v / yMax * h; }

  // 网格与坐标轴
  ctx.strokeStyle = "#2a3550"; ctx.fillStyle = "#8a96b0";
  ctx.font = "10px sans-serif"; ctx.lineWidth = 1;
  var i, v, t;
  for (i = 0; i <= 4; i++) {
    v = yMax * i / 4;
    ctx.beginPath(); ctx.moveTo(padL, Y(v)); ctx.lineTo(padL + w, Y(v)); ctx.stroke();
    ctx.fillText(v.toFixed(v < 10 ? 1 : 0), 6, Y(v) + 3);
  }
  for (i = 0; i <= 6; i++) {
    t = tMin + (tMax - tMin) * i / 6;
    ctx.fillText(t.toFixed(1) + "s", X(t) - 10, padT + h + 16);
  }

  // 事件竖线
  var evColor = { failure: "#ff7a85", reclaim: "#ffc24b", recovery: "#4ade80" };
  DATA.events.forEach(function (e) {
    var x = X(e.t);
    ctx.strokeStyle = evColor[e.type] || "#fff";
    ctx.setLineDash([4, 4]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, padT + h); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = evColor[e.type] || "#fff";
    var label = e.type === "failure" ? "故障" : e.type === "reclaim" ? "回收" : "恢复";
    ctx.fillText(label + " N" + e.node, x + 3, padT + 10);
  });

  // 折线
  series.forEach(function (s) {
    ctx.strokeStyle = s.color; ctx.lineWidth = 1.6;
    if (s.dash) ctx.setLineDash([5, 3]);
    ctx.beginPath();
    s.points.forEach(function (p, idx) {
      if (idx === 0) ctx.moveTo(X(p[0]), Y(p[1]));
      else ctx.lineTo(X(p[0]), Y(p[1]));
    });
    ctx.stroke();
    ctx.setLineDash([]);
  });

  // 图例
  var legend = document.getElementById(legendId);
  legend.innerHTML = series.map(function (s) {
    return "<span style='--c:" + s.color + "'>" + s.name + "</span>";
  }).join("");
}

var samples = DATA.samples;
function col(fn) {
  return samples.map(function (s) { return [s.t, fn(s)]; });
}

// 图 1：全局池 + 在途租约
drawChart("chartGlobal", "legendGlobal", [
  { name: "全局池可用令牌", color: "#4f8cff", points: col(function (s) { return s.global; }) },
  { name: "被租约占用（在途）", color: "#ffc24b", points: col(function (s) { return s.outstanding; }) },
  { name: "容量上限 C", color: "#8a96b0", dash: true,
    points: [[samples[0].t, DATA.config.capacity], [samples[samples.length - 1].t, DATA.config.capacity]] }
]);

// 图 2：各节点本地令牌
var localSeries = [];
for (var n = 0; n < DATA.nodes; n++) {
  (function (idx) {
    localSeries.push({
      name: "节点 " + idx, color: PALETTE[idx % PALETTE.length],
      points: col(function (s) { return s.local[idx]; })
    });
  })(n);
}
drawChart("chartLocal", "legendLocal", localSeries);

// 图 3：累计放行
var allowedSeries = [];
for (var m = 0; m < DATA.nodes; m++) {
  (function (idx) {
    allowedSeries.push({
      name: "节点 " + idx, color: PALETTE[idx % PALETTE.length],
      points: col(function (s) { return s.allowedTotal[idx]; })
    });
  })(m);
}
allowedSeries.push({
  name: "理论上限 C + r·t + N·B", color: "#ff7a85", dash: true,
  points: col(function (s) {
    return DATA.config.capacity + DATA.config.ratePerSec * s.t + DATA.config.bound;
  })
});
drawChart("chartAllowed", "legendAllowed", allowedSeries);
</script>
</body>
</html>
`
