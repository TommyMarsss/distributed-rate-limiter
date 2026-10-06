# 分布式令牌桶限流器

多个节点共享同一个全局配额的令牌桶限流器。仅使用 Go 标准库；演示报告为单一静态
HTML 文件（原生 HTML/CSS/JavaScript，无框架、无图形库、不启动任何服务）。

```
limiter/            核心库：协调器、节点、网络/时钟仿真、HTML 报告
cmd/simreport/      演示：运行多节点场景并生成 report.html
```

```bash
go test ./...                 # 运行全部自动化测试
go run ./cmd/simreport        # 生成 report.html（浏览器直接打开）
```

## 一、配额协调协议（租约 + 定期同步）

系统由一个**协调器**（持有全局令牌池）和若干**节点**（持有本地配额）组成：

- 全局池容量 `C`、匀速补充速率 `R`（令牌/秒）。**所有补充与窗口计算只使用协调器
  时钟**（见第三节）。
- 节点每隔 `SyncPeriod` 秒向协调器发送一次同步消息；本地令牌在流量下用完后也会
  立即触发同步（"用完再申请"）。同步消息经模拟网络延迟（`MinDelay`–`MaxDelay`）
  送达。
- 同步消息携带：(a) 自上次同步以来**已消费**的令牌数；(b) 申请**补足**的令牌数
  （`want = Batch − 本地余量`）。
- 协调器收到后：核销旧租约（`Remaining −= consumed`，余量**结转**进新租约），
  从池中发放 `grant = min(want, Pool)`，新租约 `Remaining = 结转余量 + grant`，
  租约带 TTL。每个节点任意时刻至多持有一个有效租约。
- 节点在本地放行请求只扣本地配额，无需等待协调器，因此限流决策的延迟与协调器
  无关；协调器只负责配额的批发与回收。

### 关键不变式

```
Pool + Σ(所有未过期租约的 Remaining) ≤ C        (I)
```

补充时池的上限是 `C − 在外租约总量`（租出的令牌仍占用桶容量），发放时池不为负，
因此 (I) 恒成立。

## 二、超额上限的理论推导

设 `A(t)` 为到时刻 `t` 全部节点累计放行的请求数。由 (I)：

```
A(t) ≤ 已发放总量 = (C + R·t) − Pool(t) − ΣRemaining(t) + 双花部分
     ≤ C + R·t + D(t)
```

`D(t)` 是**双花**：故障节点已消费但未上报的令牌，在租约过期时被协调器当作"未消费"
回收到池中、随后被再次发放消费。一个节点至多持有一个大小 ≤ `Batch` 的租约，因此
每个故障节点贡献的双花至多 `Batch`：

```
A(t) ≤ C + R·t + F·B
```

- `C`：桶容量（允许的瞬时突发）；
- `R·t`：匀速补充的长期速率；
- `F`：到 `t` 为止发生故障的节点数；`B = Batch`。

**无故障时超额为零**：`A(t) ≤ C + R·t`，与单机令牌桶完全一致。任意窗口
`[t1, t2]` 内的放行量 ≤ `R·Δt + C + F·B`，即"短暂小幅超额"被严格限制在
`C + F·B` 以内。测试 `TestGlobalBoundConcurrent` / `TestFailureReclaim` /
`TestMultiNodeBurst` 在每个采样点验证 `A(t) − (C + R·t + F·B) ≤ 0`。

## 三、节点故障与配额回收

- 每份租约带 TTL（`ExpiresAt = 发放时刻 + TTL`）。节点故障后不再同步，其租约在
  TTL 到期时被协调器回收：`Pool += Remaining`，令牌重新进入全局可用池。
- 故障节点持有的令牌最迟在 `最后成功同步时刻 + 网络延迟上界 + TTL` 内回到池中
  （测试 `TestFailureReclaim` 验证该时限）。
- 故障期间被"冻结"的已消费未上报令牌是唯一的双花来源，已计入上界 `F·B`。
- 节点恢复后丢弃全部本地状态（崩溃即丢失），重新走正常同步流程领令牌；若它
  （违反假设地）上报了已过期租约的消费，协调器会将其计入 `Overshoot` 指标——
  该值同样以 `B` 为上界（`TestCoordinatorExpiredLeaseReportCountsOvershoot`）。
- 因此故障节点不会永久占用配额：`TestFailedNodeDoesNotStarveOthers` 验证回收后
  存活节点恢复到约满额的全局速率。

## 四、时钟不同步的处理

设计上**消除了节点时钟对限流窗口的一切影响**：

- 令牌补充、租约过期、窗口边界的全部时间计算只使用协调器时钟（消息到达时刻）；
- 节点本地时钟（可有 ±偏移）仅用于决定"何时发同步"，即只影响同步相位，不影响
  任何配额数值。

因此时钟偏移至多让某节点的同步请求早到/晚到 `|offset|` 秒，带来的放行差异上界为
一个批次加上偏移窗口内的补充量：

```
|A_i − A_j| ≤ 3B + R·(2·maxOffset + maxDelay)
```

测试 `TestClockSkewFairness`（偏移 ±0.3s）实测各节点放行差远小于该上界
（765 vs 787，约 3%）。

## 五、演示场景与报告

`cmd/simreport` 运行 60 秒场景：4 个节点，C=150、R=100 tok/s、B=20、TTL=4s、
同步周期 0.5s、网络延迟 20–120ms、时钟偏移 ±0.25s；基线流量 40 req/s/节点
（总需求 160 > R，持续过载）；t=20s 节点 2 故障、t=35s 恢复；t=40–45s 节点 1、3
突发 200 req/s。

生成的 `report.html` 包含：

- 全局令牌池水位曲线（含容量上限线）；
- 各节点本地配额曲线；
- 累计放行 vs 理论上界 `C + R·t + F·B` 曲线；
- 故障 / 配额回收 / 恢复事件在所有图上以竖线高亮，并附事件表与节点统计。

典型结果：累计放行 6065 ≤ 上界 6170，越界峰值 0；故障节点的 12.2 个令牌在
TTL 内回收；各节点放行占比与流量占比一致，时钟偏移未造成不成比例的优势。

## 测试一览

| 测试 | 验证点 |
|---|---|
| `TestCoordinatorGrantNeverExceedsPool` | 发放不超过池余额，不变式 (I) |
| `TestCoordinatorRefillCapsAtCapacityMinusOutstanding` | 补充上限 = C − 在外租约 |
| `TestCoordinatorSyncTopUpCarriesLeftover` | 同步核销与余量结转 |
| `TestCoordinatorLeaseExpiryReclaims` | 租约恰在 TTL 到期时回收 |
| `TestCoordinatorExpiredLeaseReportCountsOvershoot` | 过期后上报计入 Overshoot |
| `TestGlobalBoundConcurrent` | 6 节点并发过载下 `A(t) ≤ C + R·t` 恒成立 |
| `TestFailureReclaim` | 故障后配额在 TTL 内回收，`A(t) ≤ C + R·t + B` |
| `TestFailedNodeDoesNotStarveOthers` | 回收后其余节点恢复满额速率 |
| `TestClockSkewFairness` | ±0.3s 时钟偏移下放行比例公平 |
| `TestSingleNodeLowTraffic` | 单节点小流量几乎零拒绝 |
| `TestSingleNodeOverload` | 单节点超限流量被约束在 `C + R·t` |
| `TestMultiNodeBurst` | 多节点同时突发不越界且每节点都有份额 |
