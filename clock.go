package ratelimit

import "time"

// Clock 抽象时钟，单位为模拟时间。协调器与各节点可以持有不同的 Clock 实现，
// 用于模拟节点间的时钟偏移。
type Clock interface {
	Now() time.Duration
}

// simClock 直接读取模拟器的当前时间（真实时间）。
type simClock struct {
	sim *Simulator
}

func (c *simClock) Now() time.Duration { return c.sim.now }

// offsetClock 在真实时间基础上叠加固定偏移，模拟节点本地时钟与协调器时钟
// 不同步的场景。offset 为正表示节点时钟偏快，为负表示偏慢。
type offsetClock struct {
	sim    *Simulator
	offset time.Duration
}

func (c *offsetClock) Now() time.Duration { return c.sim.now + c.offset }
