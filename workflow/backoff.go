package workflow

import "time"

// BackoffPolicy 计算第 attempt 次失败后的等待时长。
// attempt 从 1 开始（第 1 次尝试失败后的退避）。
type BackoffPolicy interface {
	Backoff(attempt int) time.Duration
}

// ConstantBackoff 固定退避。
type ConstantBackoff struct {
	Delay time.Duration
}

func (b ConstantBackoff) Backoff(attempt int) time.Duration {
	return b.Delay
}

// ExponentialBackoff 指数退避：Base * 2^(attempt-1)，封顶 Max。
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b ExponentialBackoff) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if b.Max > 0 && d >= b.Max {
			return b.Max
		}
	}
	if b.Max > 0 && d > b.Max {
		return b.Max
	}
	return d
}

// NoBackoff 不退避（测试默认）。
type NoBackoff struct{}

func (b NoBackoff) Backoff(attempt int) time.Duration { return 0 }
