package workflow

import "time"

// BackoffPolicy 决定一次失败后等待多久再重试。
// 参数 attempt 为"刚结束的尝试序号"，从 1 开始：
// 第 1 次尝试失败后，退避时长 = Delay(1)。
type BackoffPolicy interface {
	Delay(attempt int) time.Duration
}

// NoBackoff：立即重试。
type NoBackoff struct{}

func (b *NoBackoff) Delay(attempt int) time.Duration { return 0 }

// ConstantBackoff：每次失败后固定等待 Interval。
type ConstantBackoff struct{ Interval time.Duration }

func (b *ConstantBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return b.Interval
}

// ExponentialBackoff：Base * 2^(attempt-1)，并以 Max 封顶（Max>0 时生效）。
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b *ExponentialBackoff) Delay(attempt int) time.Duration {
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

// defaultBackoff：任务未显式配置时使用（立即重试）。
var defaultBackoff BackoffPolicy = &NoBackoff{}
