package workflow

import (
	"context"
	"time"
)

// TaskStatus 表示一个任务在生命周期中的状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"   // 等待调度（依赖未满足或尚未被引擎选中）
	StatusReady     TaskStatus = "ready"     // 依赖已满足，等待并发名额
	StatusRunning   TaskStatus = "running"   // 正在执行某一次尝试
	StatusRetrying  TaskStatus = "retrying"  // 一次尝试失败，退避等待下一次尝试
	StatusSucceeded TaskStatus = "succeeded" // 最终成功
	StatusFailed    TaskStatus = "failed"    // 重试耗尽，最终失败
	StatusSkipped   TaskStatus = "skipped"   // 依赖失败 / 依赖缺失，未被执行
)

// Attempt 记录任务的一次执行尝试结果。
type Attempt struct {
	Index   int       `json:"index"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
	Err     string    `json:"err,omitempty"`
}

// TaskState 是一个任务可持久化的完整运行状态。
type TaskState struct {
	Name       string     `json:"name"`
	Status     TaskStatus `json:"status"`
	Attempts   []Attempt  `json:"attempts"`
	Result     string     `json:"result,omitempty"`
	SkipReason string     `json:"skip_reason,omitempty"`
	Updated    time.Time  `json:"updated"`
}

// TaskFunc 是任务的实际执行函数。可通过 context 感知取消。
type TaskFunc func(ctx context.Context) (string, error)

// TaskSpec 描述一个任务及其调度参数。
type TaskSpec struct {
	Name       string
	Deps       []string
	MaxRetries int           // 最大重试次数（不含首次执行）；0 表示只执行一次
	Backoff    BackoffPolicy // 退避策略；nil 表示不退避
	Fn         TaskFunc
}

// WorkflowSpec 描述一次工作流提交。
type WorkflowSpec struct {
	ID    string
	Tasks []TaskSpec
	// MaxConcurrency 为整个工作流同时处于执行状态（含退避等待）的任务数上限。
	MaxConcurrency int
}

// WorkflowResult 是一次工作流运行的最终结论。
type WorkflowResult struct {
	WorkflowID string
	Success    bool
	Tasks      map[string]*TaskState
	// RunErr 描述整个工作流级别的结论（环、缺失依赖、最终失败等）。
	RunErr error
	// Reused 为 true 表示该结果是从已完成的历史运行直接复用，未重复执行任何任务。
	Reused bool
	// Recovered 为 true 表示本次运行是从断点恢复的。
	Recovered bool
}

// Engine 的完整定义见 engine.go。
