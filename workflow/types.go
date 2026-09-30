package workflow

import (
	"context"
	"time"
)

// TaskStatus 任务的生命周期状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // 尚未开始
	StatusReady     TaskStatus = "READY"     // 依赖已满足，等待调度（或崩溃恢复后的断点）
	StatusRunning   TaskStatus = "RUNNING"   // 某次尝试正在执行
	StatusRetrying  TaskStatus = "RETRYING"  // 尝试失败，退避等待重试（仍由原 goroutine 接管）
	StatusSucceeded TaskStatus = "SUCCEEDED" // 最终成功
	StatusFailed    TaskStatus = "FAILED"    // 最终失败（重试耗尽）
	StatusSkipped   TaskStatus = "SKIPPED"   // 不会执行（上游失败导致）
)

// WorkflowStatus 工作流整体状态。
type WorkflowStatus string

const (
	WFRunning   WorkflowStatus = "RUNNING"
	WFSucceeded WorkflowStatus = "SUCCEEDED"
	WFFailed    WorkflowStatus = "FAILED"
)

// TaskSpec 是任务的静态描述（不含执行函数，执行函数由 ExecutorRegistry 注入）。
type TaskSpec struct {
	Name         string        // 任务唯一标识
	Dependencies []string      // 前置任务名
	MaxRetries   int           // 最大重试次数（不含首次尝试，0 表示不重试）
	Timeout      time.Duration // 单次尝试超时，0 表示不限制
}

// WorkflowSpec 是工作流的静态描述。
type WorkflowSpec struct {
	WorkflowID string
	Tasks      []TaskSpec
	// MaxConcurrency 整体并发上限（同时处于 RUNNING 的任务数），<=0 表示不限。
	MaxConcurrency int
}

// Attempt 记录单次尝试的结果。
type Attempt struct {
	AttemptNo  int       `json:"attempt_no"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Err        string    `json:"err,omitempty"` // 空表示成功
}

// FailKind 标识最终失败的可区分原因（持久化，重启后仍可判定错误类型）。
type FailKind string

const (
	FailKindNone       FailKind = "" // 未失败
	FailKindRetries    FailKind = "RETRIES_EXHAUSTED"
	FailKindDependency FailKind = "DEPENDENCY_FAILED"
	FailKindNoExecutor FailKind = "NO_EXECUTOR"
)

// TaskState 是任务的运行时状态（持久化对象）。
type TaskState struct {
	Name       string     `json:"name"`
	Status     TaskStatus `json:"status"`
	Attempt    int        `json:"attempt"` // 已完成记账的尝试序号（0 表示尚未记账）
	Attempts   []Attempt  `json:"attempts"`
	UpdatedAt  time.Time  `json:"updated_at"`
	FailKind   FailKind   `json:"fail_kind,omitempty"`
	FailReason string     `json:"fail_reason,omitempty"`
	FailedDeps []string   `json:"failed_deps,omitempty"` // 因依赖失败而被跳过时记录
}

// WorkflowState 是工作流的运行时状态（持久化对象）。
type WorkflowState struct {
	WorkflowID string                `json:"workflow_id"`
	Status     WorkflowStatus        `json:"status"`
	Tasks      map[string]*TaskState `json:"tasks"`
	UpdatedAt  time.Time             `json:"updated_at"`
}

// Executor 是任务执行函数。返回 error 表示本次尝试失败。
type Executor func(ctx context.Context) error

// ExecutorRegistry 按任务名提供执行函数（重启后需重新注册）。
type ExecutorRegistry map[string]Executor
