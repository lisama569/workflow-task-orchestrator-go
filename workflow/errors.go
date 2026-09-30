package workflow

import (
	"strconv"
	"strings"
)

// 本文件定义可区分的错误类型，调用方可用 errors.As 判定失败原因。

// CycleError 依赖图中存在环。
type CycleError struct {
	Cycle []string
}

func (e *CycleError) Error() string {
	return "workflow has dependency cycle: " + strings.Join(e.Cycle, " -> ")
}

// MissingDependencyError 任务依赖了不存在的任务。
type MissingDependencyError struct {
	Task string
	Deps []string
}

func (e *MissingDependencyError) Error() string {
	return "task " + e.Task + " depends on missing tasks: " + strings.Join(e.Deps, ", ")
}

// DuplicateTaskError 工作流内出现重名任务。
type DuplicateTaskError struct {
	Task string
}

func (e *DuplicateTaskError) Error() string {
	return "duplicate task name: " + e.Task
}

// DependencyFailedError 任务的前置依赖失败或被跳过，任务无法执行。
type DependencyFailedError struct {
	Task       string
	FailedDeps []string
}

func (e *DependencyFailedError) Error() string {
	return "task " + e.Task + " skipped due to failed dependencies: " + strings.Join(e.FailedDeps, ", ")
}

// RetriesExhaustedError 任务重试耗尽，标记为最终失败。
type RetriesExhaustedError struct {
	Task      string
	Attempts  int
	LastError string
}

func (e *RetriesExhaustedError) Error() string {
	return "task " + e.Task + " retries exhausted after " +
		strconv.Itoa(e.Attempts) + " attempt(s): " + e.LastError
}

// ExecutorNotFoundError 找不到任务的执行函数。
type ExecutorNotFoundError struct {
	Task string
}

func (e *ExecutorNotFoundError) Error() string {
	return "executor not registered for task: " + e.Task
}
