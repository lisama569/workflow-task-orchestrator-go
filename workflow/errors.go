package workflow

import "strings"

// CycleError：依赖图中存在环，任务无法形成合法的拓扑顺序。
type CycleError struct {
	Cycle []string // 构成环的任务名（含回到起点的路径）
}

func (e *CycleError) Error() string {
	return "workflow dependency cycle detected: " + strings.Join(e.Cycle, " -> ")
}

// MissingDependencyError：任务声明了工作流中不存在的依赖。
type MissingDependencyError struct {
	Task string
	Deps []string
}

func (e *MissingDependencyError) Error() string {
	return "task " + quote(e.Task) + " depends on missing task(s): " + joinQuoted(e.Deps)
}

// DependencyFailedError：任务的依赖出现最终失败 / 缺失 / 成环，任务因此被跳过、未执行。
// 这不是静默跳过：跳过原因会同时写进 TaskState.SkipReason 与聚合错误中。
type DependencyFailedError struct {
	Task string
	Deps []string // 导致跳过的上游任务
}

func (e *DependencyFailedError) Error() string {
	return "task " + quote(e.Task) + " skipped because of failed/skipped dependency(ies): " + joinQuoted(e.Deps)
}

// TaskFailedError：任务执行次数用尽（1 + MaxRetries）后仍然失败。
// Attempts 保留每一次尝试的错误原因，便于复现与排查。
type TaskFailedError struct {
	Task     string
	Attempts []Attempt
}

func (e *TaskFailedError) Error() string {
	var b strings.Builder
	b.WriteString("task ")
	b.WriteString(quote(e.Task))
	b.WriteString(" permanently failed after ")
	b.WriteString(itoa(len(e.Attempts)))
	b.WriteString(" attempt(s)")
	var reasons []string
	for i, a := range e.Attempts {
		if a.Err != "" {
			reasons = append(reasons, "attempt#"+itoa(i+1)+"="+a.Err)
		}
	}
	if len(reasons) > 0 {
		b.WriteString(": ")
		b.WriteString(strings.Join(reasons, "; "))
	}
	return b.String()
}

// WorkflowExecutionError：工作流运行结束后，聚合所有最终失败与被跳过的任务。
type WorkflowExecutionError struct {
	Failed  []string          // 重试耗尽的任务
	Skipped map[string]string // 被跳过任务 -> 跳过原因
}

func (e *WorkflowExecutionError) Error() string {
	var b strings.Builder
	b.WriteString("workflow finished with failures")
	if len(e.Failed) > 0 {
		b.WriteString("; failed tasks: ")
		b.WriteString(joinQuoted(e.Failed))
	}
	if len(e.Skipped) > 0 {
		b.WriteString("; skipped tasks: ")
		var parts []string
		for name, reason := range e.Skipped {
			parts = append(parts, quote(name)+"("+reason+")")
		}
		b.WriteString(strings.Join(parts, ", "))
	}
	return b.String()
}

func quote(s string) string { return "\"" + s + "\"" }

func joinQuoted(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = quote(x)
	}
	return strings.Join(parts, ", ")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
