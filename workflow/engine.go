package workflow

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Engine 是本地工作流编排引擎。零值不可用，请用 NewEngine。
type Engine struct {
	store   Store
	execs   ExecutorRegistry
	backoff BackoffPolicy
	logger  func(format string, args ...any)
	clock   func() time.Time
}

// Option 配置引擎。
type Option func(*Engine)

func WithBackoff(b BackoffPolicy) Option {
	return func(e *Engine) { e.backoff = b }
}

func WithLogger(f func(format string, args ...any)) Option {
	return func(e *Engine) { e.logger = f }
}

// withClock 仅用于测试，让时间戳可复现。
func withClock(c func() time.Time) Option {
	return func(e *Engine) { e.clock = c }
}

func noopLogger(string, ...any) {}

// NewEngine 构造引擎。
func NewEngine(store Store, execs ExecutorRegistry, opts ...Option) *Engine {
	e := &Engine{
		store:   store,
		execs:   execs,
		backoff: NoBackoff{},
		logger:  noopLogger,
		clock:   time.Now,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Run 提交并运行工作流。
// 同一 WorkflowID 重复提交：已完成任务不重复执行，从断点继续；
// 已到终态的工作流直接返回既有结果，保证可复现。
func (e *Engine) Run(ctx context.Context, spec *WorkflowSpec) (*WorkflowState, error) {
	if err := validate(spec); err != nil {
		return nil, err
	}

	specs := make(map[string]*TaskSpec, len(spec.Tasks))
	for i := range spec.Tasks {
		specs[spec.Tasks[i].Name] = &spec.Tasks[i]
	}

	st, err := e.store.Load(spec.WorkflowID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		st = newState(spec)
		if err := e.store.Save(st); err != nil {
			return nil, err
		}
	} else {
		// 恢复：任务集合必须与提交的 spec 一致
		if err := checkCompatible(st, spec); err != nil {
			return nil, err
		}
		e.recoverInterrupted(st)
		if err := e.store.Save(st); err != nil {
			return nil, err
		}
	}

	// 终态工作流重复提交：直接封板返回，不执行任何任务
	if st.Status != WFRunning {
		return st, e.finalError(st)
	}

	return e.schedule(ctx, spec, specs, st)
}

// recoverInterrupted 回滚上次中断时的非终态在飞任务：
//   - RUNNING 残留在执行中的尝试：结果未知，Attempt 回滚 1（Attempts 无该记录），转 READY；
//   - RETRYING 残留在退避中的尝试：已记账的尝试保留，转 READY，从“下一次尝试”继续。
func (e *Engine) recoverInterrupted(st *WorkflowState) {
	for _, t := range st.Tasks {
		switch t.Status {
		case StatusRunning:
			e.logger("[%s] recover: task %s RUNNING(residual) -> READY, attempt #%d discarded",
				st.WorkflowID, t.Name, t.Attempt)
			t.Status = StatusReady
			if t.Attempt > 0 {
				t.Attempt--
			}
			t.UpdatedAt = e.clock()
		case StatusRetrying:
			e.logger("[%s] recover: task %s RETRYING(residual) -> READY, resume from attempt #%d",
				st.WorkflowID, t.Name, t.Attempt+1)
			t.Status = StatusReady
			t.UpdatedAt = e.clock()
		}
	}
}

func checkCompatible(st *WorkflowState, spec *WorkflowSpec) error {
	if len(st.Tasks) != len(spec.Tasks) {
		return errors.New("workflow " + spec.WorkflowID +
			" cannot resume: task set differs from persisted state")
	}
	for _, t := range spec.Tasks {
		if _, ok := st.Tasks[t.Name]; !ok {
			return errors.New("workflow " + spec.WorkflowID +
				" cannot resume: task " + t.Name + " missing from persisted state")
		}
	}
	return nil
}

func (e *Engine) schedule(
	ctx context.Context,
	spec *WorkflowSpec,
	specs map[string]*TaskSpec,
	st *WorkflowState,
) (*WorkflowState, error) {
	var mu sync.Mutex
	inflight := 0
	kick := make(chan struct{}, 1)
	wake := func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	}

	// sem 限制“同时执行尝试”的数量；退避、等待期间不持槽，
	// 重试也要重新抢槽，因此重试期间不会突破并发上限。
	sem := make(chan struct{}, spec.MaxConcurrency)
	if spec.MaxConcurrency <= 0 {
		sem = make(chan struct{}, len(spec.Tasks)+1)
	}

	var wg sync.WaitGroup

	persist := func() {
		// 调用方必须持有 mu
		st.UpdatedAt = e.clock()
		if err := e.store.Save(cloneState(st)); err != nil {
			e.logger("[%s] persist error: %v", st.WorkflowID, err)
		}
	}

	transition := func(ts *TaskState, from, to TaskStatus, note string) {
		// 调用方必须持有 mu
		e.logger("[%s] task %s: %s -> %s%s", st.WorkflowID, ts.Name, from, to, note)
		ts.Status = to
		ts.UpdatedAt = e.clock()
	}

	startTask := func(name string) {
		ts := st.Tasks[name]
		t := specs[name]
		inflight++
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runTask(ctx, st.WorkflowID, t, ts, &mu, sem, wake, persist, transition)
			mu.Lock()
			inflight--
			mu.Unlock()
			wake()
		}()
	}

	for {
		mu.Lock()

		// 1) 失败传播：依赖 FAILED/SKIPPED 的未决任务标记 SKIPPED
		e.propagateFailures(st, specs, persist, transition)

		// 2) 终态判定
		if allTerminal(st) {
			mu.Unlock()
			if st.Status == WFRunning {
				mu.Lock()
				if e.hasFailure(st) {
					st.Status = WFFailed
				} else {
					st.Status = WFSucceeded
				}
				e.logger("[%s] workflow -> %s", st.WorkflowID, st.Status)
				persist()
				mu.Unlock()
			}
			return st, e.finalError(st)
		}

		// 3) 推进状态：依赖已满足的 PENDING 转 READY；READY 任务抢并发槽启动
		var started []string
		if ctx.Err() == nil {
			for _, t := range spec.Tasks {
				ts := st.Tasks[t.Name]
				if ts.Status == StatusPending && depsSatisfied(st, t.Dependencies) {
					transition(ts, StatusPending, StatusReady, "")
				}
				if ts.Status == StatusReady {
					select {
					case sem <- struct{}{}:
						transition(ts, StatusReady, StatusRunning, "")
						started = append(started, t.Name)
					default:
						// 并发槽已满，等下一轮唤醒
					}
				}
			}
		}
		if len(started) > 0 {
			persist()
		}
		mu.Unlock()

		for _, n := range started {
			startTask(n)
		}

		mu.Lock()
		w := inflight
		mu.Unlock()

		// ctx 已取消且没有在飞任务：工作流保持 RUNNING，断点已落盘
		if w == 0 && ctx.Err() != nil {
			return st, ctx.Err()
		}

		select {
		case <-kick:
		case <-ctx.Done():
			// 有在飞任务时唤醒主循环，让其回滚落盘后退出
		}
	}
}

// runTask 执行单个任务的全部尝试。mu 保护共享状态；sem 仅在真正执行尝试期间持有。
func (e *Engine) runTask(
	ctx context.Context,
	wfID string,
	t *TaskSpec,
	ts *TaskState,
	mu *sync.Mutex,
	sem chan struct{},
	wake func(),
	persist func(),
	transition func(ts *TaskState, from, to TaskStatus, note string),
) {
	exec, ok := e.execs[t.Name]
	if !ok {
		mu.Lock()
		err := &ExecutorNotFoundError{Task: t.Name}
		ts.FailKind = FailKindNoExecutor
		ts.FailReason = err.Error()
		transition(ts, ts.Status, StatusFailed, " ("+err.Error()+")")
		persist()
		mu.Unlock()
		<-sem // 释放调度循环预占的槽位
		return
	}

	maxAttempts := t.MaxRetries + 1
	for {
		mu.Lock()
		attemptNo := ts.Attempt + 1
		ts.Attempt = attemptNo
		// 进入此处时状态必为 RUNNING（首次由调度循环翻转，重试在抢到槽后翻转）
		ts.Status = StatusRunning
		e.logger("[%s] task %s running attempt %d/%d",
			wfID, ts.Name, attemptNo, maxAttempts)
		startedAt := e.clock()
		persist()
		mu.Unlock()

		attemptCtx := ctx
		var cancel context.CancelFunc
		if t.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, t.Timeout)
		}
		err := runOnce(attemptCtx, exec)
		if cancel != nil {
			cancel()
		}
		finishedAt := e.clock()

		// 中断：本次尝试结果未知，回滚计数回到 READY，等恢复后重跑
		if ctx.Err() != nil {
			mu.Lock()
			ts.Attempt = attemptNo - 1
			transition(ts, StatusRunning, StatusReady, " (interrupted; attempt discarded)")
			persist()
			mu.Unlock()
			<-sem
			return
		}

		mu.Lock()
		a := Attempt{AttemptNo: attemptNo, StartedAt: startedAt, FinishedAt: finishedAt}
		if err != nil {
			a.Err = err.Error()
			ts.Attempts = append(ts.Attempts, a)
			e.logger("[%s] task %s attempt %d/%d FAILED: %v",
				wfID, ts.Name, attemptNo, maxAttempts, err)

			if attemptNo >= maxAttempts {
				rerr := &RetriesExhaustedError{
					Task: t.Name, Attempts: attemptNo, LastError: err.Error(),
				}
				ts.FailKind = FailKindRetries
				ts.FailReason = rerr.Error()
				transition(ts, StatusRunning, StatusFailed, " (retries exhausted)")
				persist()
				mu.Unlock()
				<-sem
				return
			}

			transition(ts, StatusRunning, StatusRetrying, " (will retry after backoff)")
			persist()
			mu.Unlock()
			<-sem // 退避期间释放并发槽

			wait := e.backoff.Backoff(attemptNo)
			e.logger("[%s] task %s backoff %s before attempt %d",
				wfID, ts.Name, wait, attemptNo+1)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					// RETRYING 断点保留：已记账尝试不回滚，恢复后从下一次尝试继续
					wake()
					return
				}
			}
			// 重新竞争并发槽后开始下一次尝试（重试同样受上限约束）
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wake()
				return
			}
			mu.Lock()
			transition(ts, StatusRetrying, StatusRunning, " (retry acquired slot)")
			mu.Unlock()
			continue
		}

		ts.Attempts = append(ts.Attempts, a)
		e.logger("[%s] task %s attempt %d SUCCEEDED", wfID, ts.Name, attemptNo)
		transition(ts, StatusRunning, StatusSucceeded, "")
		persist()
		mu.Unlock()
		<-sem
		return
	}
}

func runOnce(ctx context.Context, exec Executor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("executor panic recovered: " + toString(r))
		}
	}()
	return exec(ctx)
}

func toString(v any) string {
	switch x := v.(type) {
	case error:
		return x.Error()
	case string:
		return x
	default:
		return "panic"
	}
}

// propagateFailures 把依赖失败/跳过的未决任务标记为 SKIPPED 并记录失败依赖。
// 必须在持有 mu 时调用，且每次状态翻转都持久化。
func (e *Engine) propagateFailures(
	st *WorkflowState,
	specs map[string]*TaskSpec,
	persist func(),
	transition func(ts *TaskState, from, to TaskStatus, note string),
) {
	changed := true
	for changed {
		changed = false
		for name, t := range specs {
			ts := st.Tasks[name]
			if ts.Status != StatusPending && ts.Status != StatusReady {
				continue
			}
			var bad []string
			for _, d := range t.Dependencies {
				if ds := st.Tasks[d]; ds != nil &&
					(ds.Status == StatusFailed || ds.Status == StatusSkipped) {
					bad = append(bad, d)
				}
			}
			if len(bad) > 0 {
				derr := &DependencyFailedError{Task: name, FailedDeps: bad}
				ts.FailKind = FailKindDependency
				ts.FailedDeps = bad
				ts.FailReason = derr.Error()
				transition(ts, ts.Status, StatusSkipped, " (upstream failed: "+joinNames(bad)+")")
				persist()
				changed = true
			}
		}
	}
}

func joinNames(ns []string) string {
	out := ""
	for i, n := range ns {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// depsSatisfied 判断依赖是否已到“有定论”状态（成功 / 失败 / 跳过）。
func depsSatisfied(st *WorkflowState, deps []string) bool {
	for _, d := range deps {
		ds := st.Tasks[d]
		if ds == nil {
			return false
		}
		switch ds.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped:
		default:
			return false
		}
	}
	return true
}

func allTerminal(st *WorkflowState) bool {
	for _, ts := range st.Tasks {
		if ts.Status != StatusSucceeded &&
			ts.Status != StatusFailed && ts.Status != StatusSkipped {
			return false
		}
	}
	return true
}

func (e *Engine) hasFailure(st *WorkflowState) bool {
	for _, ts := range st.Tasks {
		if ts.Status == StatusFailed || ts.Status == StatusSkipped {
			return true
		}
	}
	return false
}

// finalError 按持久化的 FailKind 重建可 errors.As 判定的错误集合。
func (e *Engine) finalError(st *WorkflowState) error {
	var errs []error
	for _, ts := range st.Tasks {
		switch ts.FailKind {
		case FailKindRetries:
			last := ""
			if len(ts.Attempts) > 0 {
				last = ts.Attempts[len(ts.Attempts)-1].Err
			}
			errs = append(errs, &RetriesExhaustedError{
				Task: ts.Name, Attempts: ts.Attempt, LastError: last,
			})
		case FailKindDependency:
			errs = append(errs, &DependencyFailedError{
				Task: ts.Name, FailedDeps: ts.FailedDeps,
			})
		case FailKindNoExecutor:
			errs = append(errs, &ExecutorNotFoundError{Task: ts.Name})
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
