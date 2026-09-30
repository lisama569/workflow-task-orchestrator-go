package workflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Option 配置 Engine。
type Option func(*Engine)

// WithLogger 注入日志函数，用于打印任务状态流转与每次尝试结果。
func WithLogger(f func(string, ...any)) Option {
	return func(e *Engine) { e.logf = f }
}

// WithClock 注入时钟，便于测试中控制时间。
func WithClock(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// Engine 是本地工作流编排引擎。状态通过 Store 持久化，因此：
//   - 重复提交已完成的工作流时直接复用历史结论，不重复执行任务；
//   - 中断（进程崩溃 / context 取消）后重新提交时，已到终态的任务
//     不重跑，未到终态的任务从断点重新调度。
type Engine struct {
	store Store
	now   func() time.Time
	logf  func(string, ...any)
}

// NewEngine 创建一个本地编排引擎。
func NewEngine(store Store, opts ...Option) *Engine {
	e := &Engine{
		store: store,
		now:   time.Now,
		logf:  func(format string, args ...any) { log.Printf("workflow: "+format, args...) },
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Submit 按依赖关系并发调度工作流。
//
// 返回的 error 只表示"工作流没能进入正常运行"这类提交级结论
// （依赖成环、依赖缺失、定义与历史快照不一致等），使用 errors.As
// 可区分具体类型；任务级失败体现在 WorkflowResult.RunErr 中。
func (e *Engine) Submit(ctx context.Context, spec WorkflowSpec) (*WorkflowResult, error) {
	if spec.ID == "" {
		return nil, errors.New("workflow id is required")
	}
	specs, err := buildSpecMap(spec)
	if err != nil {
		return nil, err
	}
	if err := validateDAG(specs); err != nil {
		return nil, err
	}

	snap, err := e.store.Load(spec.ID)
	if err != nil {
		return nil, fmt.Errorf("load workflow %q snapshot: %w", spec.ID, err)
	}

	// 已跑到终态：直接复用结论，不重复执行 / 重复计数任何任务。
	if snap != nil && snap.Finished {
		e.logf("[%s] finished snapshot found, reuse historical result (no task executed)", spec.ID)
		return e.resultFromSnapshot(spec.ID, snap, true), nil
	}

	r, err := e.newRun(spec, specs, snap)
	if err != nil {
		return nil, err
	}

	if err := r.execute(ctx); err != nil {
		// 中断（取消 / 崩溃模拟）：返回 ctx 错误；快照保留在可恢复断点。
		return nil, err
	}
	return r.result(false), nil
}

// ---------- 运行期结构 ----------

type run struct {
	e         *Engine
	id        string
	specs     map[string]TaskSpec
	states    map[string]*TaskState
	terminal  map[string]bool // 恢复时已在终态的任务：直接放行其下游，不再调度
	snap      *WorkflowSnapshot
	sem       chan struct{}
	done      map[string]chan struct{} // 任务到达终态时关闭
	doneOnce  map[string]*sync.Once    // 保证每个任务的 done channel 只关闭一次（恢复路径与运行路径可能竞争）
	recovered bool
	mu        sync.Mutex // 保护 states / snap 的一切读写，以及持久化期间的快照一致性
	wg        sync.WaitGroup
}

func (e *Engine) newRun(spec WorkflowSpec, specs map[string]TaskSpec, snap *WorkflowSnapshot) (*run, error) {
	states := make(map[string]*TaskState, len(spec.Tasks))
	for name := range specs {
		states[name] = &TaskState{Name: name, Status: StatusPending, Updated: e.now()}
	}

	r := &run{
		e:        e,
		id:       spec.ID,
		specs:    specs,
		states:   states,
		terminal: map[string]bool{},
		snap:     &WorkflowSnapshot{WorkflowID: spec.ID, Tasks: states},
		done:     make(map[string]chan struct{}, len(specs)),
		doneOnce: make(map[string]*sync.Once, len(specs)),
	}

	cap := spec.MaxConcurrency
	if cap <= 0 {
		cap = len(specs)
	}
	r.sem = make(chan struct{}, cap)

	for name := range specs {
		r.done[name] = make(chan struct{})
		r.doneOnce[name] = &sync.Once{}
	}

	if snap != nil {
		// 快照存在但未到终态 => 断点恢复。
		if len(snap.Tasks) != len(specs) {
			return nil, fmt.Errorf("workflow %q definition changed: snapshot has %d tasks, spec has %d",
				spec.ID, len(snap.Tasks), len(specs))
		}
		r.recovered = true
		r.snap.Recovered = true
		for name, st := range snap.Tasks {
			if _, ok := specs[name]; !ok {
				return nil, fmt.Errorf("workflow %q definition changed: task %q in snapshot not in spec", spec.ID, name)
			}
			switch st.Status {
			case StatusSucceeded, StatusFailed, StatusSkipped:
				// 终态：完整保留（含结果 / 每次尝试 / 跳过原因），不重跑；
				// 直接放行其依赖者的等待。
				st.Updated = e.now()
				states[name] = st
				r.terminal[name] = true
				r.finish(name)
				e.logf("[%s] recover task %q at terminal state %s (not re-executed)", spec.ID, name, st.Status)
			default:
				// pending/ready/running/retrying 均非终态：回到 pending 重新调度；
				// 已落盘的失败尝试记录保留并计入总尝试次数。
				kept := len(st.Attempts)
				st.Status = StatusPending
				st.Updated = e.now()
				states[name] = st
				e.logf("[%s] recover task %q from non-terminal state -> pending (kept %d recorded attempt(s))",
					spec.ID, name, kept)
			}
		}
	}

	if err := e.store.Save(r.snap); err != nil {
		return nil, fmt.Errorf("save initial snapshot: %w", err)
	}
	return r, nil
}

// execute 启动每个任务的调度协程。
func (r *run) execute(ctx context.Context) error {
	abort := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			close(abort)
		case <-abort:
		}
	}()

	names := make([]string, 0, len(r.specs))
	for name := range r.specs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if r.terminal[name] {
			continue // 恢复的终态任务：不启动协程、不重跑
		}
		r.wg.Add(1)
		go r.runTask(ctx, abort, name)
	}
	r.wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.snap.Finished = true
	r.snap.Success = r.allSucceeded()
	if err := r.e.store.Save(r.snap); err != nil {
		return fmt.Errorf("save final snapshot: %w", err)
	}
	return nil
}

// runTask 是单个任务的状态机：等依赖 -> 抢并发名额 -> 尝试/退避循环 -> 落盘终态。
func (r *run) runTask(ctx context.Context, abort <-chan struct{}, name string) {
	defer r.wg.Done()
	spec := r.specs[name]

	// 1) 等待所有依赖到达终态。
	for _, dep := range spec.Deps {
		ch := r.done[dep]
		select {
		case <-ch:
		case <-abort:
			return
		case <-ctx.Done():
			return
		}
	}

	// 2) 依赖结论：必须全部成功，否则显式跳过（不静默、不当成功）。
	var badDeps []string
	r.mu.Lock()
	for _, dep := range spec.Deps {
		if r.states[dep].Status != StatusSucceeded {
			badDeps = append(badDeps, dep)
		}
	}
	st := r.states[name]
	r.mu.Unlock()
	// ctx 已取消时不再做任何终态落盘（否则可能把"中断"误记为失败 / 跳过）。
	if ctx.Err() == nil && len(badDeps) > 0 {
		sort.Strings(badDeps)
		reason := (&DependencyFailedError{Task: name, Deps: badDeps}).Error()
		r.markSkipped(name, reason)
		return
	}
	if len(badDeps) > 0 {
		return
	}

	r.transition(name, StatusReady, "all dependencies succeeded, waiting for concurrency slot")

	// 3) 申请并发名额。重试 / 退避期间名额不释放，因此绝不突破上限。
	select {
	case r.sem <- struct{}{}:
	case <-abort:
		return
	case <-ctx.Done():
		return
	}
	defer func() { <-r.sem }()

	maxAttempts := 1 + spec.MaxRetries
	backoff := spec.Backoff
	if backoff == nil {
		backoff = defaultBackoff
	}

	// 断点恢复时，已落盘的失败尝试继续计入总次数。
	r.mu.Lock()
	attemptNo := len(st.Attempts) + 1
	r.mu.Unlock()
	for ; attemptNo <= maxAttempts; attemptNo++ {
		if isAborted(ctx, abort) {
			return
		}
		r.transition(name, StatusRunning, fmt.Sprintf("attempt %d/%d starts", attemptNo, maxAttempts))

		started := r.e.now()
		res, err := spec.Fn(ctx)
		ended := r.e.now()

		// 恢复边界：这次尝试是在中止信号下结束的（进程在途被杀的模拟），
		// 它的结果不可信 => 不记录、不进入终态，快照停留在最近一次可信流转；
		// 重启后该任务从 pending 断点重新调度。
		if isAborted(ctx, abort) {
			r.e.logf("[%s] task %q: attempt %d interrupted in-flight, result discarded (resume from checkpoint)",
				r.id, name, attemptNo)
			return
		}

		rec := Attempt{Index: attemptNo, Started: started, Ended: ended}
		if err != nil {
			rec.Err = err.Error()
		}

		if err == nil {
			r.mu.Lock()
			st.Result = res
			st.Attempts = append(st.Attempts, rec)
			r.mu.Unlock()
			r.e.logf("[%s] task %q attempt %d/%d succeeded: %q", r.id, name, attemptNo, maxAttempts, res)
			r.transition(name, StatusSucceeded, "")
			r.finish(name)
			return
		}

		r.mu.Lock()
		st.Attempts = append(st.Attempts, rec)
		attempts := append([]Attempt(nil), st.Attempts...)
		r.mu.Unlock()
		r.e.logf("[%s] task %q attempt %d/%d failed: %s", r.id, name, attemptNo, maxAttempts, err.Error())

		if attemptNo == maxAttempts {
			// 重试耗尽：最终失败，保留每一次尝试原因。
			r.transition(name, StatusFailed, (&TaskFailedError{Task: name, Attempts: attempts}).Error())
			r.finish(name)
			return
		}

		// 还有重试机会：进入退避（仍占用并发名额）。
		delay := backoff.Delay(attemptNo)
		r.transition(name, StatusRetrying,
			fmt.Sprintf("attempt %d failed, backoff %s before attempt %d", attemptNo, delay, attemptNo+1))
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-abort:
				timer.Stop()
				return
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}
}

// isAborted 判断本次运行是否已被中止（外部 cancel 或内部 abort）。
func isAborted(ctx context.Context, abort <-chan struct{}) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-abort:
		return true
	default:
		return false
	}
}

// markSkipped 在锁内写入跳过原因、落盘两次可解释流转，并通知下游。
func (r *run) markSkipped(name, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.states[name]
	st.SkipReason = reason
	r.setStateLocked(st, StatusReady, "")
	r.setStateLocked(st, StatusSkipped, reason)
	r.finish(name) // sync.Once 幂等，持锁调用也安全
}

// finish 关闭任务的终态通知 channel（一次性，幂等）。
func (r *run) finish(name string) {
	r.doneOnce[name].Do(func() { close(r.done[name]) })
}

// transition 加锁包装。
func (r *run) transition(name string, to TaskStatus, note string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setStateLocked(r.states[name], to, note)
}

// setStateLocked 记录一次状态流转日志并立即持久化快照；调用方必须持有 r.mu，
// 从而保证序列化快照时没有任何任务协程并发修改 states。
func (r *run) setStateLocked(st *TaskState, to TaskStatus, note string) {
	name, from := st.Name, st.Status
	st.Status = to
	st.Updated = r.e.now()
	if note == "" {
		r.e.logf("[%s] task %q: %s -> %s", r.id, name, from, to)
	} else {
		r.e.logf("[%s] task %q: %s -> %s (%s)", r.id, name, from, to, note)
	}
	if err := r.e.store.Save(r.snap); err != nil {
		r.e.logf("[%s] task %q: persist snapshot error: %v", r.id, name, err)
	}
}

func (r *run) allSucceeded() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, st := range r.states {
		if st.Status != StatusSucceeded {
			return false
		}
	}
	return true
}

func (r *run) result(reused bool) *WorkflowResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.e.resultFromSnapshot(r.id, r.snap, reused)
}

func (e *Engine) resultFromSnapshot(id string, snap *WorkflowSnapshot, reused bool) *WorkflowResult {
	res := &WorkflowResult{
		WorkflowID: id,
		Success:    snap.Finished && snap.Success,
		Tasks:      snap.Tasks,
		Recovered:  snap.Recovered,
		Reused:     reused,
	}
	if snap.Finished && !snap.Success {
		res.RunErr = buildExecutionError(snap.Tasks)
	}
	return res
}

func buildExecutionError(tasks map[string]*TaskState) *WorkflowExecutionError {
	werr := &WorkflowExecutionError{Skipped: map[string]string{}}
	for name, st := range tasks {
		switch st.Status {
		case StatusFailed:
			werr.Failed = append(werr.Failed, name)
		case StatusSkipped:
			werr.Skipped[name] = st.SkipReason
		}
	}
	sort.Strings(werr.Failed)
	return werr
}

// ---------- 静态校验 ----------

func buildSpecMap(spec WorkflowSpec) (map[string]TaskSpec, error) {
	m := make(map[string]TaskSpec, len(spec.Tasks))
	for _, t := range spec.Tasks {
		if t.Name == "" {
			return nil, errors.New("task name is required")
		}
		if t.Fn == nil {
			return nil, fmt.Errorf("task %q has no executor", t.Name)
		}
		if _, dup := m[t.Name]; dup {
			return nil, fmt.Errorf("duplicate task definition: %q", t.Name)
		}
		m[t.Name] = t
	}
	return m, nil
}

// validateDAG 先检查缺失依赖，再用 DFS 三色标记检查环，保证两类结论可区分。
func validateDAG(specs map[string]TaskSpec) error {
	// 缺失依赖
	type miss struct {
		task string
		deps []string
	}
	var first *miss
	for name, t := range specs {
		var missing []string
		seen := map[string]bool{}
		for _, d := range t.Deps {
			if d == name {
				// 自依赖也是环，交给环检测处理。
			}
			if _, ok := specs[d]; !ok {
				if !seen[d] {
					missing = append(missing, d)
					seen[d] = true
				}
			}
		}
		if len(missing) > 0 && first == nil {
			sort.Strings(missing)
			first = &miss{task: name, deps: missing}
		}
	}
	if first != nil {
		return &MissingDependencyError{Task: first.task, Deps: first.deps}
	}

	// 环检测（DFS），并在发现环时给出环上路径。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string

	names := make([]string, 0, len(specs))
	for n := range specs {
		names = append(names, n)
	}
	sort.Strings(names)

	var dfs func(string) []string
	dfs = func(u string) []string {
		color[u] = gray
		stack = append(stack, u)
		deps := append([]string(nil), specs[u].Deps...)
		sort.Strings(deps)
		for _, v := range deps {
			if color[v] == white {
				if cyc := dfs(v); cyc != nil {
					return cyc
				}
			} else if color[v] == gray {
				// 从 v 在栈中的位置开始构成环。
				i := 0
				for stack[i] != v {
					i++
				}
				cyc := append([]string(nil), stack[i:]...)
				cyc = append(cyc, v)
				return cyc
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = black
		return nil
	}

	for _, n := range names {
		if color[n] == white {
			if cyc := dfs(n); cyc != nil {
				return &CycleError{Cycle: cyc}
			}
		}
	}
	return nil
}
