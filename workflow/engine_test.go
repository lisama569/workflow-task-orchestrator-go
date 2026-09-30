package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 所有测试用例通过 WithLogger(t.Logf) 打印任务状态流转与每次尝试结果
// （go test -v 可见）。

func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) {
		t.Helper()
		t.Logf(format, args...)
	}
}

func TestHappyPathDependencyOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(name string) Executor {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	execs := ExecutorRegistry{
		"a": record("a"),
		"b": record("b"),
		"c": record("c"),
	}
	spec := &WorkflowSpec{
		WorkflowID:     "happy",
		MaxConcurrency: 2,
		Tasks: []TaskSpec{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c", Dependencies: []string{"a", "b"}},
		},
	}
	eng := NewEngine(NewMemoryStore(), execs, WithLogger(testLogger(t)))
	st, err := eng.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status = %s", st.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("bad execution order: %v", order)
	}
	for _, name := range []string{"a", "b", "c"} {
		if st.Tasks[name].Status != StatusSucceeded {
			t.Fatalf("task %s not succeeded: %s", name, st.Tasks[name].Status)
		}
	}
}

func TestCycleDetected(t *testing.T) {
	spec := &WorkflowSpec{
		WorkflowID: "cycle",
		Tasks: []TaskSpec{
			{Name: "a", Dependencies: []string{"c"}},
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c", Dependencies: []string{"b"}},
		},
	}
	eng := NewEngine(NewMemoryStore(), ExecutorRegistry{}, WithLogger(testLogger(t)))
	_, err := eng.Run(context.Background(), spec)
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CycleError, got %T: %v", err, err)
	}
	t.Logf("cycle error as expected: %v (path: %v)", err, ce.Cycle)
}

func TestMissingDependencyDetected(t *testing.T) {
	spec := &WorkflowSpec{
		WorkflowID: "missing",
		Tasks: []TaskSpec{
			{Name: "a", Dependencies: []string{"ghost"}},
		},
	}
	eng := NewEngine(NewMemoryStore(), ExecutorRegistry{}, WithLogger(testLogger(t)))
	_, err := eng.Run(context.Background(), spec)
	var me *MissingDependencyError
	if !errors.As(err, &me) {
		t.Fatalf("want *MissingDependencyError, got %T: %v", err, err)
	}
	if me.Task != "a" || len(me.Deps) != 1 || me.Deps[0] != "ghost" {
		t.Fatalf("unexpected missing dep detail: %+v", me)
	}
	t.Logf("missing dependency error as expected: %v", err)
}

func TestDependencyFailurePropagates(t *testing.T) {
	execs := ExecutorRegistry{
		"a": func(ctx context.Context) error { return errors.New("boom-a") },
		"b": func(ctx context.Context) error {
			t.Fatal("b must never run when dependency a failed")
			return nil
		},
		"c": func(ctx context.Context) error {
			t.Fatal("c must never run when transitive dependency failed")
			return nil
		},
	}
	spec := &WorkflowSpec{
		WorkflowID:     "depfail",
		MaxConcurrency: 4,
		Tasks: []TaskSpec{
			{Name: "a", MaxRetries: 1}, // 共 2 次尝试，全失败 -> FAILED
			{Name: "b", Dependencies: []string{"a"}},
			{Name: "c", Dependencies: []string{"b"}},
		},
	}
	eng := NewEngine(NewMemoryStore(), execs, WithLogger(testLogger(t)))
	st, err := eng.Run(context.Background(), spec)
	if err == nil {
		t.Fatal("expected workflow error")
	}

	// 重试耗尽错误与依赖失败错误必须都能 errors.As 出来、彼此可区分
	var retriesErr *RetriesExhaustedError
	if !errors.As(err, &retriesErr) {
		t.Fatalf("want *RetriesExhaustedError in joined error, got %v", err)
	}
	if retriesErr.Task != "a" || retriesErr.Attempts != 2 {
		t.Fatalf("unexpected retries err: %+v", retriesErr)
	}
	var depErr *DependencyFailedError
	if !errors.As(err, &depErr) {
		t.Fatalf("want *DependencyFailedError in joined error, got %v", err)
	}

	if st.Status != WFFailed {
		t.Fatalf("status = %s", st.Status)
	}
	if st.Tasks["a"].Status != StatusFailed || st.Tasks["a"].FailKind != FailKindRetries {
		t.Fatalf("task a should be FAILED/RETRIES_EXHAUSTED: %+v", st.Tasks["a"])
	}
	if st.Tasks["b"].Status != StatusSkipped || st.Tasks["b"].FailKind != FailKindDependency {
		t.Fatalf("task b should be SKIPPED: %+v", st.Tasks["b"])
	}
	if st.Tasks["c"].Status != StatusSkipped {
		t.Fatalf("task c should be transitively SKIPPED: %+v", st.Tasks["c"])
	}
	if n := len(st.Tasks["a"].Attempts); n != 2 {
		t.Fatalf("task a should record 2 attempts, got %d", n)
	}
	if st.Tasks["b"].Attempt != 0 {
		t.Fatalf("skipped task b must have zero attempts, got %d", st.Tasks["b"].Attempt)
	}
	t.Logf("a failed (%s); b,c skipped due to: %s", retriesErr, depErr)
}

func TestConcurrencyLimitRespected(t *testing.T) {
	var current, peak int64
	release := make(chan struct{})
	started := make(chan string, 8)

	mk := func(name string) Executor {
		return func(ctx context.Context) error {
			cur := atomic.AddInt64(&current, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			started <- name
			<-release // 阻塞，直到测试放行
			atomic.AddInt64(&current, -1)
			return nil
		}
	}
	execs := ExecutorRegistry{"a": mk("a"), "b": mk("b"), "c": mk("c"), "d": mk("d")}
	spec := &WorkflowSpec{
		WorkflowID:     "conc",
		MaxConcurrency: 2,
		Tasks: []TaskSpec{
			{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"},
		},
	}
	eng := NewEngine(NewMemoryStore(), execs, WithLogger(testLogger(t)))
	done := make(chan struct{})
	var st *WorkflowState
	var err error
	go func() {
		st, err = eng.Run(context.Background(), spec)
		close(done)
	}()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case n := <-started:
			got[n] = true
		case <-time.After(time.Second):
			t.Fatal("expected exactly 2 tasks to start, timed out")
		}
	}
	// 等待并确认没有第 3 个任务突破上限
	select {
	case n := <-started:
		t.Fatalf("concurrency limit breached, 3rd task started: %s", n)
	case <-time.After(50 * time.Millisecond):
	}
	if p := atomic.LoadInt64(&peak); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2", p)
	}
	close(release)
	<-done
	if err != nil || st.Status != WFSucceeded {
		t.Fatalf("unexpected result: status=%v err=%v", st.Status, err)
	}
	if len(got) != 2 {
		t.Fatalf("expected first wave of 2, got %v", got)
	}
	t.Logf("peak concurrency observed = %d (limit 2)", atomic.LoadInt64(&peak))
}

func TestRetriesExhausted(t *testing.T) {
	var attempts int64
	execs := ExecutorRegistry{
		"a": func(ctx context.Context) error {
			n := atomic.AddInt64(&attempts, 1)
			return fmt.Errorf("transient error #%d", n)
		},
	}
	spec := &WorkflowSpec{
		WorkflowID:     "retry-ex",
		MaxConcurrency: 1,
		Tasks:          []TaskSpec{{Name: "a", MaxRetries: 2}}, // 首次 + 2 次重试 = 3 次尝试
	}
	eng := NewEngine(NewMemoryStore(), execs,
		WithBackoff(NoBackoff{}), WithLogger(testLogger(t)))
	st, err := eng.Run(context.Background(), spec)
	if err == nil {
		t.Fatal("expected final error")
	}
	var re *RetriesExhaustedError
	if !errors.As(err, &re) {
		t.Fatalf("want *RetriesExhaustedError, got %T: %v", err, err)
	}
	if re.Task != "a" || re.Attempts != 3 || re.LastError != "transient error #3" {
		t.Fatalf("unexpected retries exhausted detail: %+v", re)
	}
	ts := st.Tasks["a"]
	if ts.Status != StatusFailed || ts.FailKind != FailKindRetries {
		t.Fatalf("task should be FAILED: %+v", ts)
	}
	if atomic.LoadInt64(&attempts) != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", attempts)
	}
	if len(ts.Attempts) != 3 {
		t.Fatalf("want 3 recorded attempts, got %d", len(ts.Attempts))
	}
	for i, a := range ts.Attempts {
		want := fmt.Sprintf("transient error #%d", i+1)
		if a.Err != want {
			t.Fatalf("attempt %d reason = %q, want %q", i+1, a.Err, want)
		}
		if !a.FinishedAt.After(a.StartedAt) && !a.FinishedAt.Equal(a.StartedAt) {
			t.Fatalf("attempt %d has bad timestamps", i+1)
		}
		t.Logf("attempt %d recorded: err=%q", a.AttemptNo, a.Err)
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	var attempts int64
	execs := ExecutorRegistry{
		"a": func(ctx context.Context) error {
			n := atomic.AddInt64(&attempts, 1)
			if n < 3 {
				return fmt.Errorf("fail #%d", n)
			}
			return nil
		},
	}
	spec := &WorkflowSpec{
		WorkflowID:     "retry-ok",
		MaxConcurrency: 1,
		Tasks:          []TaskSpec{{Name: "a", MaxRetries: 3}},
	}
	eng := NewEngine(NewMemoryStore(), execs,
		WithBackoff(ExponentialBackoff{Base: time.Millisecond, Max: 10 * time.Millisecond}),
		WithLogger(testLogger(t)))
	st, err := eng.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	ts := st.Tasks["a"]
	if ts.Status != StatusSucceeded {
		t.Fatalf("want SUCCEEDED, got %s", ts.Status)
	}
	if ts.Attempt != 3 || len(ts.Attempts) != 3 {
		t.Fatalf("want 3 attempts (2 fail + 1 success), got attempt=%d records=%d",
			ts.Attempt, len(ts.Attempts))
	}
	if ts.Attempts[0].Err != "fail #1" || ts.Attempts[1].Err != "fail #2" {
		t.Fatalf("attempt reasons not preserved: %+v", ts.Attempts)
	}
	if ts.Attempts[2].Err != "" {
		t.Fatalf("3rd attempt should have empty error, got %q", ts.Attempts[2].Err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("workflow status = %s", st.Status)
	}
	t.Logf("task succeeded on attempt %d", ts.Attempt)
}

// TestRestartResumeKeepsAttemptCount 验证崩溃/中断恢复边界：
//   - 已成功的任务不重复执行；
//   - 中断时正在执行的尝试结果未知 -> 回滚记账，恢复后重新跑；
//   - 中断时已记录的尝试次数保留，重试预算不被重置。
func TestRestartResumeKeepsAttemptCount(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir)

	var aRuns int64
	var cRuns int64
	var bAttempts int64 // 仅统计“当前进程”发起的尝试次数
	var resumed int32   // 0 = 首次运行；1 = 恢复后运行

	// 阻塞点：让第一次运行在 b 的第 3 次尝试执行期间被取消
	gateB := make(chan struct{})
	bOnThird := make(chan struct{})

	// bExec 模拟真实业务逻辑，行为由“当前是第几次执行 + 是否恢复后”决定：
	//   首次运行：第 1、2 次尝试失败，第 3 次阻塞到被取消（结果未知，不记账）；
	//   恢复运行：只执行 1 次并成功。
	bExec := func(ctx context.Context) error {
		n := atomic.AddInt64(&bAttempts, 1)
		if atomic.LoadInt32(&resumed) == 0 {
			if n <= 2 {
				return fmt.Errorf("b transient fail %d", n)
			}
			close(bOnThird)
			<-gateB
			return ctx.Err() // 被取消 -> 引擎丢弃这次尝试
		}
		return nil
	}

	makeExecs := func() ExecutorRegistry {
		return ExecutorRegistry{
			"a": func(ctx context.Context) error {
				atomic.AddInt64(&aRuns, 1)
				return nil
			},
			"b": bExec,
			"c": func(ctx context.Context) error {
				atomic.AddInt64(&cRuns, 1)
				return nil
			},
		}
	}

	spec := &WorkflowSpec{
		WorkflowID:     "restart-1",
		MaxConcurrency: 1,
		Tasks: []TaskSpec{
			{Name: "a"},
			{Name: "b", Dependencies: []string{"a"}, MaxRetries: 4},
			{Name: "c", Dependencies: []string{"b"}},
		},
	}

	// 第一次运行：a 成功，b 失败 2 次后第 3 次尝试卡在 RUNNING，此时取消
	ctx1, cancel1 := context.WithCancel(context.Background())
	eng1 := NewEngine(store, makeExecs(), WithLogger(prefixed(t, "RUN1")))
	done1 := make(chan struct{})
	go func() {
		_, _ = eng1.Run(ctx1, spec)
		close(done1)
	}()
	select {
	case <-bOnThird:
	case <-time.After(2 * time.Second):
		t.Fatal("b did not reach 3rd attempt")
	}
	cancel1()
	close(gateB)
	<-done1

	// 中断后的断点快照：a SUCCEEDED，b READY 且记账 2 次，c PENDING
	st, err := store.Load("restart-1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFRunning {
		t.Fatalf("interrupted workflow should remain RUNNING in store, got %s", st.Status)
	}
	if st.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("a should be SUCCEEDED, got %s", st.Tasks["a"].Status)
	}
	if st.Tasks["b"].Status != StatusReady || st.Tasks["b"].Attempt != 2 {
		t.Fatalf("b breakpoint = %s attempt=%d, want READY/2", st.Tasks["b"].Status, st.Tasks["b"].Attempt)
	}
	if len(st.Tasks["b"].Attempts) != 2 {
		t.Fatalf("b should retain 2 failed attempt records, got %d", len(st.Tasks["b"].Attempts))
	}
	t.Logf("breakpoint persisted: %s", summarize(st))

	// 模拟进程重启：全新 Engine + 全新执行器注册，从同一 FileStore 恢复。
	// 新进程计数器归零，但持久化的尝试预算不归零。
	atomic.StoreInt64(&aRuns, 0)
	atomic.StoreInt64(&cRuns, 0)
	atomic.StoreInt64(&bAttempts, 0)
	atomic.StoreInt32(&resumed, 1)
	eng2 := NewEngine(store, makeExecs(), WithLogger(prefixed(t, "RUN2")))
	st2, err2 := eng2.Run(context.Background(), spec)
	if err2 != nil {
		t.Fatalf("resumed run should succeed, got %v", err2)
	}
	if st2.Status != WFSucceeded {
		t.Fatalf("resumed workflow status = %s", st2.Status)
	}

	// a 已成功，重启后绝不重复执行
	if atomic.LoadInt64(&aRuns) != 0 {
		t.Fatalf("a must not run again after restart, got %d", aRuns)
	}
	// 恢复后 b 只发起 1 次尝试即成功；跨重启累计 3 次
	if atomic.LoadInt64(&bAttempts) != 1 {
		t.Fatalf("b should execute once after resume, got %d", bAttempts)
	}
	if got := len(st2.Tasks["b"].Attempts); got != 3 {
		t.Fatalf("b total attempts after resume = %d, want 3", got)
	}
	if st2.Tasks["b"].Attempt != 3 {
		t.Fatalf("b attempt counter = %d, want 3", st2.Tasks["b"].Attempt)
	}
	if atomic.LoadInt64(&cRuns) != 1 {
		t.Fatalf("c must run exactly once after resume, got %d", cRuns)
	}
	t.Logf("resumed result: %s", summarize(st2))
}

func prefixed(t *testing.T, p string) func(string, ...any) {
	logf := testLogger(t)
	return func(format string, args ...any) {
		logf("["+p+"] "+format, args...)
	}
}

func summarize(st *WorkflowState) string {
	out := fmt.Sprintf("workflow=%s", st.Status)
	for _, ts := range st.Tasks {
		out += fmt.Sprintf(" | %s=%s(attempt=%d,records=%d)",
			ts.Name, ts.Status, ts.Attempt, len(ts.Attempts))
	}
	return out
}

// TestResubmitTerminalWorkflowIdempotent 同一工作流重复提交：
// 终态直接返回既有结果，任务零重复执行，错误结论保持可复现。
func TestResubmitTerminalWorkflowIdempotent(t *testing.T) {
	var aRuns int64
	var bRuns int64
	execs := ExecutorRegistry{
		"a": func(ctx context.Context) error {
			atomic.AddInt64(&aRuns, 1)
			return errors.New("permanent failure")
		},
		"b": func(ctx context.Context) error {
			atomic.AddInt64(&bRuns, 1)
			t.Fatal("b must never run")
			return nil
		},
	}
	spec := &WorkflowSpec{
		WorkflowID:     "resubmit",
		MaxConcurrency: 2,
		Tasks: []TaskSpec{
			{Name: "a", MaxRetries: 1},
			{Name: "b", Dependencies: []string{"a"}},
		},
	}
	store := NewMemoryStore()
	eng := NewEngine(store, execs, WithLogger(prefixed(t, "SUBMIT1")))
	st1, err1 := eng.Run(context.Background(), spec)
	if err1 == nil || st1.Status != WFFailed {
		t.Fatalf("first submit should fail: status=%v err=%v", st1.Status, err1)
	}
	if atomic.LoadInt64(&aRuns) != 2 {
		t.Fatalf("a attempts = %d, want 2", aRuns)
	}

	// 第二次提交：a 已 FAILED、b 已 SKIPPED，二者都不应再执行
	st2, err2 := eng.Run(context.Background(), spec)
	if err2 == nil {
		t.Fatal("second submit must return the same distinguishable error")
	}
	var re *RetriesExhaustedError
	if !errors.As(err2, &re) || re.Task != "a" || re.Attempts != 2 {
		t.Fatalf("second submit error not reproducible: %v", err2)
	}
	var de *DependencyFailedError
	if !errors.As(err2, &de) || de.Task != "b" {
		t.Fatalf("second submit missing dependency error: %v", err2)
	}
	if st2.Status != WFFailed {
		t.Fatalf("status = %s", st2.Status)
	}
	if atomic.LoadInt64(&aRuns) != 2 || atomic.LoadInt64(&bRuns) != 0 {
		t.Fatalf("tasks re-executed on resubmit: a=%d b=%d", aRuns, bRuns)
	}
	if len(st2.Tasks["a"].Attempts) != 2 {
		t.Fatalf("attempt history altered on resubmit: %d", len(st2.Tasks["a"].Attempts))
	}
	t.Logf("resubmit returned identical terminal result, zero re-execution")
}

// TestRetryDoesNotExceedConcurrency 验证退避后的重试与其他任务竞争同一批槽位，
// 任意时刻“正在执行尝试”的任务数不超过 MaxConcurrency。
func TestRetryDoesNotExceedConcurrency(t *testing.T) {
	var current, peak int64
	mk := func(failFirst int) Executor {
		var n int64
		return func(ctx context.Context) error {
			cur := atomic.AddInt64(&current, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			k := atomic.AddInt64(&n, 1)
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			if k <= int64(failFirst) {
				return fmt.Errorf("fail %d", k)
			}
			return nil
		}
	}
	execs := ExecutorRegistry{"a": mk(2), "b": mk(2), "c": mk(1)}
	spec := &WorkflowSpec{
		WorkflowID:     "retry-conc",
		MaxConcurrency: 2,
		Tasks: []TaskSpec{
			{Name: "a", MaxRetries: 2},
			{Name: "b", MaxRetries: 2},
			{Name: "c", MaxRetries: 1},
		},
	}
	eng := NewEngine(NewMemoryStore(), execs,
		WithBackoff(ConstantBackoff{Delay: 2 * time.Millisecond}),
		WithLogger(testLogger(t)))
	st, err := eng.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status = %s", st.Status)
	}
	if p := atomic.LoadInt64(&peak); p > 2 {
		t.Fatalf("peak concurrency during retries = %d, exceeds limit 2", p)
	}
	total := 0
	for _, ts := range st.Tasks {
		total += len(ts.Attempts)
	}
	t.Logf("peak concurrency across all attempts = %d, total attempts = %d",
		atomic.LoadInt64(&peak), total)
}

// TestRecoverWhileRetrying 中断点卡在 RETRYING（退避中）时：
// 已记账的失败尝试保留，恢复后从下一次尝试继续，无需重跑已记账尝试。
func TestRecoverWhileRetrying(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir)

	var bAttempts int64
	var resumed int32
	inBackoff := make(chan struct{})

	bExec := func(ctx context.Context) error {
		n := atomic.AddInt64(&bAttempts, 1)
		if atomic.LoadInt32(&resumed) == 0 {
			if n == 1 {
				return errors.New("fail before backoff")
			}
			t.Fatal("first run should be cancelled during backoff, never attempt 2")
		}
		return nil // 恢复后第一次执行即成功
	}

	spec := &WorkflowSpec{
		WorkflowID:     "retry-restart",
		MaxConcurrency: 1,
		Tasks:          []TaskSpec{{Name: "b", MaxRetries: 2}},
	}

	// 长退避把断点稳定留在 RETRYING；用日志钩子确认退避已开始
	ctx1, cancel1 := context.WithCancel(context.Background())
	logf := prefixed(t, "RUN1")
	eng1 := NewEngine(store, ExecutorRegistry{"b": bExec},
		WithBackoff(ConstantBackoff{Delay: time.Hour}),
		WithLogger(func(format string, args ...any) {
			if strings.Contains(format, "backoff") {
				select {
				case <-inBackoff:
				default:
					close(inBackoff)
				}
			}
			logf(format, args...)
		}))
	done1 := make(chan struct{})
	go func() {
		_, _ = eng1.Run(ctx1, spec)
		close(done1)
	}()
	select {
	case <-inBackoff:
	case <-time.After(2 * time.Second):
		t.Fatal("never entered backoff")
	}
	cancel1()
	<-done1

	st, err := store.Load("retry-restart")
	if err != nil {
		t.Fatal(err)
	}
	if st.Tasks["b"].Status != StatusRetrying {
		t.Fatalf("breakpoint status = %s, want RETRYING", st.Tasks["b"].Status)
	}
	if st.Tasks["b"].Attempt != 1 || len(st.Tasks["b"].Attempts) != 1 {
		t.Fatalf("want 1 accounted attempt, got attempt=%d records=%d",
			st.Tasks["b"].Attempt, len(st.Tasks["b"].Attempts))
	}

	// 重启恢复：RETRYING -> READY，attempt 保持 1，恢复后第 2 次尝试成功
	atomic.StoreInt64(&bAttempts, 0)
	atomic.StoreInt32(&resumed, 1)
	eng2 := NewEngine(store, ExecutorRegistry{"b": bExec},
		WithBackoff(NoBackoff{}), WithLogger(prefixed(t, "RUN2")))
	st2, err2 := eng2.Run(context.Background(), spec)
	if err2 != nil {
		t.Fatalf("resumed run err: %v", err2)
	}
	ts := st2.Tasks["b"]
	if ts.Status != StatusSucceeded || ts.Attempt != 2 {
		t.Fatalf("b after resume = %s attempt=%d, want SUCCEEDED/2", ts.Status, ts.Attempt)
	}
	if len(ts.Attempts) != 2 || ts.Attempts[0].Err != "fail before backoff" {
		t.Fatalf("attempt history not preserved across RETRYING recovery: %+v", ts.Attempts)
	}
	if atomic.LoadInt64(&bAttempts) != 1 {
		t.Fatalf("b should execute exactly once after resume, got %d", bAttempts)
	}
	t.Logf("RETRYING recovery ok: %s", summarize(st2))
}
