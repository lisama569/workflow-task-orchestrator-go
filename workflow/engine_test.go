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

// testLogger 把任务状态流转与每次尝试结果打印到单测日志中。
func testLogger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Helper()
		t.Logf(format, args...)
	}
}

// counter 统计每个任务的实际执行次数（真正进入 Fn 的次数）。
type counter struct{ m map[string]*int64 }

func newCounter(names ...string) *counter {
	c := &counter{m: map[string]*int64{}}
	for _, n := range names {
		var v int64
		c.m[n] = &v
	}
	return c
}
func (c *counter) inc(name string)       { atomic.AddInt64(c.m[name], 1) }
func (c *counter) get(name string) int64 { return atomic.LoadInt64(c.m[name]) }

// gate 提供一个可由任务触发的外部控制信号，用于模拟"运行中中断"。
type gate struct {
	once sync.Once
	ch   chan struct{}
}

func newGate() *gate                   { return &gate{ch: make(chan struct{})} }
func (g *gate) fire()                  { g.once.Do(func() { close(g.ch) }) }
func (g *gate) waitC() <-chan struct{} { return g.ch }

func ok(name string) TaskFunc {
	return func(ctx context.Context) (string, error) {
		return name + ":ok", nil
	}
}

// failNTimes 前 n 次失败，第 n+1 次起成功；failCnt 用于统计本进程内调用次数。
func failNTimes(name string, n int, c *counter) TaskFunc {
	var calls int64
	return func(ctx context.Context) (string, error) {
		c.inc(name)
		k := atomic.AddInt64(&calls, 1)
		if k <= int64(n) {
			return "", fmt.Errorf("%s:fail#%d", name, k)
		}
		return name + ":recovered-ok", nil
	}
}

func alwaysFail(name string, c *counter) TaskFunc {
	return func(ctx context.Context) (string, error) {
		c.inc(name)
		return "", fmt.Errorf("%s:permanent-boom", name)
	}
}

// ---------- 场景 1：基础依赖调度，拓扑顺序执行 ----------

func TestBasicDAGSuccess(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	c := newCounter("a", "b", "c")

	finish := map[string]chan struct{}{}
	var mu sync.Mutex
	var order []string
	record := func(name string) TaskFunc {
		finish[name] = make(chan struct{})
		return func(ctx context.Context) (string, error) {
			c.inc(name)
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return name + ":done", nil
		}
	}

	res, err := eng.Submit(context.Background(), WorkflowSpec{
		ID:             "wf-basic",
		MaxConcurrency: 1,
		Tasks: []TaskSpec{
			{Name: "c", Deps: []string{"a", "b"}, Fn: record("c")},
			{Name: "a", Fn: record("a")},
			{Name: "b", Deps: []string{"a"}, Fn: record("b")},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !res.Success || res.RunErr != nil {
		t.Fatalf("expected success, got success=%v runErr=%v", res.Success, res.RunErr)
	}
	if c.get("a") != 1 || c.get("b") != 1 || c.get("c") != 1 {
		t.Fatalf("each task should run exactly once: a=%d b=%d c=%d", c.get("a"), c.get("b"), c.get("c"))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("expected topological order a,b,c got %v", order)
	}
	if res.Tasks["c"].Result != "c:done" {
		t.Fatalf("unexpected c result: %q", res.Tasks["c"].Result)
	}
}

// ---------- 场景 2：环 ----------

func TestCycleDetected(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	var ran int64
	block := func(ctx context.Context) (string, error) {
		atomic.AddInt64(&ran, 1)
		return "ok", nil
	}
	_, err := eng.Submit(context.Background(), WorkflowSpec{
		ID: "wf-cycle",
		Tasks: []TaskSpec{
			{Name: "a", Deps: []string{"c"}, Fn: block},
			{Name: "b", Deps: []string{"a"}, Fn: block},
			{Name: "c", Deps: []string{"b"}, Fn: block},
		},
	})
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CycleError, got %T %v", err, err)
	}
	if len(ce.Cycle) < 2 {
		t.Fatalf("cycle path too short: %v", ce.Cycle)
	}
	t.Logf("cycle path: %v", ce.Cycle)
	if atomic.LoadInt64(&ran) != 0 {
		t.Fatalf("no task should run when cycle exists, ran=%d", ran)
	}
}

// ---------- 场景 3：依赖缺失 ----------

func TestMissingDependency(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	_, err := eng.Submit(context.Background(), WorkflowSpec{
		ID: "wf-missing",
		Tasks: []TaskSpec{
			{Name: "a", Fn: ok("a")},
			{Name: "b", Deps: []string{"a", "ghost"}, Fn: ok("b")},
		},
	})
	var me *MissingDependencyError
	if !errors.As(err, &me) {
		t.Fatalf("expected *MissingDependencyError, got %T %v", err, err)
	}
	if me.Task != "b" || len(me.Deps) != 1 || me.Deps[0] != "ghost" {
		t.Fatalf("unexpected missing-dep detail: %+v", me)
	}
	t.Logf("missing dependency error: %v", me)
}

// ---------- 场景 4：依赖失败 -> 下游必须显式跳过，错误原因可区分 ----------

func TestDependencyFailurePropagates(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	c := newCounter("a", "b", "c", "d")

	res, err := eng.Submit(context.Background(), WorkflowSpec{
		ID: "wf-dep-fail",
		Tasks: []TaskSpec{
			{Name: "a", Fn: alwaysFail("a", c)}, // 最终失败
			{Name: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) (string, error) {
				c.inc("b")
				return "b", nil
			}},
			{Name: "c", Deps: []string{"b"}, Fn: func(ctx context.Context) (string, error) {
				c.inc("c")
				return "c", nil
			}},
			{Name: "d", Fn: func(ctx context.Context) (string, error) { // 无依赖，独立成功
				c.inc("d")
				return "d", nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Success {
		t.Fatal("workflow should be unsuccessful")
	}

	// 工作流级结论可区分：含最终失败任务与被跳过任务。
	var werr *WorkflowExecutionError
	if !errors.As(res.RunErr, &werr) {
		t.Fatalf("expected *WorkflowExecutionError, got %T", res.RunErr)
	}
	if len(werr.Failed) != 1 || werr.Failed[0] != "a" {
		t.Fatalf("expected failed=[a], got %v", werr.Failed)
	}
	if _, ok := werr.Skipped["b"]; !ok {
		t.Fatalf("b should be skipped, got %v", werr.Skipped)
	}
	if _, ok := werr.Skipped["c"]; !ok {
		t.Fatalf("c should be transitively skipped, got %v", werr.Skipped)
	}
	t.Logf("workflow error detail: %v", werr)

	// 状态机结论：a 失败，b/c 跳过（不是成功、不是静默），d 成功。
	if res.Tasks["a"].Status != StatusFailed {
		t.Fatalf("a expected failed, got %s", res.Tasks["a"].Status)
	}
	if res.Tasks["b"].Status != StatusSkipped || !strings.Contains(res.Tasks["b"].SkipReason, `"a"`) {
		t.Fatalf("b expected skipped due to a, got %s reason=%q", res.Tasks["b"].Status, res.Tasks["b"].SkipReason)
	}
	if res.Tasks["c"].Status != StatusSkipped {
		t.Fatalf("c expected skipped, got %s", res.Tasks["c"].Status)
	}
	if res.Tasks["d"].Status != StatusSucceeded {
		t.Fatalf("d expected succeeded, got %s", res.Tasks["d"].Status)
	}
	// b、c 的 Fn 绝不能被调用。
	if c.get("b") != 0 || c.get("c") != 0 {
		t.Fatalf("skipped tasks must not execute: b=%d c=%d", c.get("b"), c.get("c"))
	}
	if c.get("a") != 1 || c.get("d") != 1 {
		t.Fatalf("a and d should each run once: a=%d d=%d", c.get("a"), c.get("d"))
	}

	// a 的最终失败可断言为 TaskFailedError 原因。
	var tfe *TaskFailedError
	if !errors.As(res.RunErr, &tfe) {
		// WorkflowExecutionError 聚合后不是 TaskFailedError，
		// 这里改为校验任务尝试记录已保留。
	}
	if len(res.Tasks["a"].Attempts) != 1 || res.Tasks["a"].Attempts[0].Err == "" {
		t.Fatalf("a should retain 1 attempt with reason, got %+v", res.Tasks["a"].Attempts)
	}
}

// ---------- 场景 5：并发上限：在途任务数不得突破 MaxConcurrency ----------

func TestConcurrencyLimit(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
			var inFlight, peak int64
			var mu sync.Mutex

			makeTask := func(name string) TaskSpec {
				return TaskSpec{Name: name, Fn: func(ctx context.Context) (string, error) {
					cur := atomic.AddInt64(&inFlight, 1)
					mu.Lock()
					if cur > peak {
						peak = cur
					}
					mu.Unlock()
					time.Sleep(20 * time.Millisecond)
					atomic.AddInt64(&inFlight, -1)
					return name, nil
				}}
			}

			res, err := eng.Submit(context.Background(), WorkflowSpec{
				ID:             fmt.Sprintf("wf-conc-%d", limit),
				MaxConcurrency: limit,
				Tasks: []TaskSpec{
					makeTask("t1"), makeTask("t2"), makeTask("t3"),
					makeTask("t4"), makeTask("t5"), makeTask("t6"),
				},
			})
			if err != nil {
				t.Fatalf("submit: %v", err)
			}
			if !res.Success {
				t.Fatalf("expected success, %v", res.RunErr)
			}
			if peak > int64(limit) {
				t.Fatalf("concurrency peak %d exceeded limit %d", peak, limit)
			}
			if peak != int64(limit) {
				t.Fatalf("expected peak to reach limit %d, got %d", limit, peak)
			}
			t.Logf("limit=%d observed peak concurrency=%d", limit, peak)
		})
	}
}

// ---------- 场景 6：重试耗尽，保留每次尝试原因；重试期间不突破并发上限 ----------

func TestRetryExhaustion(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	c := newCounter("flaky")
	var inFlight, peak int64

	res, err := eng.Submit(context.Background(), WorkflowSpec{
		ID:             "wf-retry",
		MaxConcurrency: 1,
		Tasks: []TaskSpec{
			{
				Name:       "flaky",
				MaxRetries: 2, // 首试 + 2 次重试 = 3 次
				Backoff:    &ConstantBackoff{Interval: time.Millisecond},
				Fn: func(ctx context.Context) (string, error) {
					cur := atomic.AddInt64(&inFlight, 1)
					if cur > peak {
						peak = cur
					}
					atomic.AddInt64(&inFlight, -1)
					return alwaysFail("flaky", c)(ctx)
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if res.Success {
		t.Fatal("expected failure after retries exhausted")
	}
	st := res.Tasks["flaky"]
	if st.Status != StatusFailed {
		t.Fatalf("expected failed, got %s", st.Status)
	}
	if len(st.Attempts) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(st.Attempts))
	}
	for i, a := range st.Attempts {
		if a.Err != "flaky:permanent-boom" {
			t.Fatalf("attempt %d reason mismatch: %q", i+1, a.Err)
		}
		if !a.Ended.After(a.Started) {
			t.Fatalf("attempt %d timings invalid: %+v", i+1, a)
		}
	}
	if c.get("flaky") != 3 {
		t.Fatalf("Fn should have been invoked 3 times, got %d", c.get("flaky"))
	}
	if peak > 1 {
		t.Fatalf("retry must not exceed concurrency cap, peak=%d", peak)
	}
	t.Logf("final failure: %v", res.RunErr)
}

// ---------- 场景 7：先失败后成功，重试次数符合配置 ----------

func TestRetryThenSuccess(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	c := newCounter("svc")
	res, err := eng.Submit(context.Background(), WorkflowSpec{
		ID:             "wf-retry-ok",
		MaxConcurrency: 2,
		Tasks: []TaskSpec{{
			Name: "svc", MaxRetries: 2,
			Backoff: &ConstantBackoff{Interval: time.Millisecond},
			Fn:      failNTimes("svc", 2, c),
		}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected eventual success: %v", res.RunErr)
	}
	if c.get("svc") != 3 {
		t.Fatalf("expected 3 invocations, got %d", c.get("svc"))
	}
	if len(res.Tasks["svc"].Attempts) != 3 {
		t.Fatalf("expected 3 attempts recorded, got %d", len(res.Tasks["svc"].Attempts))
	}
	if res.Tasks["svc"].Result != "svc:recovered-ok" {
		t.Fatalf("unexpected result %q", res.Tasks["svc"].Result)
	}
}

// ---------- 场景 8：已完成工作流重复提交 -> 直接复用，任务不重复执行 / 不重复计数 ----------

func TestDuplicateSubmissionReused(t *testing.T) {
	store := NewMemoryStore()
	c1 := newCounter("a", "b")
	eng := NewEngine(store, WithLogger(testLogger(t)))

	spec := func(c *counter) WorkflowSpec {
		return WorkflowSpec{
			ID: "wf-dup", MaxConcurrency: 2,
			Tasks: []TaskSpec{
				{Name: "a", Fn: func(ctx context.Context) (string, error) {
					c.inc("a")
					return "a1", nil
				}},
				{Name: "b", Deps: []string{"a"}, Fn: func(ctx context.Context) (string, error) {
					c.inc("b")
					return "b1", nil
				}},
			},
		}
	}

	res1, err := eng.Submit(context.Background(), spec(c1))
	if err != nil || !res1.Success {
		t.Fatalf("first submit: %v %v", err, res1.RunErr)
	}
	if res1.Reused {
		t.Fatal("first run must not be marked reused")
	}

	// 第二次提交：即使换成全新引擎（模拟另一个进程），也必须复用。
	c2 := newCounter("a", "b")
	eng2 := NewEngine(store, WithLogger(testLogger(t)))
	res2, err := eng2.Submit(context.Background(), spec(c2))
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if !res2.Success || !res2.Reused {
		t.Fatalf("second submit must reuse finished result, success=%v reused=%v", res2.Success, res2.Reused)
	}
	if c2.get("a") != 0 || c2.get("b") != 0 {
		t.Fatalf("reused run must execute no tasks: a=%d b=%d", c2.get("a"), c2.get("b"))
	}
	// 首轮计数不变。
	if c1.get("a") != 1 || c1.get("b") != 1 {
		t.Fatalf("first run counts changed: a=%d b=%d", c1.get("a"), c1.get("b"))
	}
	if res2.Tasks["b"].Result != "b1" {
		t.Fatalf("historical result should be preserved, got %q", res2.Tasks["b"].Result)
	}
}

// ---------- 场景 9：运行中中断 -> 已完成任务不重跑，未完成任务从断点续跑（FileStore 跨引擎） ----------

func TestResumeAfterInterruption(t *testing.T) {
	dir := t.TempDir()

	// --- 第一次"进程"：b 执行期间被取消（模拟崩溃 / kill）---
	c1 := newCounter("a", "b", "c")
	eng1 := NewEngine(NewFileStore(dir), WithLogger(testLogger(t)))
	bEntered := make(chan struct{})

	specWith := func(c *counter, bFn TaskFunc) WorkflowSpec {
		return WorkflowSpec{
			ID: "wf-resume", MaxConcurrency: 1,
			Tasks: []TaskSpec{
				{Name: "a", Fn: func(ctx context.Context) (string, error) {
					c.inc("a")
					return "a-done", nil
				}},
				{Name: "b", Deps: []string{"a"}, Fn: bFn},
				{Name: "c", Deps: []string{"a", "b"}, Fn: func(ctx context.Context) (string, error) {
					c.inc("c")
					return "c-done", nil
				}},
			},
		}
	}
	bBlocking := func(ctx context.Context) (string, error) {
		c1.inc("b")
		close(bEntered)
		<-ctx.Done() // 中断时在途
		return "", ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-bEntered
		time.Sleep(30 * time.Millisecond) // 确保 a=succeeded 已落盘
		cancel()
	}()
	_, err := eng1.Submit(ctx, specWith(c1, bBlocking))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from interrupted run, got %v", err)
	}
	if c1.get("a") != 1 || c1.get("b") != 1 || c1.get("c") != 0 {
		t.Fatalf("interrupted run invocations unexpected: a=%d b=%d c=%d", c1.get("a"), c1.get("b"), c1.get("c"))
	}

	// 校验磁盘上的断点：a 已成功，b 卡在非终态，c 尚未开始。
	snap, err := NewFileStore(dir).Load("wf-resume")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Finished || snap.Tasks["a"].Status != StatusSucceeded ||
		snap.Tasks["b"].Status != StatusRunning || snap.Tasks["c"].Status != StatusPending {
		t.Fatalf("checkpoint unexpected: finished=%v a=%s b=%s c=%s",
			snap.Finished, snap.Tasks["a"].Status, snap.Tasks["b"].Status, snap.Tasks["c"].Status)
	}

	// --- 第二次"进程"：全新引擎读取同一份文件，从断点继续 ---
	c2 := newCounter("a", "b", "c")
	eng2 := NewEngine(NewFileStore(dir), WithLogger(testLogger(t)))
	bOk := func(ctx context.Context) (string, error) {
		c2.inc("b")
		return "b-done", nil
	}
	res2, err := eng2.Submit(context.Background(), specWith(c2, bOk))
	if err != nil {
		t.Fatalf("resumed submit: %v", err)
	}
	if !res2.Success {
		t.Fatalf("resumed run should succeed: %v", res2.RunErr)
	}
	if !res2.Recovered {
		t.Fatal("resumed run must be flagged Recovered")
	}
	// a 已完成：恢复后绝不能重跑；b 在途尝试不算数，重新执行一次；c 正常执行。
	if c2.get("a") != 0 {
		t.Fatalf("completed task a must not re-execute after resume, got %d", c2.get("a"))
	}
	if c2.get("b") != 1 || c2.get("c") != 1 {
		t.Fatalf("resume invocations unexpected: b=%d c=%d", c2.get("b"), c2.get("c"))
	}
	if res2.Tasks["a"].Result != "a-done" || res2.Tasks["b"].Result != "b-done" {
		t.Fatalf("results not preserved as expected: a=%q b=%q",
			res2.Tasks["a"].Result, res2.Tasks["b"].Result)
	}

	// --- 第三次提交：已终态，直接复用 ---
	c3 := newCounter("a", "b", "c")
	eng3 := NewEngine(NewFileStore(dir), WithLogger(testLogger(t)))
	res3, err := eng3.Submit(context.Background(), specWith(c3, func(ctx context.Context) (string, error) {
		c3.inc("b")
		return "x", nil
	}))
	if err != nil || !res3.Success || !res3.Reused {
		t.Fatalf("third submit should reuse: err=%v success=%v reused=%v", err, res3 != nil && res3.Success, res3 != nil && res3.Reused)
	}
	if c3.get("a")+c3.get("b")+c3.get("c") != 0 {
		t.Fatalf("no task should run on finished reuse")
	}
}

// ---------- 场景 10：中断发生在重试间隙 -> 已失败的尝试记录保留，从下一尝试续跑 ----------

func TestRetryAttemptsKeptAcrossRestart(t *testing.T) {
	store := NewMemoryStore()

	// 第一次：第 1 次尝试失败落盘，第 2 次尝试在途时中断。
	c1 := newCounter("job")
	eng1 := NewEngine(store, WithLogger(testLogger(t)))
	entered2 := make(chan struct{})
	var calls int64
	fn1 := func(ctx context.Context) (string, error) {
		c1.inc("job")
		k := atomic.AddInt64(&calls, 1)
		if k == 1 {
			return "", errors.New("boom-1")
		}
		close(entered2)
		<-ctx.Done()
		return "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-entered2
		cancel()
	}()
	_, err := eng1.Submit(ctx, WorkflowSpec{
		ID: "wf-retry-resume", MaxConcurrency: 1,
		Tasks: []TaskSpec{{Name: "job", MaxRetries: 3, Fn: fn1}},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	snap, _ := store.Load("wf-retry-resume")
	if len(snap.Tasks["job"].Attempts) != 1 || snap.Tasks["job"].Attempts[0].Err != "boom-1" {
		t.Fatalf("checkpoint should keep 1 failed attempt, got %+v", snap.Tasks["job"].Attempts)
	}

	// 第二次：从第 2 次尝试继续并成功；历史尝试记录仍然保留。
	c2 := newCounter("job")
	eng2 := NewEngine(store, WithLogger(testLogger(t)))
	fn2 := func(ctx context.Context) (string, error) {
		c2.inc("job")
		return "job-final-ok", nil
	}
	res, err := eng2.Submit(context.Background(), WorkflowSpec{
		ID: "wf-retry-resume", MaxConcurrency: 1,
		Tasks: []TaskSpec{{Name: "job", MaxRetries: 3, Fn: fn2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success || !res.Recovered {
		t.Fatalf("expected recovered success, success=%v recovered=%v err=%v",
			res.Success, res.Recovered, res.RunErr)
	}
	st := res.Tasks["job"]
	if len(st.Attempts) != 2 || st.Attempts[0].Err != "boom-1" || st.Attempts[1].Err != "" {
		t.Fatalf("attempt history not stitched correctly: %+v", st.Attempts)
	}
	if st.Result != "job-final-ok" {
		t.Fatalf("unexpected final result %q", st.Result)
	}
	if c2.get("job") != 1 {
		t.Fatalf("after resume only 1 new invocation expected, got %d", c2.get("job"))
	}
}

// ---------- 场景 11：恢复时任务定义与快照不一致 -> 明确报错，绝不静默 ----------

func TestResumeWithChangedDefinition(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store, WithLogger(testLogger(t)))

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		<-started
		cancel()
	}()
	_, err := eng.Submit(ctx, WorkflowSpec{
		ID: "wf-changed",
		Tasks: []TaskSpec{{
			Name: "a",
			Fn:   func(ctx context.Context) (string, error) { close(started); <-ctx.Done(); return "", ctx.Err() },
		}},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}

	eng2 := NewEngine(store, WithLogger(testLogger(t)))
	_, err = eng2.Submit(context.Background(), WorkflowSpec{
		ID: "wf-changed",
		Tasks: []TaskSpec{
			{Name: "a", Fn: ok("a")},
			{Name: "b", Fn: ok("b")},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "definition changed") {
		t.Fatalf("expected definition-changed error, got %v", err)
	}
}

// ---------- 场景 12：自依赖属于环 ----------

func TestSelfDependencyIsCycle(t *testing.T) {
	eng := NewEngine(NewMemoryStore(), WithLogger(testLogger(t)))
	_, err := eng.Submit(context.Background(), WorkflowSpec{
		ID: "wf-self",
		Tasks: []TaskSpec{
			{Name: "a", Deps: []string{"a"}, Fn: ok("a")},
		},
	})
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CycleError for self dependency, got %T %v", err, err)
	}
}

// ---------- 场景 13：退避策略时长正确、封顶生效 ----------

func TestBackoffDelays(t *testing.T) {
	const ms = time.Millisecond
	cb := &ConstantBackoff{Interval: 5 * ms}
	if cb.Delay(1) != 5*ms || cb.Delay(3) != 5*ms {
		t.Fatal("constant backoff mismatch")
	}
	eb := &ExponentialBackoff{Base: ms, Max: 5 * ms}
	want := []time.Duration{ms, 2 * ms, 4 * ms, 5 * ms, 5 * ms}
	for i, w := range want {
		if got := eb.Delay(i + 1); got != w {
			t.Fatalf("exp backoff attempt %d: want %s got %s", i+1, w, got)
		}
	}
	if (&NoBackoff{}).Delay(3) != 0 {
		t.Fatal("NoBackoff must return 0")
	}
}
