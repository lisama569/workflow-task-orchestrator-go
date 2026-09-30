# 本地工作流编排引擎（Go）

一个纯本地、无外部依赖的工作流任务编排引擎。任务与依赖在内存中描述，
调度过程真实并发；每次状态流转与每次尝试结果都持久化到快照
（内存或 JSON 文件），因此失败重试、重复提交与重启恢复的结果都
**可预期、可复现**。

## 快速开始

```go
store := workflow.NewFileStore("./.snapshots") // 或 NewMemoryStore()
eng := workflow.NewEngine(store)

res, err := eng.Submit(ctx, workflow.WorkflowSpec{
    ID:             "order-pipeline",
    MaxConcurrency: 4, // 整个工作流同时执行的任务上限
    Tasks: []workflow.TaskSpec{
        {Name: "stock", Fn: deductStock},
        {Name: "pay", Deps: []string{"stock"},
            MaxRetries: 2,
            Backoff:    &workflow.ExponentialBackoff{Base: 10 * time.Millisecond, Max: time.Second},
            Fn:         charge},
        {Name: "ship", Deps: []string{"pay"}, Fn: shipOrder},
    },
})
```

## 1. 依赖调度规则

- 每个任务声明零到多个 `Deps`；任务只有在**所有依赖都成功**后才会开始执行。
- 提交时先做静态校验，两类问题给出**可区分、不静默**的错误（`errors.As` 断言）：
  - `*CycleError`：依赖图存在环（含自依赖），错误中带环路径，任何任务都不会执行。
  - `*MissingDependencyError`：依赖了工作流中不存在的任务。
- 运行期依赖结论：
  - 某个依赖 `failed`（重试耗尽）或 `skipped` 时，依赖它的任务进入
    `skipped` 终态，`SkipReason` 明确写出"因为哪个依赖失败/被跳过"；
    **绝不会静默跳过，也不会被当成成功**。跳过会沿依赖链传播。
  - 与依赖无关的独立任务不受影响，继续正常执行。
- 工作流跑完后若存在失败/跳过，`WorkflowResult.RunErr` 为
  `*WorkflowExecutionError`，其中 `Failed`（重试耗尽列表）与
  `Skipped`（任务→跳过原因）分别聚合，可与提交级错误区分。

## 2. 重试规则

- `MaxRetries` 是**首次执行之外的额外重试次数**；总尝试次数 = `1 + MaxRetries`。
- `Backoff` 决定第 N 次尝试失败后的等待：
  - `NoBackoff`（默认）：立即重试；
  - `ConstantBackoff{Interval}`：固定间隔；
  - `ExponentialBackoff{Base, Max}`：`Base * 2^(N-1)`，以 `Max` 封顶。
- 每次尝试都记录 `Attempt{Index, Started, Ended, Err}`，全部保留在
  `TaskState.Attempts` 中；成功时记录 `Result`。
- 总尝试次数用尽仍失败 => 任务进入 `failed` 终态，`TaskFailedError`
  文本中带每一次尝试的原因；该结论不可通过再次提交推翻（见恢复规则）。

## 3. 并发语义

- `MaxConcurrency`（缺省 = 任务总数）限制整个工作流**同时处于执行状态的任务数**，
  通过容量为该值的信号量实现；未获得名额的 ready 任务等待。
- **重试与退避期间不释放名额**：一个任务从首次尝试到成功/最终失败的全过程
  （含退避 sleep）都占着一个名额，因此重试绝不会突破并发上限。
- 状态与快照的所有读写由互斥锁保护，可安全地以 `-race` 运行。

## 4. 恢复边界（重启 / 中断 / 重复提交）

每次任务状态流转（ready/running/retrying/succeeded/failed/skipped）都立即
把整个工作流快照原子写入 `Store`（`FileStore` 使用"临时文件 + rename"）。

以同一 `WorkflowID` 再次 `Submit` 时：

1. **快照已是终态（finished）**：直接复用历史结论，`WorkflowResult.Reused = true`，
   **不执行、不计数任何任务**（幂等去重）。
2. **快照存在但未完成（中断恢复）**：`Recovered = true`，
   - `succeeded / failed / skipped` 任务：**完整保留、绝不重跑、不重复计数**，
     其结果、每次尝试记录、跳过原因原样可用，并直接放行其下游；
   - `pending / ready / running / retrying` 任务：回到 `pending` 重新调度，
     **已经落盘的失败尝试记录保留并计入总尝试次数**（重试预算不会因重启变多）。
3. **中断时正在执行的那一次尝试（in-flight）**：其结果视为不可信，
   **不记录、不落任何终态**——因为真实进程被 kill 时该尝试天然没有机会提交结论。
   快照停在最近一次可信流转，重启后从该断点重跑该任务。
   测试通过 `context.Cancel` 模拟这一过程（`Submit` 返回 `context.Canceled`）。
4. 恢复时若发现任务集合与历史快照不一致，直接返回
   `definition changed` 错误，不做猜测性执行。

任务状态机：

```
pending ──依赖未满足──> (等待)
pending ──依赖全部成功──> ready ──获得名额──> running
running ──成功──────────────────────────────> succeeded
running ──失败且仍有额度──> retraying ──退避结束──> running
running ──失败且额度耗尽──> failed
等待中 ──依赖 failed/skipped──> skipped（带明确原因）
```

## 5. 本地验证

```bash
# 全量测试（含依赖失败、并发上限、重试耗尽、重启恢复等场景）
go test ./... -v

# 竞态检测 + 多次重复（验证并发安全与恢复的可复现性）
go test ./... -race -count=10
```

测试覆盖的场景（`workflow/engine_test.go`，日志中会打印状态流转与每次尝试结果）：

| 测试 | 覆盖点 |
| --- | --- |
| `TestBasicDAGSuccess` | 拓扑顺序、各执行一次、结果保留 |
| `TestCycleDetected` / `TestSelfDependencyIsCycle` | 成环（含自环）可区分错误，任务零执行 |
| `TestMissingDependency` | 缺失依赖可区分错误 |
| `TestDependencyFailurePropagates` | 依赖最终失败 → 下游显式 skipped 并传播，独立任务不受影响，错误聚合可区分 |
| `TestConcurrencyLimit` | 并发峰值恰好达到且绝不超过 1/2/3 上限 |
| `TestRetryExhaustion` | 3 次尝试后最终失败、每次原因保留、重试期间不突破上限 |
| `TestRetryThenSuccess` | 重试后成功、尝试记录与结果 |
| `TestBackoffDelays` | 固定/指数退避时长与封顶 |
| `TestDuplicateSubmissionReused` | 已完成工作流重复提交：零执行、结论复用 |
| `TestResumeAfterInterruption` | 运行中中断 → 新引擎读文件快照续跑：已完成任务不重跑、在途尝试作废、续跑成功、再次提交复用 |
| `TestRetryAttemptsKeptAcrossRestart` | 中断在重试间隙：历史失败尝试保留，从下一尝试续跑，总预算不重置 |
| `TestResumeWithChangedDefinition` | 恢复时定义不一致 → 明确报错 |

`FileStore` 的快照文件可直接打开查看，例如中断后可以看到
`finished=false`、`a=succeeded`、`b=running`、`c=pending` 这样的可解释断点。
