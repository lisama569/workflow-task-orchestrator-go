# 本地工作流编排引擎（Go）

一个纯本地、无外部依赖的工作流任务编排引擎。任务与依赖关系保存在内存 / JSON 文件中，
用于模拟真实调度，并让**并发执行、失败重试、重启恢复**的结果可预期、可复现。

- 模块路径：`github.com/lisama569/workflow-task-orchestrator-go`
- 代码包：`workflow`
- Go 版本：1.25

## 快速开始

```go
store := workflow.NewFileStore("./.workflow-state") // 或 workflow.NewMemoryStore()
eng := workflow.NewEngine(store, workflow.ExecutorRegistry{
    "download": func(ctx context.Context) error { /* ... */ return nil },
    "convert":  func(ctx context.Context) error { /* ... */ return nil },
},
    workflow.WithBackoff(workflow.ExponentialBackoff{Base: 10 * time.Millisecond, Max: time.Second}),
    workflow.WithLogger(func(format string, args ...any) { log.Printf(format, args...) }),
)

spec := &workflow.WorkflowSpec{
    WorkflowID:     "demo-1",
    MaxConcurrency: 4,
    Tasks: []workflow.TaskSpec{
        {Name: "download", MaxRetries: 2},
        {Name: "convert", Dependencies: []string{"download"}, MaxRetries: 1, Timeout: 5 * time.Second},
    },
}
state, err := eng.Run(context.Background(), spec)
```

任务的业务逻辑通过 `ExecutorRegistry` 按任务名注入（执行函数不参与持久化，
“重启”后需要重新注册）。

## 任务状态机

```
PENDING ──依赖全部有定论──► READY ──抢到并发槽──► RUNNING ──成功──► SUCCEEDED
                              ▲                     │
                              │                失败且仍有重试预算
                              │                     ▼
                              └──恢复/中断◄── RETRYING（退避中，不占并发槽）
                                    │
  依赖 FAILED / SKIPPED            │
PENDING ───────────────────────► SKIPPED
RUNNING 重试耗尽 ───────────────► FAILED
```

- `SUCCEEDED` / `FAILED` / `SKIPPED` 为终态。
- `RETRYING` 表示“尝试失败、退避等待下一次尝试”，任务仍由同一个执行 goroutine 接管，
  调度器不会重复调度它。

## 依赖规则

- 任务在所有依赖 **SUCCEEDED** 后才会开始执行。
- 提交时静态校验，三类错误彼此可区分（可用 `errors.As` 判定）：
  - `*CycleError`：依赖图存在环，错误中给出环路径，如 `a -> c -> b -> a`；
  - `*MissingDependencyError`：依赖了工作流中不存在的任务，错误中带任务名与缺失依赖名；
  - `*DuplicateTaskError`：任务重名。
- 运行期某依赖 **FAILED** 或 **SKIPPED**：
  - 下游未开始的任务级联标记为 **SKIPPED**（绝不静默执行，也绝不计为成功），
    并在 `TaskState.FailedDeps` 中记录导致跳过的直接依赖；
  - 工作流返回的错误可被 `errors.As` 为 `*DependencyFailedError`。

## 重试规则

- 每个任务独立配置 `MaxRetries`（不含首次尝试）：总尝试次数 = `MaxRetries + 1`。
- 每次尝试无论成败都记录在 `TaskState.Attempts` 中：
  尝试序号、开始/结束时间、失败原因（成功原因为空字符串）。
- 退避策略可配置，实现 `BackoffPolicy` 即可，内置：
  - `NoBackoff`（默认，立即重试）
  - `ConstantBackoff{Delay}`
  - `ExponentialBackoff{Base, Max}`：`Base * 2^(n-1)`，封顶 `Max`
- 重试耗尽后任务标记为 **FAILED**，`FailKind = RETRIES_EXHAUSTED`，
  返回错误可 `errors.As` 为 `*RetriesExhaustedError`，其中含总尝试次数与最后一次原因；
  每一次尝试的原因仍完整保留在 `Attempts` 中。
- executor panic 会被恢复并记为该次尝试失败，不会击穿引擎。
- 单次尝试可用 `TaskSpec.Timeout` 限时，超时以 `context.DeadlineExceeded` 计入尝试失败。

## 并发语义

- `WorkflowSpec.MaxConcurrency` 限制**同一时刻正在执行尝试**的任务数（信号量实现）。
- 任务**退避等待期间释放并发槽**；重试开始前必须重新竞争槽位，
  因此**重试期间也不会突破并发上限**。
- 不同任务天然并发执行；同一任务的多次尝试严格串行。
- `MaxConcurrency <= 0` 表示不限并发。

## 恢复边界（重启 / 中断 / 重复提交）

每次状态翻转与每次尝试记账都会立即落盘（`FileStore` 采用“临时文件 + 原子 rename”）。

| 中断时任务状态 | 恢复行为 |
| --- | --- |
| `SUCCEEDED` | 保持成功，**绝不重复执行、不重复计数** |
| `FAILED` / `SKIPPED` | 终态封板，结论与错误类型保持可复现 |
| `RUNNING`（尝试结果未知） | 回滚为 `READY`，**丢弃这次未知尝试的计数**（`Attempts` 中本就没有该记录），恢复后重跑该次 |
| `RETRYING`（退避中） | 回滚为 `READY`，**已记账的失败尝试全部保留**，恢复后从“下一次尝试”继续，重试预算不重置 |
| `PENDING` | 原样等待依赖 |

- 用同一个 `WorkflowID` 重新提交即表示“继续跑”：已完成的任务不重复执行，
  未完成的任务从上述断点接着跑。
- 工作流已到终态（`SUCCEEDED` / `FAILED`）后重复提交：不执行任何任务，
  直接返回持久化的既有状态与同样可区分的错误。
- 恢复时要求提交的任务集合与持久化状态一致，否则返回明确的不可恢复错误
  （防止用同 ID 提交不同定义后产生歧义）。
- 边界说明：`RUNNING` 中断时业务函数是否产生了外部副作用无从得知，
  引擎只承诺“这次尝试不记账、会被重新执行”；业务函数需要自行保证幂等。

## 错误如何区分

`Run` 返回的工作流级错误用 `errors.Join` 汇总所有终态失败任务，配合 `errors.As`：

```go
var ce *workflow.CycleError
var me *workflow.MissingDependencyError
var re *workflow.RetriesExhaustedError
var de *workflow.DependencyFailedError
switch {
case errors.As(err, &ce): // 环
case errors.As(err, &me): // 缺失依赖
case errors.As(err, &re): // 重试耗尽
case errors.As(err, &de): // 因依赖失败被跳过
}
```

此外每个任务的持久化状态带 `FailKind`（`RETRIES_EXHAUSTED` / `DEPENDENCY_FAILED` /
`NO_EXECUTOR`），即使跨重启只拿到状态对象，也能区分失败原因。

## 本地验证

```bash
# 全部单测（-v 可查看任务状态流转与每次尝试结果的日志）
go test -v -count=1 ./...

# 竞态检测
go test -race -count=1 ./...

go vet ./...
```

测试覆盖场景：

| 测试 | 场景 |
| --- | --- |
| `TestHappyPathDependencyOrder` | 依赖顺序调度 |
| `TestCycleDetected` | 环检测，断言 `*CycleError` 与环路径 |
| `TestMissingDependencyDetected` | 缺失依赖，断言 `*MissingDependencyError` |
| `TestDependencyFailurePropagates` | 依赖失败级联 SKIPPED，两类错误可区分 |
| `TestConcurrencyLimitRespected` | 实时并发峰值不超过上限 |
| `TestRetryDoesNotExceedConcurrency` | 含重试的全过程并发不越界 |
| `TestRetriesExhausted` | 重试耗尽 FAILED，保留每次尝试原因 |
| `TestRetryEventuallySucceeds` | 前若干次失败后成功，指数退避生效 |
| `TestRestartResumeKeepsAttemptCount` | 运行中中断 + FileStore 重启恢复（RUNNING 断点） |
| `TestRecoverWhileRetrying` | 退避中中断 + 重启恢复（RETRYING 断点，预算保留） |
| `TestResubmitTerminalWorkflowIdempotent` | 终态工作流重复提交零重复执行、错误可复现 |

状态文件示例：`<state-dir>/<workflowID>.json`。
