# P2 Durable Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans, task-by-task, native execution. Steps use checkbox syntax.

**Goal:** 已有工作流页面真正获得不可变版本、精确审批、重启恢复和可查询结果，不再依赖 HTTP 存活。

**Architecture:** PostgreSQL 保存冻结定义、运行、步骤尝试、审批、Eino checkpoint、事件和事务 outbox。Eino Graph 执行 DSL；worker 用带 generation 的租约领取，Redis 只通知，PG 扫描兜底。现有 workflow API/页面保持兼容并增加审批标识、取消和事件 cursor。

**Tech Stack:** Go 1.26.0, Eino v0.9.22, Hertz, GORM/PostgreSQL 16, Redis 7, React/TypeScript.

**Spec:** `docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md`，已批准。用户本轮明确要求一次性推进，沿用原生执行，不重复阶段审批。

## Global Constraints

- 保留平台所有主线功能与上游归属，专用 checkout/feature branch，不修改简历仓库。
- PostgreSQL 为权威；状态、事件、审批决定和 outbox 同事务。过期执行者不得写入。
- Run 冻结 DSL/子图与依赖配置，保留 Eino/图/schema 版本；不让编辑影响既有运行。
- 等待审批释放执行槽；并行/连续/子图审批用精确 interrupt ID 恢复。
- 外部写操作默认不可盲重放；稳定 operation ID，结果未知需核对或人工处理。
- 浏览器断开不取消持久工作流。完成后才发可靠终态；慢订阅按 cursor 重连。
- 本轮不再尝试被执行工具拒绝的本地删除清理；保留验证资料。

## Review Focus

- 两个 worker 同时领取与旧 generation 迟到提交：只承认一个合法执行者。
- 编辑/删除定义、并发审批与重复决定：旧运行仍按冻结版本，只恢复指定中断。
- kill 发生在工具返回但账本提交前：不重复执行未知写操作。
- 并行或嵌套节点等待审批：不能吞掉等待状态、重复执行兄弟节点或占槽等待。
- Redis 丢通知、数据库暂断、SSE 停读与 cursor 过期：状态可找回，错误明确且资源有界。

### Task 1: PostgreSQL execution authority

**Files:** create `backend/migrations/004_durable_execution.{up,down}.sql`, `backend/internal/execution/{types.go,store.go,checkpoint.go,store_integration_test.go}`.

**Interfaces:** `NewStore(*gorm.DB)*Store`; `Store.Enqueue(context.Context,int64,int64,json.RawMessage,json.RawMessage,time.Time)(*Run,error)`; `Claim(ctx,worker,lease)(*Run,error)`; `Renew(ctx,*Run,lease)error`; `Finish(ctx,*Run,status,output,error)error`; `Get(ctx,ownerID,id)(*Run,error)`; `Decide(ctx,ownerID,runID,interruptID,hash,approved)error`; `CheckpointStore` implements `compose.CheckPointStore` with fenced writes. Enqueue's explicit deadline bounds the whole run, including approval waiting; default is chosen by service.

- [ ] Write `TestExecutionClaimFencing`, `TestExecutionApprovalCAS`, `TestExecutionTransactionEvents`, `TestCheckpointVersionAndLease` against isolated real PG schemas; assert old leases cannot renew/finish/checkpoint, foreign owner cannot query/approve, one decision/outbox survives concurrent duplicates, terminal cannot regress.
- [ ] Run `go test -tags=integration ./internal/execution -count=1 -timeout 90s`; expected RED missing types/behavior.
- [ ] Implement additive migration, SQL row locks/SKIP LOCKED, generation CAS, bounded data, deadline, snapshot and owner checks, ordered events/outbox; no AutoMigrate in application startup.
- [ ] Repeat real PG tests and race; expected GREEN. Commit `feat: persist fenced workflow runs checkpoints and approvals`.

### Task 2: Eino DSL compiler and durable operations

**Files:** modify `backend/internal/workflow/{dsl.go,engine.go}`; create `backend/internal/workflow/{graph.go,graph_test.go}` and `backend/internal/execution/{operations.go,driver.go,driver_integration_test.go}`; modify tool boundary only to accept a server-created approval capability bound to tool/argument hash/operation.

**Interfaces:** `workflow.Compile(ctx,*DSL,Deps,compose.CheckPointStore)(compose.Runnable[map[string]any,map[string]any],error)`; `Driver.Execute(ctx,*Run)error`; stable node execution path identifies operation; pending approvals carry exact interrupt IDs and hashes.

- [ ] Write `TestGraphSequentialApprovals`, `TestGraphParallelApprovals`, `TestGraphNestedApproval`, `TestGraphConditionsAndValidation`, `TestOperationUnknownNotReplayed`; assert saved graph resumes only approved paths, completed siblings not rerun, malformed/dangling/cyclic/unbounded definitions fail, schema/authority stays enforced.
- [ ] Run graph/driver tests, observe RED on old first-human recovery or absent implementation.
- [ ] Compile actual Eino nodes/branches/subgraphs with state/checkpoints; frozen definitions and dependencies, bounded 32-way fanout/4 active node calls, node/run deadlines. Persist attempts/results, reuse committed results; unknown non-idempotent writes enter needs_attention. Refuse unsafe legacy workflow tool invocation.
- [ ] Run graph tests, real PG interruption/lease-expiry/operation fault regressions and race. Commit `feat: compile resumable workflows through Eino Graph`.

### Task 3: Connect worker API and existing frontend

**Files:** modify `backend/internal/service/workflowsvc.go`, `backend/internal/model/workflow.go`, `backend/internal/repo/ops.go`, `backend/internal/api/{handler_workflow.go,router.go}`, `backend/cmd/{server,worker}/main.go`, frontend workflow API/types/pages; create `backend/internal/execution/{worker.go,outbox.go,events.go}` and protocol/integration tests.

**Interfaces:** existing Run returns accepted persisted run immediately; Get/List expose current durable status/steps/pending approvals; Approve accepts optional interrupt ID/hash (legacy omitted selection only for exactly one pending approval); `POST .../cancel`, `GET .../events?after=`; `Worker.Run(ctx)error` drains/renews bounded claims, PG polling independent of Redis health.

- [ ] Write real HTTP/PG tests: submit -> worker -> two approvals -> success; restart -> resume; foreign owner denied; definition edit not applied; concurrent approval idempotent; disconnect subscription leaves run alive; cancellation terminal; reliable event cursor and stale cursor error.
- [ ] Observe RED, then replace synchronous legacy production drive with execution service; retain API fields, real frontend polling and exact approval controls, explicit needs_attention UI; persist notification transactionally, publish/ACK only after confirmed success.
- [ ] Run full Go/race/PG, frontend typecheck/build and actual browser/HTTP product walkthrough; expected GREEN. Commit `feat: connect durable workflow worker and product controls`.

### Task 4: Process fault recovery and delivery evidence

**Files:** create `backend/internal/execution/process_integration_test.go`, deployment runbook and `docs/research/2026-10-20-p2-validation.md`; update CI and README.

- [ ] Start a real worker process, kill during safe work and after external write before commit; verify lease recovery and unknown-operation stop; Redis unavailable still drains PG; repeated notifications do not duplicate committed work.
- [ ] Fix observed failures with RED/GREEN tests; run full matrix with fixed sources, record actual commands/results/remaining gaps, commit. Continue P3 without another permission gate.
