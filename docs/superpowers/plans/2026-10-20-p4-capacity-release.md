# P4 Capacity and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans, native execution; one fresh whole-branch reviewer at completion.

**Goal:** 给出实际多 worker 额度、故障边界、完整功能验收和可复现容量结果，达到或如实记录设计目标差距。

**Architecture:** PostgreSQL 共享租约额度和公平领取，等待不占槽；开放到达率真实 server/worker/DB/SSE 负载，依据 profile 修热点；固定依赖、迁移与回滚演练。

**Tech Stack:** 已有 Go/Eino/PG/Redis/React 栈，独立负载与故障工具。

**Spec:** `docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md`，连续实施已授权。

## Global Constraints

- server+worker 合计 4 vCPU/8 GiB；外置 PG/Redis/Milvus/MinIO 单列资源。
- 模型桩 250ms/工具 100ms；短运行 2 次模型+1次工具。真实 LLM 正确率/费用另列。
- 50/200/500 SSE、8/32/64 slots、1/5/10/20 new runs/s；基准档 200/32/10 持续30分钟。
- 控制接口 p95<150ms、p99<500ms；有效负载错误率<0.1%；不越权、不丢终态、不盲重放写操作。
- 排空5分钟、重复三个周期；报告 offered/completed/rejected/timeout/canceled/needs_attention、排队/执行/首token、CPU/heap/GC/连接/积压。

## Review Focus

- 多进程各自限额导致整体超卖，单用户饥饿与租约过期后旧执行者继续工作。
- 等待审批/退避仍占槽，数据库/Redis故障被成功 ACK 掩盖。
- 开放负载实际 offered 不足却报告达到目标，或漏掉积压/取消/unknown。
- 慢订阅与过期 cursor 导致连接/内存增长或终态丢失。
- 新版本恢复旧 checkpoint、迁移回滚破坏运行中实例与凭证泄漏。

### Task 1: Shared admission and bounded queues

**Files:** execution quota/worker/store tests and implementation; gateway provider limits; config, metrics and migration.

**Interfaces:** shared lease Acquire/Renew/Release scoped global/owner/provider; atomic claim combines ready work and capacity, bounded queue and expiry; control APIs remain responsive.

- [x] Write real multi-process PG tests exceeding global/user/provider limits, expired owner, approval wait and queue overload; RED.
- [x] Implement transactional shared quotas and fair ready claims, explicit overload/backoff/deadline; bounded version graph cache only if profile supports it.
- [x] Run competing worker processes and resource recovery tests; GREEN and commit (`3cfb6d3`; full Linux gate passed).

### Task 2: Open-load and failure experiments

**Files:** `backend/cmd/loadtest/`, isolated infrastructure/peer scripts and machine-readable results; P4 report.

- [ ] Validate load generator records scheduled vs actually offered work and all outcomes against independent DB totals, not only successful responses.
- [ ] Run exact staged load and 30-minute target, cancellation/approval/restart/slow-SSE scenarios, profile real bottlenecks, TDD any fixes.
- [ ] Repeat three drain/recovery cycles and measure resource trends. Publish actual result and target gaps; do not fabricate or reduce the required workload silently.

### Task 3: Review release and push

**Files:** CI, deployment/rollback runbook, README, acceptance matrix and reports.

- [ ] Full Go vet/race/unit/real infrastructure/process E2E, frontend build/browser, fixed knowledge queries and optional real-model results; list every failed/unavailable check.
- [ ] One fresh independent whole-branch reviewer per native skill; fix Critical/Important with RED/GREEN, rerun appropriate full checks; record rulings.
- [ ] Exercise additive migration and rollback compatibility; push tested main to authorized repository without repeat confirmation. Confirm actual CI and remote SHA. Preserve local validation scratch where cleanup remains policy-blocked.
