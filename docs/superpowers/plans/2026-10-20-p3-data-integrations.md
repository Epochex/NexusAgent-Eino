# P3 Data and Integrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans, native execution.

**Goal:** 文档索引、检索、长期记忆、MCP/A2A 在真实依赖下完成可重试、隔离和重启后的产品闭环。

**Architecture:** 延续 P2 事务任务/outbox；版本化 chunk/embedding 与 ready 可见性，PG 权威回查；MCP registry 启动重建，远端网络统一边界；已有页面实测。

**Tech Stack:** 已锁定 Go/Eino、PostgreSQL、Redis、Milvus、MinIO、HTTP/SSE、React。

**Spec:** `docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md`，连续实施已授权。

## Global Constraints

- 不将 ILIKE/TF-IDF 称为 BM25；至少 100 个带相关 chunk、无答案和权限反例的固定查询。
- 文档/embedding 版本不可静默混用；重投稳定 chunk ID；仅完整 ready 版本可见。
- 向量召回前按用户/Agent/知识库隔离，回表再次校验；记忆操作使用稳定 ID。
- 解析失败/降级必须可见；网络调用、输出和资源有界；MCP/A2A 声明匹配实际行为。

## Review Focus

- 多页同 seq、递归不前进、超大 DOCX 解压、PDF 单页失败不得静默丢内容。
- embed/upsert 中断与配置切换时重投不能产生重复/混维度/半成品检索。
- 他人/他 Agent 的向量不得挤掉合法召回或影响记忆合并。
- MCP 同步失败/重启/并发握手、SSRF redirect/DNS 重绑定与响应超限。
- A2A 部分输出错误、能力声明、越权 task 查询和取消。

### Task 1: Real document ingestion

**Files:** splitter/loader tests + implementations, `repo/rag.go`, model/rag, additive migration, worker/doc task handling, KB upload/retry services.

**Interfaces:** preserve Load/IndexDocument/Retrieve signatures; extend LoadedDoc warnings; stable `(document,version,global_seq)` chunk identity; outbox enqueue and ready-generation publication transactional.

- [ ] Write regressions for long delimiter-containing text termination, UTF8/overlap/size, HTML removal, DOCX paragraphs/entities/limits, PDF warning visibility, multi-page global seq, partial embedding retry idempotency and ready visibility.
- [ ] Run RED; implement progress-guaranteed splitter, bounded format parsing and explicit warnings, document-version job/outbox/lease and deterministic chunks, pinned embedding identity/dimension, error-checked state updates.
- [ ] Run real PG/Redis/MinIO/Milvus ingestion -> forced failure -> retry -> retrieval tests; commit.

### Task 2: Retrieval and memory correctness

**Files:** engine/fusion, vector Store/Milvus implementation, repo/rag, long_term manager, chatsvc preparation, migration; fixed 100-query JSONL evaluator and tests.

**Interfaces:** add prefilter-capable vector search; Engine/Manager propagate scoped context, deadlines and status metadata; memory vector tasks carry stable memory IDs and revision.

- [ ] Test foreign KB/Agent pollution, not-ready versions, bad embedding counts/dimensions, metadata/row mismatch, sparse/rerank degradation, reordered ADD/UPDATE/DELETE and vector failure replay.
- [ ] Observe RED; implement scoped prefilter+authoritative recheck, explicit degradation, pinned model versions, bounded preparation, stable memory operation IDs and persistent reconciliation.
- [ ] Run real infrastructure evaluation/ablation/isolation tests; record actual Recall@k/order/citations and model-answer evidence separately. Commit.

### Task 3: MCP and A2A completion

**Files:** mcp/client, service/mcpsvc, registry, repo/ops, a2a client/server/types, security outbound client, main startup; protocol and restart tests.

**Interfaces:** MCP RestoreRegistry(ctx) rebuilds persisted active tools; HTTP client enforces target policy at dial/redirect, bounded timeout/response; protocol Initialize/initialized/session/IDs/pagination and JSON/SSE decoding validated; A2A advertises only implemented features.

- [ ] Test restart -> actual Agent tool call, failed sync retains old usable definitions, removed tools revoked, concurrent requests, protocol errors/limits, redirect/private/DNS targets and task owner isolation.
- [ ] Observe RED; implement transactional stable tool sync, dynamic authoritative server checks and credential redaction, protocol-correct client and truthful A2A surface.
- [ ] Run local standards-compatible HTTP peers and actual UI flow, race and full matrix; commit.

### Task 4: Complete product validation

**Files:** reproducible isolated Compose/env scripts, actual product E2E tests, README and P3 validation report; no model secrets in tracked files.

- [ ] Deploy real PG/Redis/MinIO/Milvus and server+worker, migrate/seed, register/login two users; run Agent/tool/Host, workflow approvals/restart, upload/retry/retrieval/citations, memory and MCP/A2A flows through existing HTTP/UI.
- [ ] Fix real failures and rerun affected flows; run optional real model using user-configured endpoint, report absent configuration precisely rather than call it passed. Commit and continue P4.
