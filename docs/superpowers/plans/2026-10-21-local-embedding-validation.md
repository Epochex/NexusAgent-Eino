# Local embedding validation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. The user authorized native execution and pushing the result.

**Goal:** Run a real local Qwen3 embedding model on the user's RTX 5060, evaluate actual retrieval and RAG, and publish an evidence-backed first-person account of the entire project.

**Architecture:** A loopback-only, bounded Python acceptance service exposes the existing OpenAI-compatible embedding contract. The Go gateway, PG publication authority and real Milvus retrieval remain the application path. Model artifacts, environments, downloaded datasets and credentials stay outside Git; reports pin revisions and hashes.

**Tech Stack:** Go 1.26 / existing Hertz+Eino+PG+Milvus; Qwen3-Embedding-0.6B, PyTorch CUDA 12.8, Transformers, FastAPI.

**Spec:** Existing `docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md`; bounded follow-up approved in chat: local 0.6B model, small batches/512-token bound, existing DS generation, whole-development narrative pushed to the repo.

## Global constraints

- Keep the existing Go production interfaces; no training or large-model deployment subsystem.
- Pin model revision, dependencies, dataset source and sample selection; report actual GPU/CPU and memory peaks.
- No new external model account. Embeddings incur zero API charges. Any DS calls share the existing extended-round ledger and 4.8 CNY conservative ceiling, below the user's 5 CNY authorization.
- Preserve earlier failures and frozen capacity evidence. Never call a partial run complete or substitute synthetic vectors for trained model results.
- Publish one primary first-person Chinese development record. Attribute upstream and AI-assisted work; no fabricated employer, production rollout, audience, results or personal anecdotes.

## Review focus

- Empty/oversized/long batches, wrong model, concurrent requests: bounded rejection without silent truncation.
- Model pooling, padding, normalization, device/dtype and immutable artifact identity: reproduce the actual published model, not made-up vectors.
- Retrieval labels, multiple relevant documents, no-answer scoring and test-set tuning: distinguish subset measurements, hit rate and true recall.
- Loopback exposure, secrets, model code execution and model/dataset redistribution: keep private files out of commits and avoid untrusted remote code.
- Narrative/source consistency: retain failed runs, limited samples, older capacity versions, missing coverage and original authorship.

## Task 1: Local model service and resource probe

**Files:** `scripts/embedding/local_service.py`, `test_local_service.py`, `download_model.py`, `requirements.txt`.

**Interfaces:** consumes fixed local model directory and private token path; produces `/v1/embeddings`, `/healthz`, `/metrics` on loopback only, same ordered float-vector response as the existing Go provider.

- [ ] Write and run failing tests for request bounds, explicit long-input rejection and ordering, without loading a GPU model.
- [ ] Implement lazy-loaded inference service, one in-flight GPU batch, pinned safe weights, real token counting and resource telemetry.
- [ ] Download pinned weights/dependencies into ignored output; verify SHA256; load CUDA model and probe batch1/4 at 512 tokens, verifying dimensions, finite normalized vectors and peaks.
- [ ] Commit the reusable fixture and tests after GREEN. It is a development/acceptance service, not a hardened internet-facing deployment.

## Task 2: Actual semantic retrieval and RAG

**Files:** `backend/internal/service/local_embedding_infrastructure_test.go`, `scripts/embedding/prepare_scifact.py`, existing opt-in API fixture, public JSON/Markdown report.

**Interfaces:** consumes Task1 endpoint and reproducible BEIR SciFact subset + existing Chinese100; produces per-query rankings, true Recall@K/MRR, error/isolation/false-citation counts, latency and model resource report.

- [ ] Assert corpus/query/qrel consistency and deterministic selection; preserve dataset license/source/hash. Keep downloaded text out of Git.
- [ ] Measure dense, keyword and hybrid on existing Chinese cases and the declared public subset; don't invent a trained reranker or tune thresholds against held-out answers.
- [ ] Use real PG/Milvus and real vectors, and explicitly report fixture indexing versus full upload/worker tests.
- [ ] Validate a few authenticated upload→real embedding→retrieval→DS→citation cases through the existing opt-in API fixture and shared spend ledger; no paid broad rerun.
- [ ] Record bad cases and remaining coverage, run appropriate local regression and commit.

## Task 3: One first-person development record and delivery

**Files:** `docs/research/development-journal.md`, README and release index.

- [ ] Read actual design, Git history and P0–P4/real-model reports. Write a connected first-person account: why this project, upstream findings, runtime, durability, data, integration, capacity, real-model defects, local embedding, tradeoffs and remaining work.
- [ ] Link claims to existing reports/commits; use simulated workload numbers only with their hardware, version and controlled-model context.
- [ ] Run full required checks; obtain one fresh whole-branch review, fix Important/Critical findings with RED→GREEN and rerun affected checks.
- [ ] Fast-forward main and push under existing authorization; confirm exact-SHA CI. Retain local evidence and environments.
