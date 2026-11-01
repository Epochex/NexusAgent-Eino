# Model gateway acceptance implementation plan

> Execute inline with executing-plans; request one fresh review before merge.

**Goal:** Make New-API deployment, protocol conversion, stream completeness and upstream dispatch accounting reproducible for Nexus model integrations.

**Architecture:** A pinned unmodified New-API container relays requests to an isolated deterministic HTTP provider fixture. A dependency-free Python client bootstraps channels/tokens through management APIs, validates converted messages/tool calls, and emits a redacted per-model/channel report. This is an integration/operations tool, not a replacement router or evidence of real model quality.

**Tech Stack:** Python 3.11 stdlib, Docker Compose, New-API v1.0.0-rc.47, HTTP/SSE.

## Global constraints
- No real provider credentials or paid requests; isolated fixture network only.
- Pin image digest; bind published ports to loopback; ephemeral credentials/data.
- No client retries for inference POST; report incomplete streams and unknown usage.
- TTFT means first nonempty content or tool delta, not headers/role/keepalive.
- Never count EOF or a gateway-emitted DONE alone as successful completion.
- Keep this work under tools/modelgateway; do not change existing Go runtime.

## Review focus
- Truncated/oversized SSE, Unicode, usage-only chunks and role-only chunks.
- Permission/quota rejection must have zero upstream dispatches.
- Retry/failover attempts must be counted independently of logical requests.
- Partial stream failure must not replay an already-started inference.
- Report must contain no keys, cookies, prompts or sensitive provider errors.

### Task 1: Probe contract and stream parser
- [ ] Add failing tests for valid fragmented tool stream, malformed/truncated SSE, incomplete finish and TTFT semantics.
- [ ] Implement bounded SSE parser and normalized probe result; run unittest.

### Task 2: Real deployment and acceptance scenarios
- [ ] Add pinned Compose and local OpenAI/Claude/Gemini HTTP fixtures.
- [ ] Bootstrap actual New-API via API, verify model mapping, multimodal and tool round-trip, HTTP errors, quota, denied models, retry/failover and half-stream dispatch counts.
- [ ] Run bounded concurrency experiment; export per-case results and per-model/channel metrics with failure denominator and known/unknown usage.
- [ ] Add isolated gateway CI job and documentation; run complete local acceptance.

### Task 3: Review and publication
- [ ] Fresh review, resolve important findings, rerun affected checks.
- [ ] PR and merge to main under standing user authorization; verify main CI.
- [ ] Tailor one-page RC CV, preserve protected sections, render and inspect.
