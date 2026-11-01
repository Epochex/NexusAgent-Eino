# Model gateway deployment and failure-semantics validation

Date: 2026-11-01 (Asia/Shanghai).

## Implementation

`tools/modelgateway` deploys the real New-API v1.0.0-rc.47 release behind NGINX,
initializes it through management HTTP APIs, configures OpenAI / Anthropic /
Gemini channels, model aliases, priority, scoped tokens and quota, then runs a
repeatable Python acceptance suite. All images are digest-pinned, the Docker
network is internal only, no ports are published, credentials remain in memory,
and the disposable SQLite data lives in tmpfs. The runner removes its own lab.

New-API performs the protocol conversions; this work implements deployment,
configuration, independent probes/oracles and CI. It does not claim authorship
of upstream adapters or an upstream New-API patch.

## Observations

- **13/13 integration cases passed** through actual NGINX -> New-API -> fixture
  HTTP connections. Each of three providers passed JSON and streaming tool/image
  round-trips. Exact synthetic-oracle checks cover system text, host parameter
  schema/required fields, tool return identity/content and decoded image bytes.
- Allowlist and empty token quota rejected with **zero** upstream calls. The
  configured request rate limit allowed one inference and rejected the next
  before a second upstream call.
- Explicit fixture 503 rejection switched to the lower-priority channel:
  **2 attempts / 1 completed logical request**, gateway token usage delta **16**
  quota units, matching fixture 12+4 usage and configured unit ratios.
- Source inspection traced transport timeout -> `ErrorCodeDoRequestFailed` ->
  HTTP 500 -> broad retry status match in `service/relay_error.go`,
  `relay/compatible_handler.go` and `relay/channel/api_request.go`. Baseline
  timeout dispatched **twice**. Restricting `AutomaticRetryStatusCodes` to the
  fixture's explicit rejection status **503** reduced this to **one** attempt;
  gateway token usage delta was zero. This intentionally trades broad retries
  for conservative handling of unknown outcomes, not exactly-once billing.
- Partial output reached the client with HTTP 200 and measured content TTFT.
  Without an acceptable finish reason, gateway `[DONE]` did not make it complete;
  **one** upstream call, no replay. Gateway-estimated usage remained explicitly
  distinct from unknown provider usage.
- Bounded local experiment: **120 requests at concurrency 12**, **120 completed**,
  **120 upstream calls**, content TTFT p95 **63.850ms**, including the injected
  25ms fixture delay. This is a short deterministic workload, not production
  throughput or availability. Fault/rejection cases remain in report metrics
  denominators; availability is not inferred from acceptance success.
- **10/10 unit regressions passed**, including a real local HTTP truncated-SSE
  test. Independent review found weak protocol oracles, missing partial-output
  assertions and an exceptional-stream metric omission; all three were fixed,
  and the reviewer independently reran the ten tests successfully. Full
  container acceptance was rerun after those fixes and passed all thirteen.

See `tools/modelgateway/README.md` for exact reproduction and trade-offs. A new
isolated `model-gateway-contract` CI job runs the same command without credentials.

## Limits

Peers are deterministic local HTTP implementations, not paid provider models.
Tests establish message conversion, routing, gates and failure/accounting
behavior of this pinned deployment, not semantic quality, real invoices,
multimodal inference correctness, database HA or production service experience.
Metrics are grouped by model/configured route; actual priority failover and
attempt counts are independently checked against fixture records. They are not
a production per-channel collector. An upstream may still charge after a lost
response; avoiding another dispatch cannot reconcile its unknown outcome.

The stable release artifact and runtime are used without modifying New-API.
Compatibility with other releases requires rerunning acceptance rather than
assuming management endpoints or stream semantics remain unchanged.
