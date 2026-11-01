# New-API model gateway acceptance

This tool verifies a **real, unmodified New-API v1.0.0-rc.47 container** behind
NGINX, using deterministic local OpenAI, Claude and Gemini HTTP peers. It is a
deployment / protocol / failure-semantics experiment, not a live model-quality
benchmark or an external-provider billing guarantee.

## Run

Requires Docker Compose and Python 3.11+. From the repository root:

```sh
python tools/modelgateway/run.py --output .superpowers/modelgateway-report.json
```

The runner refuses an existing lab, generates credentials in memory, initializes
the gateway through its actual setup/login/channel/token/option APIs, runs the
checks, copies a sanitized report, and removes its containers in `finally`.
No credentials, conversation text or provider error bodies are exported. There
are no published ports. All three services use an internal-only Docker network;
the gateway has no route to public model APIs. Gateway, NGINX and Python images
are digest-pinned. SQLite uses tmpfs: this is disposable, not a production data
retention or database HA architecture.

If a killed runner leaves the lab behind, inspect `docker compose -f
tools/modelgateway/compose.yaml ps` and explicitly remove only that lab with
`docker compose -f tools/modelgateway/compose.yaml down`. Compose requires
`GATEWAY_SESSION_SECRET` to be set even for inspection (any temporary random
value suffices for teardown; never reuse one for production).

## Checks and measurements

- OpenAI-compatible client -> OpenAI / Anthropic / Gemini upstreams, each with
  JSON and SSE: model aliases, system instruction, tool schema, fragmented tool
  arguments, tool result references, subsequent turn and base64 image payload.
  These test transport/format preservation, not vision or tool-selection quality.
- Model allowlist, empty token quota and model request rate limit: rejection
  occurs before dispatch; verify the upstream HTTP call count independently.
- Explicit fixture HTTP 503 rejection -> lower-priority healthy channel. Compare
  two upstream attempts with **one** successful logical request and 16 gateway
  quota units (12 input + 4 output at unit ratios). These are configured gateway
  units, not dollars or a real provider invoice.
- Timeout comparison: the initial broad retry policy dispatches twice. Allowing
  only 503 in this fixture's explicit-rejection contract reduces an ambiguous
  transport timeout to one dispatch. Both failed paths leave gateway token quota
  unchanged. Real providers may still charge after a timeout; stopping retries
  limits amplification and cannot cancel or reconcile their unknown billing.
  Do not reuse this allowlist without checking provider semantics: even a 503
  cannot universally prove that work/billing did not happen.
- Partial stream: one upstream call, incomplete result, no replay. Require both
  an acceptable finish reason and `[DONE]`; gateway-emitted `[DONE]` alone does
  not establish completion. New-API may estimate usage on this path; report it
  as a gateway estimate with provider usage unknown.
- Bounded experiment: 120 requests / 12 workers; all failures stay in the logical
  denominator, upstream attempts checked separately. Per-model/configured-route
  groups report successes, stream interruptions, TTFT samples and nearest-rank
  p95. The injected 25ms fixture delay is part of TTFT. This short, small local
  workload does not establish production capacity or reliability.

TTFT is measured from client dispatch to the first nonempty content/tool delta,
excluding response headers, assistant role, heartbeats and usage-only chunks.
HTTP read timeout, 30s parser duration check, 256KiB event and 4MiB response caps
bound probe work (a blocked read can exceed the parser duration by the socket
read timeout). Unknown usage is `null`; never silently turn it into zero cost.
The original probe identifies the configured route; failover is separately proven
with ordered fixture records. The optional operations collector below reads the
gateway's actual channel IDs, independently of these configured-route groups.

NGINX disables response buffering/cache and upstream retries. Its 8s read timeout
is above the gateway's 2s relay / stream timeout, so gateway failure handling wins
the race in these cases. Timeout policies trade resilience against duplicate paid
work; this suite deliberately uses a conservative transport policy.

## Design choices and official references

Use New-API for deployment/routing/quota/protocol conversion rather than writing
a second incomplete gateway. Retain Nexus's Go Eino runtime and cancellation /
capacity controls independently; these checks can be run before changing its
configured model endpoint. A lightweight stdlib probe keeps deterministic CI free
of model weights, GPUs, paid credentials and a Python SDK version dependency.

- [New-API source, pinned release](https://github.com/QuantumNous/new-api/tree/v1.0.0-rc.47):
  `service/relay_error.go`, `relay/channel/api_request.go`, provider adapters and
  `setting/operation_setting/status_code_ranges.go` explain timeout wrapping,
  retry status policy and conversion responsibilities.
- [New-API deployment](https://docs.newapi.pro/en/docs/installation)
- [Claude stream event lifecycle](https://platform.claude.com/docs/en/build-with-claude/streaming)
- [Gemini function calls/results](https://ai.google.dev/gemini-api/docs/function-calling)
- [OpenAI Chat API](https://developers.openai.com/api/reference/resources/chat)

No claim of upstream New-API code contribution is made: the contribution here is
repeatable deployment, conservative configuration, independent probes and CI.


## Channel operations collector

`operations.py` reads management logs without making inference calls. Enable
`ERROR_LOG_ENABLED=true` in the New-API **server environment** before starting
it; the pinned server defaults to excluding error logs. This is not an option
that can be enabled by writing an arbitrary management-option key.

```sh
# Supply NEW_API_ADMIN_TOKEN through your secret manager/environment.
python tools/modelgateway/operations.py --base-url https://gateway.example \
  --user-id 1 --window-seconds 300 --output /tmp/gateway-window.json \
  --prometheus /tmp/gateway-window.prom
```

The collector freezes a time window, paginates at 100 records/page with a bounded
500-page cap, deduplicates stable log IDs, and rejects changing totals, conflicting
rows or incomplete pagination. Capacity experiments additionally require every
known client Request ID to occur in the saved logs. Snapshot completeness means
completeness of **saved logs**; it cannot recover requests the server never logged.

Consume/error events are grouped by actual model and channel ID. A failed first
channel followed by a healthy fallback remains two channel events but one logical
request. Gateway `frt` is first **response**, which can be a role frame; only the
client probe measures first nonempty content/tool output. Missing timing stays
unknown. Gateway stream `ok` is separate from a validated terminal outcome;
interrupted, billed, and unknown streams are not silently classified as completed.
Quota units are gateway accounting, not supplier money. Prometheus labels never
include Request IDs, prompts, keys or error bodies.

Metrics are **window gauges**, not monotonically increasing counters. Inspect
`gateway_collection_success`, `gateway_collection_last_success_timestamp_seconds`
and the window timestamps before using channel values. A failed collection
atomically replaces the old export with a failed health snapshot and no channel
values, preserving last-success time. JSON and Prometheus files are each written
with a same-directory temporary file, fsync and atomic replace.

## Controlled capacity and failure comparison

```sh
# 200 client workers / 400-request burst; then explicit 503 faults;
# finally 180 seconds at a target 100 requests/s.
python tools/modelgateway/capacity_run.py
# CI profile retains both bursts and fault samples, shortens only steady traffic.
python tools/modelgateway/capacity_run.py --duration 3 --rps 10
```

The baseline mounts the previous proxy configuration (256 worker connections).
Only the proxy is replaced by the 2048-connection configuration for the comparison;
New-API, channels, token and database remain running. Both profiles use the same
512-entry fixture accept backlog, no proxy buffering/cache/retry, fixed 25ms
first-content delay and 500ms hold. Baseline reproduction requires both failed
client requests and NGINX connection-exhaustion logs; a fully passing baseline is
inconclusive, not a performance improvement. Baseline failures stay in denominators.
Do not compare successful-only baseline latency against all tuned requests as if
it proved lower latency.

The fault fixture rejects every fifth dispatch with 503 **before generation**.
Retry=0 is compared with one retry to a lower-priority healthy channel. The runner
checks all 1000 logical outcomes and exactly 1200 upstream attempts after fallback.
These rejection semantics do not generalize to arbitrary real provider 503s.
The sustained generator has a fixed arrival rate, bounded workers/pending queue,
independent upstream dispatch counts and an explicit final drain check. Generator
backpressure remains a failed logical request. The reported request rate includes
the drain interval. No live-model throughput, SLA or quality claim follows.

A request-ID logging preflight runs before the sustained phase. Sanitized workload
checkpoints are saved before telemetry validation and copied out on failure;
failed runs remain failures. The runner removes its disposable containers in
`finally`. Immediate high-concurrency token quota reads can lag async settlement;
they are diagnostic snapshots, not an exact supplier-cost reconciliation.

- [New-API channel management](https://docs.newapi.pro/en/docs/guide/feature-guide/admin/channel)
- [New-API log fields](https://docs.newapi.ai/en/docs/guide/feature-guide/user/log)
