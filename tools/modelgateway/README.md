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
Metrics identify the configured route; failover is separately proven with ordered
fixture records. They are not a production channel telemetry collector.

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
