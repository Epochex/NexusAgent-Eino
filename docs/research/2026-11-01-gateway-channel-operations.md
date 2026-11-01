# Model gateway channel operations and controlled relay validation

Validated on 2026-11-01 with the digest-pinned New-API v1.0.0-rc.47, NGINX
and Python Compose stack in `tools/modelgateway`. This is an isolated local
fixed-response HTTP experiment, not live-model throughput, quality, SLA or
supplier-billing evidence. Reproduce with `python tools/modelgateway/run.py`
and `python tools/modelgateway/capacity_run.py`.

## Changes and measured effects

| Experiment | Before | After | Independent check |
|---|---:|---:|---|
| 400 requests / 200 client workers | 127 complete (31.75%) | 400 complete (100%) | Baseline 208 NGINX connection-exhaustion messages; tuned 400 upstream dispatches and zero active after drain |
| Every fifth upstream request explicitly rejects with 503 before generation; 1000 logical requests | 800 complete, retry=0 | 1000 complete, one priority fallback retry | Exactly 1200 upstream dispatches: 1000 primary, 200 fallback |
| 180s at target 100 requests/s | Not a comparative run | 18001/18001 complete; observed 99.706 requests/s including drain | 18001 upstream dispatches; zero active after drain; client first-content P95 534.552ms |

The tuned burst had peak upstream concurrency 200, while sustained traffic had
peak 55. Concurrency limit 200 is not the same as 200 active requests throughout.
The steady fixture holds each stream 500ms and injects 25ms first-content delay;
New-API/proxy buffering and Python client scheduling are included in client TTFT.
No successful-only baseline latency comparison is used to claim a latency gain.

The baseline restores the previous 256-connection proxy config from c72bbce;
client and proxy-upstream connections both consume slots. The tuned config raises
that limit to 2048. Only the proxy is recreated between runs: same gateway, data,
token, channels, upstream and fixture accept backlog 512. Buffering, caching and
proxy retries were already off in the baseline, so they are not attributed as
new performance improvements. This is configuration diagnosis, not a new proxy
algorithm or production-capacity certification.

## Actual-channel observation

The new read-only collector uses `/api/log/` with actual channel IDs and Request
IDs. It freezes the time window, deduplicates stable log IDs, bounds pagination,
and rejects incomplete or changing snapshots. The workload additionally checks
that every known client Request ID exists in the saved logs, including failures.
This run produced three model/channel series. An intermediate channel failure
and final fallback consumption remain separate channel events for one request.

Gateway `other.frt` is first response, not client first nonempty content. Unknown
terminal status and billed interruption remain separate from validated completion.
Prometheus exports window gauges plus collection health, last-success and window
timestamps. Failed collection replaces stale values; per-file output is atomic.
Missing timing is unknown, not zero. Quota is gateway units, not supplier money;
immediate concurrent used-quota snapshots can lag asynchronous settlement and are
not an exact-cost reconciliation claim.

## Failed attempts and correction

An earlier 180-second transport run failed final log correlation. Its missing
client result checkpoint is not reconstructed or used as successful evidence.
Investigation of the pinned server found error logging defaults off and is enabled
by `ERROR_LOG_ENABLED=true` at process start. An arbitrary management-option key
with a similar name is accepted but does not change this runtime flag. The final
Compose environment uses the real flag. A request-ID preflight now runs after the
initial failure sample and before sustained traffic; sanitized checkpoints are
saved before telemetry auditing and exported even when validation fails. The
final run above passed both workload and log-completeness checks.

## Verification

- 21 Python unit tests passed, including pagination/ID coverage, interruption,
  stale metrics, baseline validity, SSE parsing and redirect/no-replay behavior.
- 13 real NGINX -> New-API -> local native-protocol HTTP contract scenarios passed.
- Full 180-second workload and exact known-request-ID log coverage passed.
- Independent focused review resolved both findings: reject nonreproduced
  baselines and replace stale healthy metrics on collector failure.
- Reports exclude credentials, prompts and raw provider error bodies. Docker
  network is internal-only with no host ports or paid-provider access.

## Implementation choices

1. Reuse New-API conversion/routing/quota rather than claim authorship of upstream
   adapters; retain Nexus Go runtime capacity, cancellation and stream lifecycle.
2. Prefer failure-stage/status policy over generic transport retries. The fixture
   503 contract is explicit pre-generation rejection; real provider 503s can have
   different billing semantics and require their own integration agreement.
3. Read actual channel events rather than infer channels from configured aliases;
   export bounded snapshot gauges rather than repeatedly incremented counters.
4. Use independent provider dispatch counts and strict finish-reason plus terminal
   frame checks; HTTP 200, gateway `ok`, EOF or DONE alone cannot prove success.

Primary references: [pinned source](https://github.com/QuantumNous/new-api/tree/v1.0.0-rc.47),
[channel operations](https://docs.newapi.pro/en/docs/guide/feature-guide/admin/channel),
[log fields](https://docs.newapi.ai/en/docs/guide/feature-guide/user/log).
The env flag is in `common/init.go`, log fields in `service/log_info_generate.go`
and pagination/record semantics in `model/log.go` in that pinned source.
