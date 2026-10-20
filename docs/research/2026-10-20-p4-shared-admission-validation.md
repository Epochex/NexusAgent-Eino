# P4 shared admission validation

This is the shared-admission correctness record, separate from the subsequent
capacity workload and real-model validation. All model services below are
controlled HTTP peers. The backing PostgreSQL and processes are real.

| Case | Before change | Validated behavior |
| --- | --- | --- |
| 48 concurrent claims, 10 owners/50 runs | 50 active; 5 per owner; flooded owner served repeatedly | At most 32 active, 4 per owner, least recently served eligible owner; approval/backoff release slots |
| Workflow queue | 101st owner submission accepted | Owner outstanding bound 100; cached request remains readable at capacity; changed payload conflicts |
| Two service instances / 24 provider calls | 24 simultaneous calls under a nominal 16 limit | 16 actual HTTP calls and 8 explicit admission rejections |
| Loss of the owned PG connection pool | Model HTTP continued beyond 8 seconds | Renewal loss cancels the actual provider request, with bounded expiration |
| Lost provider lease, streaming | Cancellation could look like normal EOF; full buffer retained local semaphore | Terminal error is observable and the local slot is released even with a saturated 64-entry buffer |
| Chat preparation lock wait | Ignored a configured 100 ms total run budget | Preparation is covered by the total deadline |
| Expired Chat lease before finalization | Assistant success committed and `done` published | Successful INSERT rolls back; failed provisional answer may be retained |
| Lease expires while assistant INSERT waits on PG lock | Stale success still committed | Last SQL write checks current PG clock and token inside the answer transaction |
| Administrator policy HTTP | No shared operational API (404) | DB usage snapshot, versioned updates, 400 validation, 409 stale update, and immediate role revocation |

Two real `cmd/worker` processes then competed for 18 runs from three actual
owners. Their local bounds each allowed four slots; the shared policy allowed
four total and two per owner. The observer sampled PostgreSQL and counted actual
HTTP peer calls. All 18 runs completed, peak actual concurrency was four, and
all three owners progressed. Killing a holder did not create extra capacity
before expiry; the survivor reclaimed expired work. The peer saw 20 calls due
to two safe read-only recoveries. This does not assert remote write exactly-once.
The Windows run passed in 26.018 seconds. The subsequent Linux 4 CPU/4 GiB
verification passed vet, all-package race tests, full real-infrastructure/process
integration tests, and builds of server/worker/cli/migrate. Integration package
times: execution 235.379 s, service 204.264 s, API 25.968 s, repo 6.538 s,
ingestion 23.704 s, vector 5.230 s, memory 26.361 s, tool 2.913 s, A2A 7.410 s,
capacity 5.085 s. This is correctness-test resource allocation, separate from
the specified capacity workload's server+worker 4 CPU/8 GiB budget.

The first full matrix failed because the legacy MCP upgrade fixture stopped at
migration 011 before constructing the current service, which requires 013. It
now seeds the old state at 010 and applies 011–013 before startup; the original
manual-review, credential-redaction, and explicit-encryption assertions remain.
Its focused regression and the final full matrix passed. Production startup
continues to fail closed on missing quota schema.

The burst fixture initially used GORM's unbounded open-connection default and
two idle connections. On Windows, opening 48 fresh connections caused variable
three-second lock/acquisition deadlines. Real candidate-query EXPLAIN measured
0.472–1.047 ms with no JIT; evidence did not support a SQL/JIT optimization claim.
The concurrency fixture now has a deliberate 16 open/idle connection budget.
Three Windows repeats passed (execution 27.008 s); initial Linux race repeats
also passed (execution 16.622 s, service 25.517 s). No production timeout was
increased to conceal those fixture failures.

Other focused successful runs: normal stream lease-loss 10 repeats (19.751 s),
full-buffer and normal stream regression five repeats (service 18.020 s;
gateway 1.405 s), mixed interactive/workflow and stale-token tests three repeats
(12.466 s), Chat commit/cancel/timeout regressions (13.242 s), and current-admin
policy HTTP test (2.890 s). Failed iterations remain in local validation logs,
including two policy-snapshot implementation errors corrected before that last
valid pass. No benchmark percentile, semantic-model quality, or real-model cost
is inferred from these correctness timings.

Operations, semantics, limitations, and rollback compatibility are documented in
[the shared capacity runbook](../shared-capacity-runbook.md).
