# Durable workflow operations

## Execution ownership

The API persists a frozen workflow submission and returns HTTP 202 with its run ID. The browser polls the run; closing the tab or event subscription does not cancel it. A worker owns each attempt through a PostgreSQL lease and generation. Waiting for approval releases its execution slot. Install all migrations through 013 before deploying the current runtime; application startup does not AutoMigrate.

Run the usual server and `go run ./cmd/worker` from `backend/` with shared database and encryption configuration. The default worker enables `execution,doc-etl,memory`, including knowledge-base tooling and memory jobs. For recovery without Redis/Milvus/MinIO, start a separate `go run ./cmd/worker -queues execution -execution-concurrency 4`. That mode enables model, human and non-KB tool nodes; it does not provide knowledge-base retrieval. Full-capability workers require the document/vector services. Configure every process with the same durable encryption key; changing it invalidates encrypted model snapshots.

PostgreSQL is authoritative. Workers poll it even when notifications are absent. Redis delivery uses a transactional outbox: confirm XADD/publish before recording `published_at`. Delivery is at least once; `(run_id,seq)` identifies duplicates. Redis stream history is bounded to approximately 100000 messages and is only a hint, not the replay source.

## Product API

- `POST /api/v1/workflows/run`: `{workflow_id,input,request_key}`. Reuse a request key for network retries of the same submission; conflicting workflow/input returns 409. A different deliberate execution needs a new key. `Idempotency-Key` is accepted as an alternative; conflicting header/body keys are rejected.
- `GET /api/v1/workflows/runs/:id`: snapshot, immutable release/hash, status, steps, active approval IDs and argument hashes.
- `POST .../:id/approve`: `{interrupt_id,args_hash,approved,comment}`. The server validates the current pending interrupt and arguments. A stale decision returns 409: reload the snapshot and review again. Legacy omission selects only one unambiguous pending interrupt.
- `POST .../:id/cancel`: fences the worker and records cancellation. Already issued remote writes cannot be undone. In-flight manual operations retain `unknown`; the operation list and event audit must be inspected.
- `GET .../:id/operations`: stable operation ID/path/hash, policy, attempts and result status.
- `POST .../:id/reconcile`: `{operation_id,args_hash,output,evidence}` records an externally verified successful result for a `needs_attention` run and resumes without repeating that write. This is an owner's explicit attestation, not an automatic remote status lookup. Canceled jobs cannot be resurrected by it. A confirmed-not-applied replay control is not implemented.
- `GET .../:id/events?after=N`: at most 128 ordered events. With `Accept: text/event-stream`, tail the same log; each SSE `id` is its sequence. Reconnect with `Last-Event-ID` or `after`. Subscribers have a 30-minute lifetime and a two-second socket write deadline per chunk.

All business endpoints require JWT and validate ownership. An approval decision, run transition, event and outbox intent commit atomically. `needs_attention` is blocked work requiring review, not business success. Unsupported checkpoint versions also enter that state rather than silently starting over.

## Retention and diagnosis

The notification loop prunes batches of 1000 events hourly for runs completed more than seven days ago, only after notifications are confirmed. Active/blocked jobs, unpublished outbox records and run snapshots remain. A cursor older than retained history returns HTTP 410; fetch the run snapshot and reconnect from its `last_seq`. This retention is enabled in full workers running notification delivery; PG-only recovery workers do not prune event history.

The whole-run deadline includes approval/reconciliation waiting. A once-per-second sweep fails overdue jobs and fences in-flight execution. Unknown external effects remain visible. Lease renewal failure cancels the executor; it does not manufacture a successful result. Shutdown cancels local work and waits up to five seconds; another worker recovers after the lease expires.

Inspect database reachability, migration version, queue/lease age, deadline, `error`, operation status, active approval ID and runtime compatibility before retrying. Never clear an unknown write or delete checkpoints merely to make a job run again. Validate in the external system and keep its reference in the reconciliation evidence.

## Validation limits

Deterministic HTTP model/tool peers verify protocol and recovery, not real model answer quality or cost. No real-model credentials were configured for this delivery. Capacity targets and full-platform integrations require their separate P3/P4 evidence; successful workflow unit tests do not establish them.
