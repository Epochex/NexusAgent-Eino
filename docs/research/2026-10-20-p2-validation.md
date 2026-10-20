# P2 durable workflow validation

Date: 2026-10-20. Status: execution authority, Eino compiler, product integration and process-fault experiments implemented. This report does not establish P3 integration quality, P4 capacity, or real-model correctness.

## Verified behavior

- Real PostgreSQL 16 migrations 004–006, isolated schema per integration test. Concurrent claim fencing, late commit rejection, encrypted immutable model settings, frozen root/subflow DSL, checkpoint compatibility, sequential/parallel/nested exact approval recovery.
- Stable operation IDs and attempts, committed result reuse, safe retry backoff, unknown manual-write outcome requiring explicit owner/hash/evidence reconciliation. Cancellation and whole-run deadlines preserve uncertain effects and prevent late success or resurrection of a canceled run.
- Eight simultaneous submissions with the same request key return one run; changed input conflicts. Workflow updates use version CAS, archive preserves history and existing jobs. PostgreSQL-only workers skip frozen capabilities they cannot supply.
- Real Hertz TCP + PostgreSQL: HTTP 202 persisted submission, integer input 9007199254740993 unchanged, unauthorized owner denied, stale approval hash returns 409, approval comment enters event audit, event subscription disconnect leaves work alive, cancel remains terminal.
- Failed notification publisher leaves the outbox pending; confirmed delivery marks it. Seven-day completed-event retention preserves active history and snapshots, and expired cursors explicitly fail. Delivery remains at least once.

## Process and network faults

`TestWorkerProcessFaultRecovery` builds and runs the actual `cmd/worker` binary, with normal 15-second leases, production service wiring, frozen encrypted settings and separate temporary working directories. Redis, Milvus and MinIO are absent in this recovery mode.

- Kill during an actual HTTP model request: a replacement worker waits for the lease, completes the run and retains one logical operation with attempt 2. Exactly two peer requests occur; the successful second response is the persisted output.
- Approve an actual `file_ops save`, then block the operation-result commit with a schema-local PostgreSQL advisory-lock trigger. After the filesystem write has returned, kill the worker. A replacement enters `needs_attention`, retains `unknown` attempt 1 and leaves file modification time/content unchanged. This is a real filesystem side effect, not proof of idempotency for every external API.
- The initial HTTP fault fixture hung because it had not consumed the request body before waiting on disconnect. Corrected body/connection ownership, then repeated successfully. Only verified test/child processes were stopped; no local deletion was retried.

`TestRedisOutageRetainsNotificationsAndPGProgress` uses real Redis 7.2.4 with a connection wrapper injecting network errors. The workflow reaches approval through PG polling while outbox delivery fails. Restoring the connection drains pending notifications to a real stream; 20 duplicate wake-up hints still yield one business execution and generation 2 after approval. This tests a Redis network outage, rather than restarting the Redis server or asserting anything about Redis persistence configuration.

## Browser walkthrough

An opt-in fixture runs production JWT/auth/workflow handlers, service, Eino worker and real PG/Redis. Vite serves the existing React console. Only auth/workflow dependencies are provisioned in this fixture; this is not a full-platform walkthrough.

Observed a real failure before the fix: StrictMode called bootstrap twice, competing to rotate the same refresh token; a page reload logged the user out. Single-flight bootstrap and disabling recursive refresh/login retries fixed it. Repeated full-page navigation to workflow and run history then preserved authentication without 401 errors.

Submitted `UI-42`, closed its modal, found the still-live run in history, approved `root/first`, observed the separate `root/second` approval, approved again and saw the successful output. Archived the definition and verified the successful historical run remained. The fixture shut down normally and drained its worker/listener. [Captured result](../images/p2-two-approvals-completed.png). The capture preceded the additional decided-approval history display; both persisted comments are covered by a subsequent PG service regression.

## Commands and environment

- Windows Go 1.26.0: full unit suite and current vet/build/PG execution/service/API integration passed. The latter includes process/Redis faults (`execution` 65.014s, `service` 22.079s, `api` 8.968s). The HTTP fixture owns and closes its keep-alive pool; a shutdown failure was reproduced and fixed with five focused repetitions, rather than increasing the timeout.
- Latest fixed-source Linux run: `go vet ./...`, `go test -race ./... -count=1 -timeout 180s`, `go test -race -tags=integration ./internal/execution ./internal/service ./internal/api -count=1 -timeout 180s`, and server/worker/cli/migrate build all passed in `golang:1.26.0`, 4 CPU / 4 GiB. Integration durations: execution 65.525s, service 17.168s, API 6.766s, including actual process/Redis faults. The opt-in browser fixture skips in normal CI; it is not counted as automated browser coverage. Set both `NX_TEST_DATABASE_DSN` and `NX_TEST_REDIS_ADDR` explicitly for the integration command.
- Frontend Node 24.14.1: TypeScript checking and production Vite build passed, including workflow controls and auth correction.
- Local ephemeral services use only loopback ports: PostgreSQL 55439 and Redis 56389. No real-model key was configured; deterministic HTTP model peers prove protocol/snapshot behavior only.

## Boundaries to retain

The API accepts external-success reconciliation as an explicit attestation; it does not query remote systems or implement confirmed-not-applied replay. PostgreSQL-only recovery workers exclude unsupported KB/tools and retain events until notification workers return. Full worker startup still requires its configured ETL dependencies. Unknown operations cannot be explained away as automatic exactly-once execution. Database outage, data pipelines, protocol integrations and sustained capacity have their own remaining acceptance work.

See [runtime runbook](workflow-runtime-runbook.md) for endpoints, recovery and retention.
