# P2 durable workflow validation

Date: 2026-10-20. Status: execution authority, Eino compiler and product integration implemented; process-kill experiments are the next task. This report does not establish P3 integration quality, P4 capacity, or real-model correctness.

## Verified behavior

- Real PostgreSQL 16 migrations 004–006, isolated schema per integration test. Concurrent claim fencing, late commit rejection, encrypted immutable model settings, frozen root/subflow DSL, checkpoint compatibility, sequential/parallel/nested exact approval recovery.
- Stable operation IDs and attempts, committed result reuse, safe retry backoff, unknown manual-write outcome requiring explicit owner/hash/evidence reconciliation. Cancellation and whole-run deadlines preserve uncertain effects and prevent late success or resurrection of a canceled run.
- Eight simultaneous submissions with the same request key return one run; changed input conflicts. Workflow updates use version CAS, archive preserves history and existing jobs. PostgreSQL-only workers skip frozen capabilities they cannot supply.
- Real Hertz TCP + PostgreSQL: HTTP 202 persisted submission, integer input 9007199254740993 unchanged, unauthorized owner denied, stale approval hash returns 409, approval comment enters event audit, event subscription disconnect leaves work alive, cancel remains terminal.
- Failed notification publisher leaves the outbox pending; confirmed delivery marks it. Seven-day completed-event retention preserves active history and snapshots, and expired cursors explicitly fail. Delivery remains at least once.

## Browser walkthrough

An opt-in fixture runs production JWT/auth/workflow handlers, service, Eino worker and real PG/Redis. Vite serves the existing React console. Only auth/workflow dependencies are provisioned in this fixture; this is not a full-platform walkthrough.

Observed a real failure before the fix: StrictMode called bootstrap twice, competing to rotate the same refresh token; a page reload logged the user out. Single-flight bootstrap and disabling recursive refresh/login retries fixed it. Repeated full-page navigation to workflow and run history then preserved authentication without 401 errors.

Submitted `UI-42`, closed its modal, found the still-live run in history, approved `root/first`, observed the separate `root/second` approval, approved again and saw the successful output. Archived the definition and verified the successful historical run remained. The fixture shut down normally and drained its worker/listener. [Captured result](../images/p2-two-approvals-completed.png). The capture preceded the additional decided-approval history display; both persisted comments are covered by a subsequent PG service regression.

## Commands and environment

- Windows Go 1.26.0: full unit suite/vet/build and PG execution/service/API integration suite passed during implementation; targeted regressions were repeated after fixes.
- Latest fixed-source Linux run: `go vet ./...`, `go test -race ./... -count=1 -timeout 180s`, `go test -race -tags=integration ./internal/execution ./internal/service ./internal/api -count=1 -timeout 180s`, and server/worker/cli/migrate build all passed in `golang:1.26.0`, 4 CPU / 4 GiB. The opt-in browser fixture skips in normal CI; it is not counted as automated browser coverage.
- Frontend Node 24.14.1: TypeScript checking and production Vite build passed, including workflow controls and auth correction.
- Local ephemeral services use only loopback ports: PostgreSQL 55439 and Redis 56389. No real-model key was configured; deterministic HTTP model peers prove protocol/snapshot behavior only.

## Boundaries to retain

The API accepts external-success reconciliation as an explicit attestation; it does not query remote systems or implement confirmed-not-applied replay. PostgreSQL-only recovery workers exclude unsupported KB/tools and retain events until notification workers return. Full worker startup still requires its configured ETL dependencies. Unknown operations cannot be explained away as automatic exactly-once execution. Process termination, infrastructure faults, data pipelines, protocol integrations and sustained capacity have their own remaining acceptance work.

See [runtime runbook](workflow-runtime-runbook.md) for endpoints, recovery and retention.
