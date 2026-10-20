# Shared execution and provider capacity

PostgreSQL is the admission authority. Apply migration 013 before starting this
version. Drain every older server/worker before relying on global limits: older
binaries do not participate in shared admission. Redis only carries wake hints.

Interactive Chat (including inbound A2A Chat) and running durable workflows share
the `run` pool. Defaults: 32 active globally, 4 per owner; 1,000 outstanding
workflows globally, 100 per owner. Approval waits and retry backoff consume queue
capacity but no active slot. Cached idempotent requests remain readable at queue
capacity. New submissions receive HTTP 429. Workers pick the least recently
served eligible owner and skip owners already at their active limit.

The separate `provider` policy defaults to 16 simultaneous calls per canonical
HTTP origin, 4 per owner per origin. Origin hashes, rather than record IDs or
names, group frozen/renamed configurations and different credentials on the same
host conservatively. Paths on the same origin share capacity. Background calls
without an actor share quota group 0; this grants no user identity or data access.
Local worker/gateway bounds remain additional limits. A stream holds provider
capacity until consumption finishes or is canceled, including a full output
buffer. Admission failures do not count as upstream circuit breaker failures.

## Operating the policies

Authenticated current administrators can inspect `GET /api/v1/admin/capacity`.
It returns `policies`, `active_runs`, `active_provider_calls`,
`outstanding_workflows`, and the database `observed_at` timestamp. Usage counts
are sampled in one SQL statement; the policies are read immediately beforehand.
Provider usage is the sum across origins, so it can exceed the policy's
per-origin limit. Queue limits in the provider row are reserved fields: provider
calls fail fast on capacity and have no shared waiting queue.

`PUT /api/v1/admin/capacity/run` (or `/provider`) requires every numeric field:

```json
{"version":1,"active_limit":32,"owner_active_limit":4,"queue_limit":1000,"owner_queue_limit":100,"lease_ms":15000}
```

Read the current version first. A successful update increments it; stale updates
return 409 and invalid limits return 400. Active limit is 1–1,024, owner active
limit is 1–global limit, outstanding limit is 1–100,000, owner outstanding limit
is 1–global outstanding limit, and lease duration is 1,000–60,000 ms. Updates
serialize with admissions through the policy row. Shrinking does not revoke
existing work; new admission waits for usage to fall below the new limits.
Changing lease duration affects new leases only. Administrator privileges are
checked against the live users table, even with a previously issued JWT.

## Lease loss and recovery

Interactive/provider leases expire after 15 seconds by default and renew every
third of their acquired duration. Renewal has a two-second bound and cannot
revive an expired lease. Losing renewal cancels the associated request. Releasing
an old unique token cannot release a replacement's slot. Durable workflows keep
their existing generation, deadline, and write fences; the shared active view
reads these leases directly rather than mirroring their state.

Chat's total deadline includes preparation. A successful assistant message
transaction checks its opaque run token as the last SQL write, using the current
database clock after any message lock wait. An expired executor cannot commit
that success; provisional text can be preserved with failed status. Canceled
requests still have a separately bounded final persistence attempt.

Canceling a connection cannot guarantee that an opaque remote service stopped
its side effect. Existing manual/idempotent workflow replay policies and unknown
outcome reconciliation still apply. These leases do not provide remote exactly
once execution.

`capacity_operations_total{scope,outcome}` counts this process's observed
admissions/rejections/errors and renewal/release failures. It is not a global
usage gauge. Use the administrator DB snapshot for shared usage. Labels do not
contain owner IDs, URLs, credentials, or origin hashes.

Migration 013 down intentionally preserves policies, leases, indexes, and the
view. Do not delete deduplication or secure state to downgrade. Deployment and
rollback still require compatible binaries and draining incompatible claimers.

## Evidence boundaries

The integration suite exercises mixed Chat/workflow admission, concurrent
owners, bounded queues, stale tokens, full-stream cancellation, policy CAS and
live privilege revocation. Two real worker processes execute 18 jobs from three
owners under a shared limit of four and per-owner limit of two, including killing
a holder and recovery after expiry. These are controlled model/infra correctness
tests, not a real-model quality result or the P4 30-minute capacity acceptance.
