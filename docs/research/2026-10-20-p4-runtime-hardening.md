# P4 runtime boundary checks

These are controlled-peer correctness and fault tests. The 30-minute open-load
acceptance and real-model quality/cost are separate results.

- **Model output/context:** actual HTTP tests reproduced an Agent raising a
  model's configured 1,024 output cap to 4,096, and an Eino call option raising
  a 128 cap to 4,096. An immutable configured ceiling now survives per-call
  overrides. Every actual attempt, including fallback and subsequent tool-result
  turns, reserves output and checks the combined serialized messages/tools.
  This uses conservative UTF-8 byte/framing accounting; it is not an exact
  tokenizer or a claimed universal estimation accuracy. The gateway/adapter/
  runtime/service regressions passed.
- **Outbox:** 512 committed notifications left 320 pending after 3.5 seconds
  under the old 32-per-500ms publisher. Continuous bounded batches plus one
  transactional batch acknowledgement drained the same backlog in 619ms in a
  focused run (532ms in a later repeat). Empty queues/failures back off. Real
  Redis outage, failed SQL acknowledgement, partial peer failure, and stable
  redelivery identity tests passed. These timings are not a capacity benchmark.
- **Claim clock:** a deterministic PG test held the capacity lock until a task
  became ready. Transaction-start `NOW()` then returned no work. The candidate
  query now uses `clock_timestamp()` after the wait. Two regression repetitions
  passed with outbox/outage/retention tests (21.756s total).
- **Process shutdown:** the actual server at commit `3cfb6d3`, exported into a
  separate local validation directory, failed to stop active Chat and an idle
  workflow SSE before a four-second test deadline; no stopped assistant answer
  had committed, and one slot remained leased. The current actual server passed
  the same Linux SIGTERM case: model/streams stopped, the stopped answer committed,
  the slot released, and the durable approval remained pending. The observed
  drain was 38.367ms; the complete race test including build/startup took 19.697s.
  It is one measured case, not a general shutdown latency guarantee.
- **Diagnostics:** server and worker have explicit private `/runtime`, `/metrics`,
  and pprof listeners with owned shutdown. Empty `diagnostics_addr` disables the
  listener. Actual TCP tests verified runtime counters, metrics, profiles,
  listener closure, and the disabled configuration.

## Dedicated PDF parser

API/worker processes no longer parse PDFs in process. Uploads require an
operator-configured private parser origin/token; missing configuration returns
503 before creating a document/job. The worker authenticates to the fixed
origin, inherits no HTTP proxy, follows no redirects, and validates bounded
text/page/warning output. Unavailable parsing remains retryable; invalid PDFs
fail explicitly. Text/DOCX/HTML retain their bounded loaders.

The private parser daemon supervises one disposable child with a ten-second
deadline. Child environment contains no application credentials. The separate
Linux container enforces 512MiB memory with no additional swap, one CPU, 32 PIDs,
nonroot UID/GID 65532, read-only root, dropped capabilities and no-new-privileges.
It has no host/application volume or Docker socket and joins only an internal
parser network. `GOMEMLIMIT` is only a soft hint; Docker supplies the hard bound.

Actual binary tests verified text/page extraction, malformed input, authentication
and killing a hung child with slot reuse. The complete real PG/MinIO/Milvus
document pipeline passed with PDF upload → parser → chunks/embeddings → retrieval
with page citation, alongside the existing retry/migration/delete cases
(race package 10.150s).

A fixed 783,342-byte PDF contains a compressed 768MiB text literal. In the
separate constrained container it triggered a Docker `oom` event, after which
the same daemon served a valid PDF. Its start time/restart count remained
unchanged; Docker's `OOMKilled` flag records the cgroup event even while the
parent remains healthy. The final race test passed in 2.142s, with fault and
valid recovery observed in 1.102s. The fixture's small outer gzip is committed
under loader testdata; its inner expansion is exercised only in that container.

An initial race attempt spent its deadline generating the large fixture and
failed. The fixed fixture removes that setup cost without reducing the PDF sent
to the parser. Earlier failed fixture/configuration iterations remain recorded
in the local validation ledger; none is counted as a successful acceptance.

## Configuration and reproduction

`backend/deployments/docker-compose.docparser.yaml` is the isolated deployment
component. Set `NX_PDF_PARSER_TOKEN` to a private token of at least 32 characters.
Attach API/document worker to its private network and set
`NX_PDF_PARSER__ENDPOINT=http://docparser:8095` and `NX_PDF_PARSER__TOKEN` to the
same token. Do not publish the parser or diagnostics through public ingress.
OCR is not supplied; image-only PDFs return an explicit failure.

The acceptance Compose uses a public test-only token and a separate internal
network. `scripts/verify-p3.ps1` joins both acceptance networks. It runs the PDF
resource-fault package after the other infrastructure packages to avoid faulting
the parser while another package tests a valid document pipeline. Diagnostics
can be enabled per process using `NX_OBSERVABILITY__DIAGNOSTICS_ADDR`; the
capacity runner must keep those listeners private.
