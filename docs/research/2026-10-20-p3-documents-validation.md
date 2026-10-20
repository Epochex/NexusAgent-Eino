# P3 document ingestion checkpoint — 2026-10-20

This report covers P3 Task1 only. Retrieval/memory evaluation, MCP/A2A completion, whole-product acceptance and P4 capacity/release work are separate remaining tasks. Main still contains the accepted P0/P1 baseline until final whole-branch integration.

## Confirmed failures and fixes

| Observed failure | Resulting behavior |
| --- | --- |
| Recursive splitter could fail to make progress, reorder text and exceed limits | Bounded rune windows with ordered, exact overlap and global chunk sequence |
| HTML/DOCX extraction lost structural/text fidelity; parse omissions were hidden | Bounded tokenizer/XML extraction and visible warnings; PDF limits and omissions disclosed |
| Retried chunks duplicated/conflicted; stale workers could write | Fenced immutable generation writes, content verification and stable IDs |
| Live model edits could mix one index's embedding space | One descriptor per attempt; KB model/dimension/schema fingerprint and immutable settings |
| Dense metadata could bypass published PG chunks | Every fused result rechecked against PG ready authority |
| Nil object storage accepted an unrecoverable document | HTTP503 before rows are created |
| Legacy Redis consumer did not claim new durable jobs | PG polling, bounded slots, renewable leases, retries and generation fencing |
| Real Milvus replacement yielded logical Count=2; wrong dimension accepted | Real Upsert, exact schema/dimension validation and strong read consistency |
| Missing RAG accepted KB; failed collection initialization left active status | No nil-dependency creation; initializing/failed/active states with explicit retry |
| HTTP finalization loss could strand a confirmed source | Delayed awaiting_source recovery verifies the confirmed S3 object |
| Failed missing-source reservation could not be repaired by reupload | Explicit same-source reupload repairs bytes and starts a new retry epoch, retaining attempts |
| Cleanup could race a late PUT/index write | Shared PG session source-IO lock across upload, indexing and cleanup |

## Real infrastructure evidence

The pinned acceptance Compose stack runs PG16, Redis7.2.4, MinIO and Milvus2.5.6 with its own etcd/object store. Loopback endpoints and public test credentials are used. Fixtures isolate PG schemas, MinIO buckets and Milvus collections. Application verification runs in Linux Go1.26 with4CPU/4GiB; infrastructure has separate limits, including Milvus2CPU/4GiB. This is a correctness resource bound, not a throughput claim.

The actual embedding HTTP peer is deterministic. It pins a selected3-dimensional model while platform default dimension is7; editing live model settings does not change the KB model. A forced failure after the first successful vector batch leaves retrieval empty. Retry retains both chunk IDs and exactly2 logical vectors; completed retrieval returns authoritative content/filename.

The same test runs the document worker without Redis. It verifies document vector/object deletion, legacy-source replacement and archival, recovery after confirmed PUT with missed enqueue, repair after a missing-source failure, and whole-KB cleanup. Independent PG/Redis tests exercise outbox delivery and notification outage recovery.

The real Milvus contract test independently replaces a vector under the same ID, immediately queries the replacement, checks Count=1, and rejects an incompatible existing dimension.

## Validation

- Windows: full Go unit suite and vet passed; frontend TypeScript check and production build passed.
- Linux4CPU/4GiB: vet, full Go race suite, actual PG/Redis/MinIO/Milvus race integration and all command builds passed. Integration durations: execution77.641s, service35.322s, API7.275s, repo3.333s, ingestion19.543s, vector2.936s.
- Immutable renewal identity was then added so GORM completion cannot mutate fields read by the heartbeat. Focused service/ingestion race and all command builds passed (26.358s/12.051s).
- The final explicit reupload repair regression was observed failing on the original behavior; post-fix focused matrix result is recorded in the task ledger.
- CI now includes ingestion/repo PG checks and a separate real MinIO/Milvus acceptance job. Remote results must be checked after push.

## Limits

Real-model answer quality and cost have not been tested because no model service is configured. Fixed-peer evidence establishes application behavior only. The PDF parser remains in-process; bounded files/pages/output are not full malicious-input memory isolation. KB archive retains empty collection metadata for explicit retention. Frontend controls compile; actual browser validation of the whole platform is still P3 Task4.
