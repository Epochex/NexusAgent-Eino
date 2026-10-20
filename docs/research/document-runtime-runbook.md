# Durable document runtime

New document ingestion uses PostgreSQL authority. Redis streams and pubsub carry retryable hints; a document-only worker continues without Redis. Upload HTTP202 means the verified source has been stored and its reserved job exists. It does not mean parsing or vectorization succeeded.

## Source and execution lifecycle

- Maximum source20MiB; upload request30s, DB transactions3s. Filename/type/UTF8 are checked. SHA256 and size are checked again by the worker.
- A transaction reserves document identity, object key, job and outbox before S3 PUT. Confirmed PUT advances the same job to queued. If HTTP dies after PUT, an awaiting_source job becomes eligible after30s and independently verifies the object.
- Same KB/filename/content hash deduplicates the upload. Separate document identities have separate object keys.
- PostgreSQL jobs use SKIP LOCKED, a generation fence, renewable15s leases and a5min per-attempt limit. Default3 total attempts per retry epoch; bounded exponential backoff. Lost executors consume the same finite budget. Explicit retry resets the epoch but retains cumulative attempts.
- Default4 document slots; accepted range1..16. Source IO locks allow at most8 holders per process and at most half the configured PG connection pool; pool size must be at least2. Upload, indexing and deletion of one document serialize remote writes and cleanup through the same PG session advisory lock. A process crash releases its session lock.
- Partial chunks/vectors stay invisible. Ready publication requires completed embedding/upsert and a fenced transaction verifying actual chunk count, token total and contiguous global sequence. Only the ready generation in its KB embedding space is retrieved.
- Schema/model/dimension are pinned by the KB fingerprint. Credential rotation is allowed only for the same enabled provider origin. Changing a model setting does not silently change an existing KB vector space.
- Milvus stores IDs, scope/version metadata and vectors; source content remains in PostgreSQL. Real Upsert replaces a stable chunk ID. Strong consistency supports immediate reads after publication.

## Management

- POST /api/v1/kbs/:id/initialize retries collection initialization, or explicitly pins a legacy KB's selected model. Missing dependencies do not create a usable KB; failed collection initialization leaves a visible failed state.
- POST /api/v1/kbs/:id/documents/:docID/retry retries a failed current-generation index job.
- POST /api/v1/kbs/:id/documents/:docID/reindex migrates a needs_reindex legacy document: read its original bytes, create a verified durable replacement, then archive and clean up the legacy identity. The returned document ID is the replacement ID.
- DELETE document immediately archives/fences the source and schedules remote cleanup. DELETE KB atomically archives all its sources, fences active work and schedules per-source cleanup. A delete job is done only after vector/object removal succeeds.
- Archive retains authority/history. Empty Milvus collection metadata is retained after KB cleanup; dropping old collections is an explicit retention operation, not part of the per-source delete completion claim.
- The UI shows failure/retry state and parser warnings, polls active processing, and exposes initialization/retry/legacy migration.

## Reproducible infrastructure validation

Start the pinned isolated stack from the repository root:

```powershell
docker compose -f backend/deployments/docker-compose.acceptance.yaml up -d --wait --wait-timeout 180
```

Use explicit test endpoints in the backend directory:

```powershell
$env:NX_TEST_DATABASE_DSN='postgres://postgres@127.0.0.1:55449/nexus_acceptance_test?sslmode=disable'
$env:NX_TEST_MILVUS_ADDR='127.0.0.1:59530'
$env:NX_TEST_MINIO_ADDR='127.0.0.1:59000'
go test -tags 'integration infrastructure' ./internal/vector ./internal/service -run 'TestMilvusUpsertAndDimensionContract|TestDocumentPipelineRealInfrastructure' -count=1 -timeout=180s
```

The test uses real PG, MinIO and Milvus with a deterministic HTTP embedding peer. It injects a second-batch failure, proves partial data is not retrieved, retries with stable chunk IDs/vector cardinality, checks authoritative citations, migrates a legacy source, recovers a confirmed PUT with lost HTTP finalization, and verifies document/KB cleanup. The document worker receives no Redis client. Separate real Redis tests verify transactional outbox delivery and notification failure recovery.

All infrastructure ports are loopback-only. The isolated acceptance stack uses public test credentials, never production keys. Test schemas/buckets/collections are unique to each fixture. Failed validation resources may be retained for diagnosis.

## Limits and evidence boundaries

Real-model quality and cost tests have not run: the user has no model configuration yet. Deterministic peer results validate the application protocol, state transitions, retry and storage behavior.

HTML/DOCX/plaintext extraction and PDF page/output limits are bounded; warnings disclose skipped/unextractable PDF pages, missing OCR and DOCX layout loss. The third-party PDF parser still runs in-process. These controls do not establish memory isolation against every malicious PDF; parser process isolation remains an explicit release concern.
