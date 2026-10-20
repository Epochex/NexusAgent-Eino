ALTER TABLE model_configs ADD COLUMN embedding_dimension INTEGER NOT NULL DEFAULT 0;
ALTER TABLE knowledge_bases
 ADD COLUMN embedding_version VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN embedding_dimension INTEGER NOT NULL DEFAULT 0,
 ADD COLUMN embedding_snapshot JSONB,
 ADD COLUMN archived_at TIMESTAMPTZ;
ALTER TABLE documents
 ADD COLUMN content_hash VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN index_version BIGINT NOT NULL DEFAULT 1,
 ADD COLUMN ready_version BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN embedding_version VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN parse_warnings JSONB NOT NULL DEFAULT '[]',
 ADD COLUMN archived_at TIMESTAMPTZ;
ALTER TABLE document_chunks
 ADD COLUMN index_version BIGINT NOT NULL DEFAULT 1,
 ADD COLUMN embedding_version VARCHAR(64) NOT NULL DEFAULT '';
-- Preserve legacy rows, including duplicate chunks; never silently assign them
-- the current vector-space identity or expose them as newly validated indexes.
UPDATE documents SET status='needs_reindex' WHERE status='ready';
CREATE UNIQUE INDEX idx_document_content ON documents(kb_id,filename,content_hash)
 WHERE content_hash<>'' AND archived_at IS NULL;
CREATE UNIQUE INDEX idx_document_version_seq ON document_chunks(document_id,index_version,seq)
 WHERE embedding_version<>'' AND index_version>0;
CREATE INDEX idx_chunk_ready_lookup ON document_chunks(kb_id,embedding_version,document_id,index_version);
CREATE TABLE document_jobs (
 id BIGSERIAL PRIMARY KEY,
 document_id BIGINT NOT NULL REFERENCES documents(id),
 owner_id BIGINT NOT NULL REFERENCES users(id),
 kb_id BIGINT NOT NULL REFERENCES knowledge_bases(id),
 index_version BIGINT NOT NULL,
 embedding_version VARCHAR(64) NOT NULL,
 processor_version VARCHAR(64) NOT NULL,
 kind VARCHAR(16) NOT NULL DEFAULT 'index' CHECK(kind IN ('index','delete')),
 status VARCHAR(24) NOT NULL DEFAULT 'queued',
 generation BIGINT NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0,
 max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 10),
 epoch_attempts INTEGER NOT NULL DEFAULT 0,
 retry_epoch INTEGER NOT NULL DEFAULT 0,
 lease_owner VARCHAR(128) NOT NULL DEFAULT '',
 lease_until TIMESTAMPTZ,
 available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 deadline_at TIMESTAMPTZ NOT NULL,
 error TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(document_id,index_version,kind)
);
CREATE INDEX idx_document_jobs_ready ON document_jobs(available_at,id)
 WHERE status IN ('queued','awaiting_source','running','retry_wait');
ALTER TABLE execution_outbox ADD COLUMN dedup_key VARCHAR(160);
CREATE UNIQUE INDEX idx_outbox_dedup_key ON execution_outbox(dedup_key);
