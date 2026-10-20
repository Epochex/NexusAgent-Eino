DROP INDEX idx_outbox_dedup_key;
ALTER TABLE execution_outbox DROP COLUMN dedup_key;
DROP TABLE document_jobs;
DROP INDEX idx_chunk_ready_lookup;
DROP INDEX idx_document_version_seq;
DROP INDEX idx_document_content;
ALTER TABLE document_chunks DROP COLUMN index_version,DROP COLUMN embedding_version;
ALTER TABLE documents DROP COLUMN content_hash,DROP COLUMN index_version,DROP COLUMN ready_version,DROP COLUMN embedding_version,DROP COLUMN parse_warnings,DROP COLUMN archived_at;
ALTER TABLE knowledge_bases DROP COLUMN embedding_version,DROP COLUMN embedding_dimension,DROP COLUMN embedding_snapshot,DROP COLUMN archived_at;
ALTER TABLE model_configs DROP COLUMN embedding_dimension;
-- needs_reindex remains explicit: rollback cannot certify old vector identity.
