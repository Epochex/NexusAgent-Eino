DROP INDEX IF EXISTS idx_documents_published_scope;
DROP INDEX IF EXISTS idx_chunks_content_trgm;
-- Retain the shared extension and structured audit metadata on rollback.
