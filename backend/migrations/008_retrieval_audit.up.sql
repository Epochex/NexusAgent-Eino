-- Serialize shared extension installation across isolated test schemas too.
SELECT pg_advisory_xact_lock(hashtextextended('nexus:shared-extensions',0));
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
DO $$
DECLARE extension_schema TEXT;
BEGIN
 SELECT namespace.nspname INTO extension_schema FROM pg_extension extension
 JOIN pg_namespace namespace ON namespace.oid=extension.extnamespace WHERE extension.extname='pg_trgm';
 EXECUTE format('CREATE INDEX IF NOT EXISTS idx_chunks_content_trgm ON document_chunks USING gin(content %I.gin_trgm_ops) WHERE status=1',extension_schema);
END $$;
CREATE INDEX IF NOT EXISTS idx_documents_published_scope ON documents(kb_id,embedding_version,id) WHERE archived_at IS NULL AND status='ready';
-- Older writers stored a bare array although the UI expects metadata.agent_trace.
UPDATE messages SET metadata=jsonb_build_object('agent_trace',metadata) WHERE jsonb_typeof(metadata)='array';
