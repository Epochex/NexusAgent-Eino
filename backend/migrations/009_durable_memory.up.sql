-- Versioned, durable memory metadata and reconciliation work.
CREATE TABLE memory_profiles(
 user_id BIGINT NOT NULL REFERENCES users(id),
 embedding_config_id BIGINT NOT NULL REFERENCES model_configs(id),
 embedding_version VARCHAR(64) NOT NULL,
 embedding_dimension INT NOT NULL CHECK(embedding_dimension BETWEEN 1 AND 16384),
 embedding_snapshot JSONB NOT NULL,
 active BOOLEAN NOT NULL DEFAULT TRUE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(user_id,embedding_version)
);
CREATE UNIQUE INDEX idx_memory_profile_active ON memory_profiles(user_id) WHERE active;
CREATE TABLE memory_profile_rebuilds(
 id BIGSERIAL PRIMARY KEY,
 owner_id BIGINT NOT NULL REFERENCES users(id),
 request_key VARCHAR(64) NOT NULL,
 embedding_version VARCHAR(64) NOT NULL,
 reindexed INT NOT NULL DEFAULT 0,
 merged INT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(owner_id,request_key)
);
ALTER TABLE memories ADD COLUMN revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
 ADD COLUMN indexed_revision BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN index_status VARCHAR(20) NOT NULL DEFAULT 'needs_reindex',
 ADD COLUMN embedding_version VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN content_hash VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN source_operation_key VARCHAR(64);
CREATE UNIQUE INDEX idx_memories_source_operation ON memories(source_operation_key) WHERE source_operation_key IS NOT NULL;
CREATE UNIQUE INDEX idx_memories_active_content ON memories(user_id,COALESCE(agent_id,0),content_hash) WHERE status=1 AND content_hash<>'';
CREATE INDEX idx_memories_published ON memories(user_id,embedding_version,id) WHERE status=1 AND index_status='ready';
CREATE TABLE memory_cursors(
 session_id UUID PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
 last_enqueued_message_id BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE memory_jobs(
 id BIGSERIAL PRIMARY KEY,
 kind VARCHAR(16) NOT NULL CHECK(kind IN ('extract','vector')),
 owner_id BIGINT NOT NULL REFERENCES users(id),
 agent_id BIGINT REFERENCES agents(id),
 session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
 through_message_id BIGINT NOT NULL DEFAULT 0,
 extractor_version VARCHAR(64) NOT NULL DEFAULT '',
 memory_id BIGINT REFERENCES memories(id),
 memory_revision BIGINT NOT NULL DEFAULT 0,
 vector_action VARCHAR(16) NOT NULL DEFAULT '',
 embedding_version VARCHAR(64) NOT NULL DEFAULT '',
 processor_version VARCHAR(64) NOT NULL,
 payload JSONB NOT NULL DEFAULT '{}',
 decision JSONB,
 facts JSONB,
 decision_hash VARCHAR(64) NOT NULL DEFAULT '',
 warnings JSONB NOT NULL DEFAULT '[]',
 parent_job_id BIGINT REFERENCES memory_jobs(id) ON DELETE SET NULL,
 rebuild_id BIGINT REFERENCES memory_profile_rebuilds(id),
 status VARCHAR(20) NOT NULL DEFAULT 'queued',
 generation BIGINT NOT NULL DEFAULT 0,
 attempts INT NOT NULL DEFAULT 0,
 epoch_attempts INT NOT NULL DEFAULT 0,
 retry_epoch INT NOT NULL DEFAULT 0,
 max_attempts INT NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 10),
 lease_owner VARCHAR(128) NOT NULL DEFAULT '',
 lease_until TIMESTAMPTZ,
 available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 deadline_at TIMESTAMPTZ NOT NULL DEFAULT NOW()+INTERVAL '24 hours',
 error VARCHAR(512) NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_memory_extraction_source ON memory_jobs(session_id,through_message_id,embedding_version,extractor_version) WHERE kind='extract';
CREATE UNIQUE INDEX idx_memory_vector_revision ON memory_jobs(memory_id,memory_revision) WHERE kind='vector';
CREATE INDEX idx_memory_queue ON memory_jobs(available_at,id) WHERE status IN ('queued','running','retry_wait');
CREATE INDEX idx_memory_jobs_owner ON memory_jobs(owner_id,id);
CREATE TABLE memory_operations(
 id BIGSERIAL PRIMARY KEY,
 job_id BIGINT NOT NULL REFERENCES memory_jobs(id) ON DELETE CASCADE,
 op_index INT NOT NULL,
 payload_hash VARCHAR(64) NOT NULL,
 op VARCHAR(16) NOT NULL,
 memory_id BIGINT REFERENCES memories(id),
 result_revision BIGINT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(job_id,op_index)
);
CREATE INDEX idx_messages_memory_cursor ON messages(session_id,id) WHERE role IN ('user','assistant') AND status=1;
