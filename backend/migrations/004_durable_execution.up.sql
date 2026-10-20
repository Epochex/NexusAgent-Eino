-- Additive: historical runs remain readable; new runs use the durable authority.
ALTER TABLE workflow_runs
 ADD COLUMN owner_id BIGINT REFERENCES users(id),
 ADD COLUMN definition JSONB,
 ADD COLUMN definition_hash CHAR(64),
 ADD COLUMN engine_version VARCHAR(32) NOT NULL DEFAULT 'eino-v0.9.22',
 ADD COLUMN graph_schema INTEGER NOT NULL DEFAULT 1,
 ADD COLUMN state_version BIGINT NOT NULL DEFAULT 1,
 ADD COLUMN generation BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN lease_owner VARCHAR(128) NOT NULL DEFAULT '',
 ADD COLUMN lease_until TIMESTAMPTZ,
 ADD COLUMN deadline_at TIMESTAMPTZ NOT NULL DEFAULT (NOW()+INTERVAL '1 day'),
 ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 ADD COLUMN pending_ids JSONB NOT NULL DEFAULT '[]',
 ADD COLUMN next_seq BIGINT NOT NULL DEFAULT 0;
UPDATE workflow_runs r SET owner_id=w.creator_id FROM workflows w WHERE w.id=r.workflow_id;
CREATE INDEX idx_execution_ready ON workflow_runs(available_at,id) WHERE definition IS NOT NULL AND status IN ('queued','running','retry_wait');
CREATE INDEX idx_execution_owner ON workflow_runs(owner_id,id DESC);

CREATE TABLE execution_checkpoints (
 run_id BIGINT NOT NULL REFERENCES workflow_runs(id),
 checkpoint_id VARCHAR(512) NOT NULL,
 definition_hash CHAR(64) NOT NULL,
 engine_version VARCHAR(32) NOT NULL,
 graph_schema INTEGER NOT NULL,
 payload BYTEA NOT NULL CHECK (octet_length(payload)<=16777216),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(run_id,checkpoint_id)
);
CREATE TABLE execution_approvals (
 id BIGSERIAL PRIMARY KEY,
 run_id BIGINT NOT NULL REFERENCES workflow_runs(id),
 interrupt_id VARCHAR(256) NOT NULL,
 path VARCHAR(1024) NOT NULL,
 args_hash VARCHAR(64) NOT NULL,
 prompt TEXT NOT NULL,
 decision VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK(decision IN ('pending','approved','rejected')),
 decided_by BIGINT REFERENCES users(id),
 decided_at TIMESTAMPTZ,
 expires_at TIMESTAMPTZ NOT NULL,
 UNIQUE(run_id,interrupt_id)
);
CREATE TABLE execution_operations (
 id VARCHAR(64) PRIMARY KEY,
 run_id BIGINT NOT NULL REFERENCES workflow_runs(id),
 path VARCHAR(1024) NOT NULL,
 kind VARCHAR(32) NOT NULL,
 args_hash VARCHAR(64) NOT NULL,
 replay_policy VARCHAR(24) NOT NULL DEFAULT 'manual',
 status VARCHAR(24) NOT NULL,
 generation BIGINT NOT NULL,
 output JSONB,
 error TEXT NOT NULL DEFAULT '',
 attempt INTEGER NOT NULL DEFAULT 1,
 started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 finished_at TIMESTAMPTZ,
 UNIQUE(run_id,path)
);
CREATE TABLE execution_attempts (
 id BIGSERIAL PRIMARY KEY,
 operation_id VARCHAR(64) NOT NULL REFERENCES execution_operations(id),
 generation BIGINT NOT NULL,
 attempt INTEGER NOT NULL,
 status VARCHAR(24) NOT NULL,
 error TEXT NOT NULL DEFAULT '',
 started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 finished_at TIMESTAMPTZ,
 UNIQUE(operation_id,attempt)
);
CREATE TABLE execution_events (
 run_id BIGINT NOT NULL REFERENCES workflow_runs(id),
 seq BIGINT NOT NULL,
 type VARCHAR(64) NOT NULL,
 schema_version INTEGER NOT NULL DEFAULT 1,
 data JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(run_id,seq)
);
CREATE TABLE execution_outbox (
 id BIGSERIAL PRIMARY KEY,
 run_id BIGINT REFERENCES workflow_runs(id),
 seq BIGINT,
 topic VARCHAR(64) NOT NULL,
 payload JSONB NOT NULL,
 published_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(run_id,seq)
);
CREATE INDEX idx_execution_outbox_pending ON execution_outbox(id) WHERE published_at IS NULL;
