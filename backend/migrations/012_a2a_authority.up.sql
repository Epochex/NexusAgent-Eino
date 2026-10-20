ALTER TABLE a2a_agents
 ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN config_version BIGINT NOT NULL DEFAULT 1 CHECK(config_version>0),
 ADD COLUMN encrypted_credentials TEXT NOT NULL DEFAULT '',
 ADD COLUMN headers_present BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN pinned_endpoint VARCHAR(512) NOT NULL DEFAULT '',
 ADD COLUMN card_hash VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN archived_at TIMESTAMPTZ;
UPDATE a2a_agents SET status='requires_upgrade';
CREATE INDEX idx_a2a_active_agents ON a2a_agents(id) WHERE enabled AND archived_at IS NULL;

CREATE TABLE a2a_tasks (
 id VARCHAR(128) PRIMARY KEY,
 owner_id BIGINT NOT NULL REFERENCES users(id),
 context_id VARCHAR(128) NOT NULL,
 state VARCHAR(32) NOT NULL,
 version BIGINT NOT NULL DEFAULT 1 CHECK(version>0),
 task JSONB NOT NULL CHECK(jsonb_typeof(task)='object' AND octet_length(task::text)<=262144),
 deadline_at TIMESTAMPTZ NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_a2a_tasks_owner_cursor ON a2a_tasks(owner_id,updated_at DESC,id DESC);
CREATE INDEX idx_a2a_tasks_expiry ON a2a_tasks(deadline_at)
 WHERE state IN ('TASK_STATE_SUBMITTED','TASK_STATE_WORKING','TASK_STATE_INPUT_REQUIRED','TASK_STATE_AUTH_REQUIRED');

CREATE TABLE a2a_delegations (
 id VARCHAR(64) PRIMARY KEY,
 owner_id BIGINT NOT NULL REFERENCES users(id),
 agent_id BIGINT NOT NULL REFERENCES a2a_agents(id),
 request_key VARCHAR(128) NOT NULL,
 input_hash VARCHAR(64) NOT NULL,
 config_version BIGINT NOT NULL,
	 snapshot JSONB NOT NULL CHECK(jsonb_typeof(snapshot)='object' AND octet_length(snapshot::text)<=524288),
 status VARCHAR(32) NOT NULL DEFAULT 'reserved',
 version BIGINT NOT NULL DEFAULT 1 CHECK(version>0),
 poll_until TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
 cancel_until TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
 remote_task_id VARCHAR(256) NOT NULL DEFAULT '',
 output TEXT NOT NULL DEFAULT '' CHECK(octet_length(output)<=65536),
 error TEXT NOT NULL DEFAULT '',
 deadline_at TIMESTAMPTZ NOT NULL DEFAULT now()+interval '150 seconds',
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(owner_id,agent_id,request_key)
);
CREATE INDEX idx_a2a_delegation_owner ON a2a_delegations(owner_id,agent_id,created_at DESC,id);
CREATE INDEX idx_a2a_delegation_active ON a2a_delegations(deadline_at,owner_id)
 WHERE status IN ('reserved','dispatching','submitted','working','input_required','auth_required','unknown');

