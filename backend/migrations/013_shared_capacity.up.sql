CREATE TABLE IF NOT EXISTS execution_capacity_policies (
 scope VARCHAR(16) PRIMARY KEY CHECK(scope IN ('run','provider')),
 active_limit INTEGER NOT NULL CHECK(active_limit BETWEEN 1 AND 1024),
 owner_active_limit INTEGER NOT NULL CHECK(owner_active_limit BETWEEN 1 AND active_limit),
 queue_limit INTEGER NOT NULL CHECK(queue_limit BETWEEN 1 AND 100000),
 owner_queue_limit INTEGER NOT NULL CHECK(owner_queue_limit BETWEEN 1 AND queue_limit),
 lease_ms INTEGER NOT NULL DEFAULT 15000 CHECK(lease_ms BETWEEN 1000 AND 60000),
 version BIGINT NOT NULL DEFAULT 1 CHECK(version>0),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO execution_capacity_policies(scope,active_limit,owner_active_limit,queue_limit,owner_queue_limit)
 VALUES ('run',32,4,1000,100),('provider',16,4,1000,100) ON CONFLICT (scope) DO NOTHING;

CREATE TABLE IF NOT EXISTS execution_capacity_leases (
 id VARCHAR(64) PRIMARY KEY,
 scope VARCHAR(16) NOT NULL CHECK(scope IN ('run','provider')),
 resource VARCHAR(128) NOT NULL,
 owner_id BIGINT NOT NULL CHECK(owner_id>=0),
 expires_at TIMESTAMPTZ NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK(scope<>'run' OR (owner_id>0 AND resource=''))
);
CREATE INDEX IF NOT EXISTS idx_capacity_resource ON execution_capacity_leases(scope,resource,expires_at,owner_id);
CREATE INDEX IF NOT EXISTS idx_capacity_expiry ON execution_capacity_leases(expires_at);
CREATE TABLE IF NOT EXISTS execution_owner_fairness (
 owner_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 last_claimed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_active_owner ON workflow_runs(owner_id,lease_until)
 WHERE definition IS NOT NULL AND status='running';
CREATE INDEX IF NOT EXISTS idx_workflow_outstanding_owner ON workflow_runs(owner_id,deadline_at)
 WHERE definition IS NOT NULL AND status NOT IN ('succeeded','failed','canceled');

-- Existing workflow leases stay authoritative: transitions atomically stop
-- counting without a second ledger, trigger, or best-effort release callback.
CREATE OR REPLACE VIEW execution_active_capacity AS
 SELECT 'workflow'::text AS source,id::text AS token,owner_id,LEAST(lease_until,deadline_at) AS expires_at
 FROM workflow_runs WHERE definition IS NOT NULL AND status='running'
 AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()
 UNION ALL
 SELECT 'chat'::text AS source,id AS token,owner_id,expires_at
 FROM execution_capacity_leases WHERE scope='run' AND expires_at>clock_timestamp();
