ALTER TABLE workflows ADD COLUMN archived_at TIMESTAMPTZ;
ALTER TABLE workflow_runs ADD COLUMN request_key VARCHAR(64), ADD COLUMN definition_release_id BIGINT;
CREATE UNIQUE INDEX idx_workflow_request_key ON workflow_runs(owner_id,request_key);
CREATE TABLE workflow_versions (
 id BIGSERIAL PRIMARY KEY,
 workflow_id BIGINT NOT NULL REFERENCES workflows(id),
 version INTEGER NOT NULL,
 definition_hash CHAR(64) NOT NULL,
 definition JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(workflow_id,definition_hash)
);
ALTER TABLE workflow_runs ADD CONSTRAINT fk_definition_release FOREIGN KEY(definition_release_id) REFERENCES workflow_versions(id);
ALTER TABLE execution_approvals ADD COLUMN comment TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_workflow_live_deadline ON workflow_runs(deadline_at,id) WHERE definition IS NOT NULL AND status IN ('queued','running','retry_wait','waiting_approval','needs_attention');
