ALTER TABLE execution_approvals DROP COLUMN comment;
DROP INDEX idx_workflow_live_deadline;
ALTER TABLE workflow_runs DROP CONSTRAINT fk_definition_release;
DROP TABLE workflow_versions;
DROP INDEX idx_workflow_request_key;
ALTER TABLE workflow_runs DROP COLUMN request_key,DROP COLUMN definition_release_id;
ALTER TABLE workflows DROP COLUMN archived_at;
