-- Roll back only after draining durable runs and exporting their evidence.
DROP TABLE execution_outbox,execution_events,execution_attempts,execution_operations,execution_approvals,execution_checkpoints;
DROP INDEX idx_execution_ready;
DROP INDEX idx_execution_owner;
ALTER TABLE workflow_runs DROP COLUMN owner_id,DROP COLUMN definition,DROP COLUMN definition_hash,DROP COLUMN engine_version,DROP COLUMN graph_schema,DROP COLUMN state_version,DROP COLUMN generation,DROP COLUMN lease_owner,DROP COLUMN lease_until,DROP COLUMN deadline_at,DROP COLUMN available_at,DROP COLUMN pending_ids,DROP COLUMN next_seq;
