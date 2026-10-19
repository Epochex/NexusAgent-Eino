DROP INDEX IF EXISTS idx_agents_owner;
ALTER TABLE agents DROP COLUMN IF EXISTS owner_id;
