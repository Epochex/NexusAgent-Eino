ALTER TABLE agents
    ADD COLUMN owner_id BIGINT REFERENCES users(id) ON DELETE CASCADE;

UPDATE agents
SET owner_id = (SELECT id FROM users WHERE role = 'admin' ORDER BY id ASC LIMIT 1)
WHERE owner_id IS NULL
  AND EXISTS (SELECT 1 FROM users WHERE role = 'admin');

CREATE INDEX idx_agents_owner ON agents(owner_id);
