ALTER TABLE tools ADD COLUMN replay_policy VARCHAR(16) NOT NULL DEFAULT 'manual' CHECK(replay_policy IN ('readonly','idempotent','manual'));
UPDATE tools SET replay_policy='readonly' WHERE type='builtin' AND code IN ('calculator','datetime','web_search','http_client','kb_search');
