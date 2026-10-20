ALTER TABLE mcp_servers ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT TRUE,
 ADD COLUMN config_version BIGINT NOT NULL DEFAULT 1 CHECK(config_version>0),
 ADD COLUMN encrypted_credentials TEXT NOT NULL DEFAULT '',
 ADD COLUMN headers_present BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN allowed_user_ids JSONB NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(allowed_user_ids)='array'),
 ADD COLUMN tools_hash VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN archived_at TIMESTAMPTZ;
UPDATE mcp_servers SET headers_present=(jsonb_typeof(headers)='object' AND headers<>'{}'::jsonb);
ALTER TABLE tools ADD COLUMN mcp_name VARCHAR(128) NOT NULL DEFAULT '',
 ADD COLUMN mcp_config_version BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN binding_version BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN policy_reviewed_by BIGINT REFERENCES users(id),
 ADD COLUMN policy_reviewed_at TIMESTAMPTZ,
 ADD COLUMN replay_evidence VARCHAR(512) NOT NULL DEFAULT '';
UPDATE tools SET mcp_name=name,mcp_config_version=1,binding_version=1,
 is_dangerous=TRUE,replay_policy='manual' WHERE type='mcp';
CREATE UNIQUE INDEX idx_mcp_tool_identity ON tools(mcp_server_id,mcp_name) WHERE type='mcp';
CREATE INDEX idx_mcp_active_servers ON mcp_servers(id) WHERE enabled AND archived_at IS NULL;
CREATE INDEX idx_mcp_active_tools ON tools(mcp_server_id,id) WHERE type='mcp' AND status=1;
-- Credentials cannot be encrypted by SQL without the application's master key.
-- Upgrade them through the explicit credential migration/admin sync action;
-- management responses never expose the legacy JSON fields.
