-- Fixed, owner-scoped reports replace arbitrary SQL expressions and functions.
-- Existing frozen SQL workflows fail schema/authority validation and must be edited.
UPDATE tools SET description='查询当前用户的运行、待审批或用量报表；不接受任意SQL',
 parameters='{"type":"object","properties":{"report":{"type":"string","enum":["recent_runs","pending_approvals","usage_summary"]},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["report"],"additionalProperties":false}'::jsonb,
 replay_policy='readonly',is_dangerous=FALSE,updated_at=NOW()
 WHERE code='db_query' AND type='builtin';
UPDATE tools SET description='在当前登录用户独立的授权目录内读取或原子保存文本；不支持跨用户文件访问、符号链接或任意系统路径',updated_at=NOW()
 WHERE code='file_ops' AND type='builtin';
