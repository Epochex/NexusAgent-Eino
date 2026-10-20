package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

// AvailableTools applies runtime availability and current grants before both
// count and pagination. Execution still rechecks authority at the call boundary.
func (s *Services) AvailableTools(ctx context.Context, typ string, page pagination.Query) ([]model.Tool, int64, error) {
	actor, err := s.activeUser(ctx)
	if err != nil {
		return nil, 0, err
	}
	codes := []string{}
	if s.Registry != nil {
		codes = s.Registry.Codes()
	}
	_, dynamic := s.Registry.(tool.DynamicRegistry)
	grants, _ := json.Marshal([]int64{actor.ID})
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	query := s.Repos.DB.WithContext(ctx).Table("tools t").
		Joins("LEFT JOIN mcp_servers s ON s.id=t.mcp_server_id AND s.archived_at IS NULL").
		Where("t.status=1").
		Where(`(t.type<>? AND t.code IN ?) OR
		(t.type=? AND ? AND (t.code IN ? OR (? AND left(t.code,4)='mcp_'))
		 AND s.enabled AND s.encrypted_credentials<>'' AND s.transport='streamable_http'
		 AND t.mcp_name<>'' AND t.mcp_config_version=s.config_version
		 AND (? OR s.allowed_user_ids @> ?::jsonb)
		 AND ((t.replay_policy='manual' AND t.is_dangerous) OR
		      (t.replay_policy IN ('readonly','idempotent') AND t.policy_reviewed_by IS NOT NULL
		       AND t.policy_reviewed_at IS NOT NULL AND octet_length(t.replay_evidence)>=8
		       AND (t.is_dangerous OR t.replay_policy='readonly'))))`,
			model.ToolTypeMCP, codes, model.ToolTypeMCP, s.Cfg.MCP.Enabled, codes, dynamic, actor.Role == "admin", string(grants))
	if typ != "" {
		query = query.Where("t.type=?", typ)
	}
	if page.Keyword != "" {
		query = query.Where("t.name ILIKE ? OR t.code ILIKE ?", "%"+page.Keyword+"%", "%"+page.Keyword+"%")
	}
	var total int64
	if err = query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	rows := make([]model.Tool, 0)
	err = query.Select("t.id,t.code,t.name,t.type,t.description,t.parameters,t.is_dangerous,t.replay_policy,t.status,t.binding_version").Order("t.id").Offset(page.Offset()).Limit(page.Limit()).Find(&rows).Error
	return rows, total, err
}
