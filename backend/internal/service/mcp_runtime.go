package service

import (
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"time"
)

func (s *MCPService) allowed(actor *model.User, srv *model.MCPServer) bool {
	if !s.svcs.Cfg.MCP.Enabled || actor == nil || actor.Status != 1 || srv.Enabled == nil || !*srv.Enabled {
		return false
	}
	if actor.Role == "admin" {
		return true
	}
	var users []int64
	if json.Unmarshal(srv.AllowedUserIDs, &users) != nil {
		return false
	}
	for _, id := range users {
		if actor.ID == id {
			return true
		}
	}
	return false
}
func (s *MCPService) invoke(ctx context.Context, code string, args map[string]any) (string, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return "", err
	}
	bound, err := s.svcs.Repos.Tool.GetByCode(ctx, code)
	if err != nil || bound.Type != model.ToolTypeMCP || bound.Status != 1 || bound.MCPServerID == nil {
		return "", errcode.ErrToolNotFound
	}
	srv, err := s.get(ctx, *bound.MCPServerID)
	if err != nil {
		return "", err
	}
	if !s.allowed(actor, srv) {
		return "", errcode.ErrForbidden
	}
	if bound.MCPConfigVersion != srv.ConfigVersion {
		return "", errcode.ErrRunConflict.WithMsg("MCP工具配置版本已变化")
	}
	if !validMCPReview(bound) {
		return "", errcode.ErrRunConflict.WithMsg("MCP工具需要管理员审阅调用和重试策略")
	}
	if expected, ok := tool.ExpectedBindingVersion(ctx, code); ok && expected != bound.BindingVersion {
		return "", errcode.ErrRunConflict.WithMsg("MCP工具绑定已变化，需要重新授权")
	}
	if srv.EncryptedCredentials == "" {
		return "", errcode.ErrRunConflict.WithMsg("请先由管理员迁移MCP凭据并同步工具")
	}
	client, err := s.protocolClient(srv)
	if err != nil {
		return "", err
	}
	defer client.Close()
	return client.CallTool(ctx, bound.MCPName, args)
}

// AuthorizeTools batches the server checks and reads the actor once for this
// definition lookup. It never caches roles or grants across invocations.
func (s *MCPService) AuthorizeTools(ctx context.Context, tools []model.Tool) error {
	ids := map[int64]bool{}
	for i := range tools {
		item := tools[i]
		if item.Type == model.ToolTypeMCP {
			if item.MCPServerID == nil || item.MCPName == "" {
				return errcode.ErrToolNotFound
			}
			ids[*item.MCPServerID] = true
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if !s.svcs.Cfg.MCP.Enabled {
		return errcode.ErrToolNotFound.WithMsg("MCP执行已停用")
	}
	actor, err := s.actor(ctx)
	if err != nil {
		return err
	}
	keys := make([]int64, 0, len(ids))
	for id := range ids {
		keys = append(keys, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var servers []model.MCPServer
	if err = s.svcs.Repos.DB.WithContext(ctx).Select("id,url,enabled,config_version,allowed_user_ids").Where("id IN ?", keys).Find(&servers).Error; err != nil {
		return err
	}
	byID := map[int64]model.MCPServer{}
	for _, srv := range servers {
		byID[srv.ID] = srv
	}
	for i := range tools {
		item := tools[i]
		if item.Type != model.ToolTypeMCP {
			continue
		}
		srv, ok := byID[*item.MCPServerID]
		if !ok || !s.allowed(actor, &srv) {
			return errcode.ErrForbidden
		}
		if item.MCPConfigVersion != srv.ConfigVersion {
			return errcode.ErrRunConflict.WithMsg("MCP配置版本已变化")
		}
		if !validMCPReview(&item) {
			return errcode.ErrRunConflict.WithMsg("MCP工具需要管理员审阅调用和重试策略")
		}
		tools[i].SourceIdentity = mcpSourceIdentity(&srv, &item)
	}
	return nil
}

func (s *MCPService) RestoreRegistry(ctx context.Context) error {
	if !s.svcs.Cfg.MCP.Enabled || s.registry == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var rows []struct{ Code string }
	err := s.svcs.Repos.DB.WithContext(ctx).Table("tools t").Select("t.code").Joins("JOIN mcp_servers s ON s.id=t.mcp_server_id").Where("t.type=? AND t.status=1 AND s.enabled AND s.archived_at IS NULL", model.ToolTypeMCP).Order("t.id").Limit(10001).Scan(&rows).Error
	if err != nil {
		return err
	}
	if len(rows) > 10000 {
		return errcode.ErrInvalidParam.WithMsg("MCP活动工具超过10000个上限")
	}
	for _, row := range rows {
		s.registerHandler(row.Code)
	}
	return nil
}
func (s *MCPService) registerHandler(code string) {
	if s.registry == nil {
		return
	}
	s.registry.Register(code, func(ctx context.Context, args map[string]any, _ string) (string, error) {
		return s.invoke(ctx, code, args)
	})
}
func (s *MCPService) CallTool(ctx context.Context, id int64, name string, args map[string]any) (string, error) {
	if _, err := s.admin(ctx); err != nil {
		return "", err
	}
	var row model.Tool
	if err := s.svcs.Repos.DB.WithContext(ctx).Where("mcp_server_id=? AND mcp_name=? AND type=? AND status=1", id, name, model.ToolTypeMCP).First(&row).Error; err != nil {
		return "", errcode.ErrToolNotFound
	}
	if row.IsDangerous || row.ReplayPolicy != "readonly" || !validMCPReview(&row) {
		return "", errcode.ErrForbidden.WithMsg("调试仅允许已审阅的只读工具；写操作请通过工作流审批执行")
	}
	return s.invoke(tool.WithBindingVersion(ctx, row.Code, row.BindingVersion), row.Code, args)
}
func (s *MCPService) markStatus(ctx context.Context, srv *model.MCPServer, status string) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = s.svcs.Repos.DB.WithContext(ctx).Model(&model.MCPServer{}).Where("id=? AND config_version=?", srv.ID, srv.ConfigVersion).Updates(map[string]any{"status": status, "last_health_at": time.Now()}).Error
}
func (s *MCPService) HealthCheck(ctx context.Context, id int64) error {
	if _, err := s.admin(ctx); err != nil {
		return err
	}
	srv, err := s.get(ctx, id)
	if err != nil {
		return err
	}
	client, err := s.protocolClient(srv)
	if err != nil {
		return err
	}
	defer client.Close()
	if err = client.Ping(ctx); err != nil {
		s.markStatus(ctx, srv, "error")
		return err
	}
	s.markStatus(ctx, srv, "connected")
	return nil
}
