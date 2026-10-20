package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/mcp"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type MCPService struct {
	svcs     *Services
	registry tool.Registry
}

func NewMCPService(svcs *Services) *MCPService { return &MCPService{svcs: svcs} }
func (s *MCPService) SetRegistry(reg tool.Registry) {
	s.registry = reg
	if dynamic, ok := reg.(tool.DynamicRegistry); ok && s.svcs.Cfg.MCP.Enabled {
		dynamic.RegisterPrefix("mcp_", func(code string) (tool.BuiltinHandler, bool) {
			if len(code) > 64 || !mcp.ValidToolName(code) {
				return nil, false
			}
			return func(ctx context.Context, args map[string]any, _ string) (string, error) {
				return s.invoke(ctx, code, args)
			}, true
		})
	}
}
func (s *MCPService) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.svcs.Repos.DB.WithContext(ctx).Transaction(fn)
}
func (s *MCPService) actor(ctx context.Context) (*model.User, error) {
	return s.svcs.activeUser(ctx)
}
func (s *MCPService) admin(ctx context.Context) (*model.User, error) {
	return s.svcs.activeAdmin(ctx)
}
func lockedMCP(tx *gorm.DB, id int64) (*model.MCPServer, error) {
	var srv model.MCPServer
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&srv, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errcode.ErrToolNotFound.WithMsg("MCP服务端不存在")
	}
	return &srv, err
}
func (s *MCPService) get(ctx context.Context, id int64) (*model.MCPServer, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv, err := s.svcs.Repos.MCP.Get(ctx, id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errcode.ErrToolNotFound.WithMsg("MCP服务端不存在")
	}
	return srv, err
}
func canonicalMCPUsers(raw []byte) (datatypes.JSON, error) {
	var ids []int64
	if len(raw) > 4096 || len(raw) > 0 && json.Unmarshal(raw, &ids) != nil || len(ids) > 128 {
		return nil, errcode.ErrInvalidParam.WithMsg("MCP授权用户列表无效")
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	unique := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id < 1 {
			return nil, errcode.ErrInvalidParam
		}
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	result, _ := json.Marshal(unique)
	return datatypes.JSON(result), nil
}
func (s *MCPService) validate(srv *model.MCPServer) error {
	if srv.Name == "" || len(srv.Name) > 64 || !utf8.ValidString(srv.Name) || len(srv.URL) > 512 {
		return errcode.ErrInvalidParam.WithMsg("MCP名称或地址无效")
	}
	if srv.Transport != "streamable_http" {
		return errcode.ErrNotImplement.WithMsg("仅支持Streamable HTTP传输")
	}
	endpoint, err := url.Parse(srv.URL)
	if err != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errcode.ErrInvalidParam.WithMsg("MCP地址不能包含凭据、查询参数或片段")
	}
	if err := (security.OutboundPolicy{PrivateOrigins: s.svcs.Cfg.Outbound.PrivateOrigins}).Validate(srv.URL); err != nil {
		return errcode.ErrDomainNotAllow.WithMsg("MCP目标未获出站许可；内部服务需要配置准确的origin").WithCause(err)
	}
	return nil
}
func mcpHasUnsupportedInput(srv *model.MCPServer) bool {
	return srv.Command != "" || len(srv.Env) > 0 && string(srv.Env) != "{}" && string(srv.Env) != "null" || len(srv.Args) > 0 && string(srv.Args) != "[]" && string(srv.Args) != "null"
}
func mcpAuthority(srv *model.MCPServer, envelope mcpCredentialEnvelope) string {
	raw, _ := json.Marshal(struct {
		URL, Transport string
		Headers        map[string]string
	}{srv.URL, srv.Transport, envelope.Headers})
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}
func (s *MCPService) Create(ctx context.Context, srv *model.MCPServer) error {
	if _, err := s.admin(ctx); err != nil {
		return err
	}
	if srv == nil {
		return errcode.ErrInvalidParam
	}
	if mcpHasUnsupportedInput(srv) {
		return errcode.ErrNotImplement.WithMsg("HTTP MCP不执行本地命令或环境变量")
	}
	if srv.Transport == "" {
		srv.Transport = "streamable_http"
	}
	if err := s.validate(srv); err != nil {
		return err
	}
	users, err := canonicalMCPUsers(srv.AllowedUserIDs)
	if err != nil {
		return err
	}
	srv.AllowedUserIDs = users
	srv.ID = 0
	srv.ConfigVersion = 1
	srv.ToolsHash = ""
	srv.Status = "disconnected"
	srv.ToolsCache = datatypes.JSON("[]")
	if srv.Enabled == nil {
		value := true
		srv.Enabled = &value
	}
	envelope, err := s.credentials(srv)
	if err != nil {
		return err
	}
	if err = s.seal(srv, envelope); err != nil {
		return err
	}
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':mcp-config',0))").Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Model(&model.MCPServer{}).Count(&count).Error; err != nil {
			return err
		}
		if count >= 128 {
			return errcode.ErrInvalidParam.WithMsg("MCP服务端数量已达128个上限")
		}
		return tx.Create(srv).Error
	})
	if err == nil {
		*srv = *redactMCP(srv)
	}
	return err
}
func (s *MCPService) Update(ctx context.Context, input *model.MCPServer) error {
	if _, err := s.admin(ctx); err != nil {
		return err
	}
	if input == nil || input.ID < 1 || input.ConfigVersion < 1 {
		return errcode.ErrInvalidParam.WithMsg("需要配置版本")
	}
	if mcpHasUnsupportedInput(input) {
		return errcode.ErrNotImplement.WithMsg("HTTP MCP不执行本地命令或环境变量")
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		current, err := lockedMCP(tx, input.ID)
		if err != nil {
			return err
		}
		if current.ConfigVersion != input.ConfigVersion {
			return errcode.ErrRunConflict.WithMsg("MCP配置已变化，请刷新")
		}
		envelope, err := s.credentials(current)
		if err != nil {
			return err
		}
		before := mcpAuthority(current, envelope)
		if input.Name != "" {
			current.Name = input.Name
		}
		if input.URL != "" {
			current.URL = input.URL
		}
		if input.Transport != "" {
			current.Transport = input.Transport
		}
		if input.Enabled != nil {
			current.Enabled = input.Enabled
		}
		if len(input.AllowedUserIDs) > 0 {
			current.AllowedUserIDs, err = canonicalMCPUsers(input.AllowedUserIDs)
			if err != nil {
				return err
			}
		}
		if input.ClearHeaders {
			envelope.Headers = map[string]string{}
		} else if len(input.Headers) > 0 && strings.TrimSpace(string(input.Headers)) != "{}" {
			if len(input.Headers) > 16384 || json.Unmarshal(input.Headers, &envelope.Headers) != nil {
				return errcode.ErrInvalidParam.WithMsg("MCP鉴权头无效")
			}
		}
		if err = s.validate(current); err != nil {
			return err
		}
		current.ConfigVersion++
		updates := map[string]any{"mcp_config_version": current.ConfigVersion}
		if before != mcpAuthority(current, envelope) {
			current.Status = "disconnected"
			updates["binding_version"] = gorm.Expr("binding_version+1")
			updates["is_dangerous"] = true
			updates["replay_policy"] = "manual"
			updates["policy_reviewed_by"] = nil
			updates["policy_reviewed_at"] = nil
			updates["replay_evidence"] = ""
		}
		if err = tx.Model(&model.Tool{}).Where("mcp_server_id=? AND type=? AND status=1", current.ID, model.ToolTypeMCP).Updates(updates).Error; err != nil {
			return err
		}
		if err = s.seal(current, envelope); err != nil {
			return err
		}
		if err = tx.Save(current).Error; err != nil {
			return err
		}
		*input = *redactMCP(current)
		return nil
	})
}
func (s *MCPService) Delete(ctx context.Context, id int64) error {
	if _, err := s.admin(ctx); err != nil {
		return err
	}
	var codes []string
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		current, err := lockedMCP(tx, id)
		if err != nil {
			return err
		}
		if err = tx.Model(&model.Tool{}).Where("mcp_server_id=? AND type=? AND status=1", id, model.ToolTypeMCP).Limit(257).Pluck("code", &codes).Error; err != nil {
			return err
		}
		if len(codes) > 256 {
			return errcode.ErrInvalidParam.WithMsg("MCP活动工具超过删除上限")
		}
		if err = tx.Model(&model.Tool{}).Where("mcp_server_id=? AND type=?", id, model.ToolTypeMCP).Updates(map[string]any{"status": 0, "binding_version": gorm.Expr("binding_version+1"), "mcp_config_version": current.ConfigVersion + 1}).Error; err != nil {
			return err
		}
		if err = tx.Model(current).Updates(map[string]any{"enabled": false, "config_version": current.ConfigVersion + 1}).Error; err != nil {
			return err
		}
		return tx.Delete(current).Error
	})
	if err == nil {
		if dynamic, ok := s.registry.(interface{ Unregister(string) }); ok {
			for _, code := range codes {
				dynamic.Unregister(code)
			}
		}
	}
	return err
}
func (s *MCPService) List(ctx context.Context) ([]model.MCPServer, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var rows []model.MCPServer
	err := s.svcs.Repos.DB.WithContext(ctx).Select("id,name,transport,url,enabled,config_version,allowed_user_ids,status,last_health_at,created_at,updated_at,headers_present,(CASE WHEN jsonb_typeof(tools_cache)='array' THEN jsonb_array_length(tools_cache) ELSE 0 END) AS tool_count,(headers<>'{}'::jsonb OR env<>'{}'::jsonb OR args<>'[]'::jsonb OR command<>'') AS credentials_pending_migration").Order("id").Limit(1001).Find(&rows).Error
	for i := range rows {
		rows[i].ToolsCache = datatypes.JSON("[]")
	}
	if len(rows) > 1000 {
		return nil, fmt.Errorf("MCP configuration limit exceeded")
	}
	return rows, err
}
func (s *MCPService) Get(ctx context.Context, id int64) (*model.MCPServer, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	srv, err := s.get(ctx, id)
	if err != nil {
		return nil, err
	}
	return redactMCP(srv), nil
}
func (s *MCPService) protocolClient(srv *model.MCPServer) (*mcp.Client, error) {
	if !s.svcs.Cfg.MCP.Enabled || srv.Enabled == nil || !*srv.Enabled {
		return nil, errcode.ErrNotImplement.WithMsg("MCP执行已停用")
	}
	if err := s.validate(srv); err != nil {
		return nil, err
	}
	envelope, err := s.credentials(srv)
	if err != nil {
		return nil, err
	}
	return mcp.NewClient(srv.URL, envelope.Headers, mcp.WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: s.svcs.Cfg.Outbound.PrivateOrigins})), nil
}
