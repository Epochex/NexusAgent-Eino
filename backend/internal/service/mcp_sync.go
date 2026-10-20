package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"time"
)

func mcpToolCode(serverID int64, name string) string {
	code := fmt.Sprintf("mcp_%d_%s", serverID, name)
	if len(code) <= 64 {
		return code
	}
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("mcp_%d_%x", serverID, sum[:16])
}
func (s *MCPService) SyncTools(ctx context.Context, id int64) ([]model.Tool, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, err
	}
	srv, err := s.get(ctx, id)
	if err != nil {
		return nil, err
	}
	if srv.EncryptedCredentials == "" || redactMCP(srv).CredentialsPendingMigration {
		if err = s.MigrateCredentials(ctx, id); err != nil {
			return nil, err
		}
		srv, err = s.get(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	client, err := s.protocolClient(srv)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	defs, err := client.ListTools(ctx)
	if err != nil {
		s.markStatus(ctx, srv, "error")
		return nil, err
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	for _, def := range defs {
		if err := einoadapter.ValidateToolSchema(def.InputSchema); err != nil {
			return nil, errcode.ErrInvalidParam.WithMsg("远端工具Schema不在当前运行时支持范围").WithCause(err)
		}
	}
	cache, err := json.Marshal(defs)
	if err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(cache))
	if _, err = s.admin(ctx); err != nil {
		return nil, err
	}
	var result []model.Tool
	var removed []string
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		current, err := lockedMCP(tx, id)
		if err != nil {
			return err
		}
		if current.ConfigVersion != srv.ConfigVersion || current.Enabled == nil || !*current.Enabled {
			return errcode.ErrRunConflict.WithMsg("MCP配置已变化，请重新同步")
		}
		if err = tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':mcp-catalogue',0))").Error; err != nil {
			return err
		}
		var active int64
		if err = tx.Model(&model.Tool{}).Where("type=? AND status=1 AND mcp_server_id<>?", model.ToolTypeMCP, id).Count(&active).Error; err != nil {
			return err
		}
		if active+int64(len(defs)) > 10000 {
			return errcode.ErrInvalidParam.WithMsg("MCP活动工具超过10000个上限")
		}
		var previous []model.Tool
		names := make([]string, 0, len(defs))
		for _, def := range defs {
			names = append(names, def.Name)
		}
		if err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("mcp_server_id=? AND type=? AND (status=1 OR mcp_name IN ?)", id, model.ToolTypeMCP, names).Limit(513).Find(&previous).Error; err != nil {
			return err
		}
		if len(previous) > 512 {
			return errcode.ErrInvalidParam.WithMsg("MCP旧工具记录超过同步上限，需要管理员迁移")
		}
		if current.ToolsHash != hash {
			current.ConfigVersion++
			byName := map[string]model.Tool{}
			for _, item := range previous {
				byName[item.MCPName] = item
			}
			keep := map[int64]bool{}
			for _, def := range defs {
				params, err := json.Marshal(def.InputSchema)
				if err != nil {
					return err
				}
				old, exists := byName[def.Name]
				if exists {
					var oldParams any
					if json.Unmarshal(old.Parameters, &oldParams) != nil {
						return fmt.Errorf("stored MCP schema is invalid")
					}
					canonical, _ := json.Marshal(oldParams)
					updates := map[string]any{"name": def.Name, "description": def.Description, "parameters": datatypes.JSON(params), "status": 1, "mcp_config_version": current.ConfigVersion}
					if string(canonical) != string(params) || old.Description != def.Description || old.Status != 1 {
						updates["binding_version"] = gorm.Expr("binding_version+1")
						updates["is_dangerous"] = true
						updates["replay_policy"] = "manual"
						updates["policy_reviewed_by"] = nil
						updates["policy_reviewed_at"] = nil
						updates["replay_evidence"] = ""
					}
					if err = tx.Model(&old).Updates(updates).Error; err != nil {
						return err
					}
					keep[old.ID] = true
				} else {
					serverID := id
					row := &model.Tool{Code: mcpToolCode(id, def.Name), Name: def.Name, MCPName: def.Name, Type: model.ToolTypeMCP, Description: def.Description, Parameters: datatypes.JSON(params), MCPServerID: &serverID, TimeoutMs: 30000, Status: 1, IsDangerous: true, ReplayPolicy: "manual", MCPConfigVersion: current.ConfigVersion, BindingVersion: 1}
					if err = tx.Create(row).Error; err != nil {
						return err
					}
					keep[row.ID] = true
				}
			}
			for _, old := range previous {
				if !keep[old.ID] {
					removed = append(removed, old.Code)
					if err = tx.Model(&old).Updates(map[string]any{"status": 0, "binding_version": gorm.Expr("binding_version+1"), "mcp_config_version": current.ConfigVersion}).Error; err != nil {
						return err
					}
				}
			}
		}
		now := time.Now()
		current.Status = "connected"
		current.LastHealthAt = &now
		current.ToolsHash = hash
		current.ToolsCache = datatypes.JSON(cache)
		if err = tx.Save(current).Error; err != nil {
			return err
		}
		return tx.Where("mcp_server_id=? AND type=? AND status=1", id, model.ToolTypeMCP).Order("name,id").Find(&result).Error
	})
	if err != nil {
		return nil, err
	}
	if dynamic, ok := s.registry.(interface{ Unregister(string) }); ok {
		for _, code := range removed {
			dynamic.Unregister(code)
		}
	}
	for _, item := range result {
		s.registerHandler(item.Code)
	}
	return result, nil
}
