package service

import (
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
	"unicode/utf8"
)

type MCPToolPolicyInput struct {
	BindingVersion int64  `json:"binding_version"`
	IsDangerous    bool   `json:"is_dangerous"`
	ReplayPolicy   string `json:"replay_policy"`
	Evidence       string `json:"evidence"`
}

func (s *MCPService) UpdateToolPolicy(ctx context.Context, serverID, toolID int64, in *MCPToolPolicyInput) (*model.Tool, error) {
	actor, err := s.admin(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil || in.BindingVersion < 1 || len(in.Evidence) > 512 || !utf8.ValidString(in.Evidence) {
		return nil, errcode.ErrInvalidParam
	}
	if in.ReplayPolicy != "manual" && in.ReplayPolicy != "readonly" && in.ReplayPolicy != "idempotent" {
		return nil, errcode.ErrInvalidParam.WithMsg("重试策略无效")
	}
	if !in.IsDangerous && in.ReplayPolicy != "readonly" {
		return nil, errcode.ErrInvalidParam.WithMsg("只有经过审阅的只读工具可在普通对话中调用")
	}
	evidence := strings.TrimSpace(in.Evidence)
	if in.ReplayPolicy != "manual" && len(evidence) < 8 {
		return nil, errcode.ErrInvalidParam.WithMsg("请记录只读或远端幂等保证的审阅依据（8..512字节）")
	}
	var result model.Tool
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		if _, err := lockedMCP(tx, serverID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND mcp_server_id=? AND type=? AND status=1", toolID, serverID, model.ToolTypeMCP).First(&result).Error; err != nil {
			return errcode.ErrToolNotFound
		}
		if result.BindingVersion != in.BindingVersion {
			return errcode.ErrRunConflict.WithMsg("工具绑定已变化，请刷新后审阅")
		}
		if err := tx.Model(&result).Updates(map[string]any{"is_dangerous": in.IsDangerous, "replay_policy": in.ReplayPolicy, "binding_version": gorm.Expr("binding_version+1"), "policy_reviewed_by": actor.ID, "policy_reviewed_at": time.Now().UTC(), "replay_evidence": evidence}).Error; err != nil {
			return err
		}
		return tx.First(&result, result.ID).Error
	})
	return &result, err
}
func (s *MCPService) Tools(ctx context.Context, id int64, q pagination.Query) ([]model.Tool, int64, error) {
	if _, err := s.admin(ctx); err != nil {
		return nil, 0, err
	}
	if _, err := s.get(ctx, id); err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	query := s.svcs.Repos.DB.WithContext(ctx).Model(&model.Tool{}).Where("mcp_server_id=? AND type=? AND status=1", id, model.ToolTypeMCP)
	var rows []model.Tool
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return nil, 0, err
	}
	err := query.Order("name,id").Offset(q.Offset()).Limit(q.Limit()).Find(&rows).Error
	return rows, count, err
}
func validMCPReview(item *model.Tool) bool {
	if item.ReplayPolicy == "manual" {
		return item.IsDangerous
	}
	return (item.ReplayPolicy == "readonly" || item.ReplayPolicy == "idempotent") && item.PolicyReviewedBy != nil && item.PolicyReviewedAt != nil && len(item.ReplayEvidence) >= 8 && (item.IsDangerous || item.ReplayPolicy == "readonly")
}
func mcpSourceIdentity(srv *model.MCPServer, item *model.Tool) string {
	raw, _ := json.Marshal(struct {
		Kind           string     `json:"kind"`
		ServerID       int64      `json:"server_id"`
		ConfigVersion  int64      `json:"config_version"`
		BindingVersion int64      `json:"binding_version"`
		Endpoint       string     `json:"endpoint"`
		RemoteName     string     `json:"remote_name"`
		ReviewedBy     *int64     `json:"reviewed_by,omitempty"`
		ReviewedAt     *time.Time `json:"reviewed_at,omitempty"`
		Evidence       string     `json:"evidence,omitempty"`
	}{"mcp", srv.ID, srv.ConfigVersion, item.BindingVersion, srv.URL, item.MCPName, item.PolicyReviewedBy, item.PolicyReviewedAt, item.ReplayEvidence})
	return string(raw)
}
