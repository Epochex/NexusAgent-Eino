package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type A2ADelegateInput struct {
	Text              string `json:"text"`
	RequestKey        string `json:"request_key"`
	ReturnImmediately bool   `json:"return_immediately"`
}
type SendTaskResult struct {
	Version    int64     `json:"version"`
	ID         string    `json:"id"`
	TaskID     string    `json:"task_id"`
	RequestKey string    `json:"request_key"`
	State      string    `json:"state"`
	Text       string    `json:"text"`
	Error      string    `json:"error,omitempty"`
	CanRefresh bool      `json:"can_refresh"`
	CanCancel  bool      `json:"can_cancel"`
	UpdatedAt  time.Time `json:"updated_at"`
}
type delegationSnapshot struct {
	BaseURL     string          `json:"base_url"`
	Endpoint    string          `json:"endpoint"`
	Card        json.RawMessage `json:"card"`
	Credentials string          `json:"encrypted_credentials"`
}
type delegationRecord struct {
	ID                                                         string
	OwnerID, AgentID, ConfigVersion, Version                   int64
	RequestKey, InputHash, Status, RemoteTaskID, Output, Error string
	Snapshot                                                   datatypes.JSON
	DeadlineAt, PollUntil, CancelUntil, CreatedAt, UpdatedAt   time.Time
}

func (delegationRecord) TableName() string { return "a2a_delegations" }
func delegationTerminal(state string) bool {
	return state == "completed" || state == "failed" || state == "canceled" || state == "rejected"
}
func delegationDTO(row *delegationRecord) *SendTaskResult {
	known := row.RemoteTaskID != ""
	return &SendTaskResult{Version: row.Version, ID: row.ID, TaskID: row.RemoteTaskID, RequestKey: row.RequestKey, State: row.Status, Text: row.Output, Error: row.Error, UpdatedAt: row.UpdatedAt, CanRefresh: known && !delegationTerminal(row.Status), CanCancel: known && !delegationTerminal(row.Status)}
}
func (s *A2AService) expireDelegations(ctx context.Context, owner int64) error {
	return s.svcs.Repos.DB.WithContext(ctx).Model(&delegationRecord{}).Where("owner_id=? AND status IN ? AND deadline_at<=now()", owner, []string{"reserved", "dispatching"}).Updates(map[string]any{"status": "unknown", "version": gorm.Expr("version+1"), "error": "委派进程或响应中断，远端结果未确认；请勿重复发送。", "updated_at": gorm.Expr("now()")}).Error
}
func (s *A2AService) ownedDelegation(ctx context.Context, owner int64, id string) (*delegationRecord, error) {
	if id == "" || len(id) > 64 {
		return nil, errcode.ErrRunNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := s.expireDelegations(ctx, owner); err != nil {
		return nil, err
	}
	var row delegationRecord
	if err := s.svcs.Repos.DB.WithContext(ctx).Where("id=? AND owner_id=?", id, owner).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errcode.ErrRunNotFound
		}
		return nil, err
	}
	return &row, nil
}

func (s *A2AService) Delegations(ctx context.Context, agentID int64, q pagination.Query) ([]SendTaskResult, int64, error) {
	actor, err := s.svcs.activeAdmin(ctx)
	if err != nil {
		return nil, 0, err
	}
	if agentID < 1 {
		return nil, 0, errcode.ErrInvalidParam
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = s.expireDelegations(ctx, actor.ID); err != nil {
		return nil, 0, err
	}
	query := s.svcs.Repos.DB.WithContext(ctx).Model(&delegationRecord{}).Where("owner_id=? AND agent_id=?", actor.ID, agentID)
	var count int64
	if err = query.Count(&count).Error; err != nil {
		return nil, 0, err
	}
	var rows []delegationRecord
	if err = query.Select("id,version,request_key,remote_task_id,status,output,error,updated_at").Order("created_at DESC,id").Offset(q.Offset()).Limit(q.Limit()).Find(&rows).Error; err != nil {
		return nil, 0, err
	}
	results := make([]SendTaskResult, 0, len(rows))
	for i := range rows {
		results = append(results, *delegationDTO(&rows[i]))
	}
	return results, count, nil
}
func (s *A2AService) Delegate(ctx context.Context, agentID int64, input *A2ADelegateInput) (*SendTaskResult, error) {
	actor, err := s.svcs.activeAdmin(ctx)
	if err != nil {
		return nil, err
	}
	if input == nil || strings.TrimSpace(input.Text) == "" || len(input.Text) > 32768 || !utf8.ValidString(input.Text) || strings.TrimSpace(input.RequestKey) == "" || len(input.RequestKey) > 128 || !utf8.ValidString(input.RequestKey) || strings.ContainsAny(input.RequestKey, "\r\n\x00") {
		return nil, errcode.ErrInvalidParam
	}
	identity, _ := json.Marshal(struct {
		Text      string
		Immediate bool
	}{input.Text, input.ReturnImmediately})
	hash := fmt.Sprintf("%x", sha256.Sum256(identity))
	lookup, stopLookup := context.WithTimeout(ctx, 2*time.Second)
	var existing delegationRecord
	lookupErr := s.svcs.Repos.DB.WithContext(lookup).Where("owner_id=? AND agent_id=? AND request_key=?", actor.ID, agentID, input.RequestKey).First(&existing).Error
	stopLookup()
	if lookupErr == nil {
		if existing.InputHash != hash {
			return nil, errcode.ErrRunConflict.WithMsg("请求号已用于不同委派内容")
		}
		current, err := s.ownedDelegation(ctx, actor.ID, existing.ID)
		if err != nil {
			return nil, err
		}
		return delegationDTO(current), nil
	}
	if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		return nil, lookupErr
	}
	if !s.svcs.Cfg.A2A.Enabled {
		return nil, errcode.ErrNotImplement
	}
	agent, err := s.get(ctx, agentID)
	if err != nil {
		return nil, err
	}
	if agent.Enabled == nil || !*agent.Enabled || agent.PinnedEndpoint == "" {
		return nil, errcode.ErrForbidden.WithMsg("远程Agent未启用或尚未同步")
	}
	headers, err := s.headers(agent)
	if err != nil {
		return nil, err
	}
	client, err := s.client(agent.BaseURL, headers)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	var card sdk.AgentCard
	if json.Unmarshal(agent.Card, &card) != nil {
		return nil, errcode.ErrRunConflict
	}
	endpoint, err := client.Endpoint(agent.BaseURL, &card)
	if err != nil || endpoint.URL != agent.PinnedEndpoint {
		return nil, errcode.ErrRunConflict.WithMsg("远端绑定需要重新同步")
	}
	frozen, err := json.Marshal(delegationSnapshot{BaseURL: agent.BaseURL, Endpoint: agent.PinnedEndpoint, Card: append([]byte(nil), agent.Card...), Credentials: agent.EncryptedCredentials})
	if err != nil || len(frozen) > 500<<10 {
		return nil, errcode.ErrInvalidParam
	}
	var row delegationRecord
	created := false
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':a2a-delegation-admission',0))").Error; err != nil {
			return err
		}
		err := tx.Where("owner_id=? AND agent_id=? AND request_key=?", actor.ID, agentID, input.RequestKey).First(&row).Error
		if err == nil {
			if row.InputHash != hash {
				return errcode.ErrRunConflict.WithMsg("请求号已用于不同委派内容")
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var counts struct{ Total, Own, History int64 }
		if err := tx.Raw("SELECT count(*) AS total,count(*) FILTER(WHERE owner_id=?) AS own FROM a2a_delegations WHERE deadline_at>now() AND status IN ('reserved','dispatching','submitted','working','input_required','auth_required','unknown')", actor.ID).Scan(&counts).Error; err != nil {
			return err
		}
		if err := tx.Model(&delegationRecord{}).Where("owner_id=?", actor.ID).Count(&counts.History).Error; err != nil {
			return err
		}
		if counts.Total >= 32 || counts.Own >= 8 {
			return errcode.ErrRateLimited.WithMsg("未完成的A2A委派已达容量上限")
		}
		if counts.History >= 10000 {
			return errcode.ErrRateLimited.WithMsg("A2A委派记录已达10000条上限，请联系管理员；当前不支持自助归档，请保留请求键去重记录")
		}
		row = delegationRecord{ID: rand.Text(), OwnerID: actor.ID, AgentID: agentID, ConfigVersion: agent.ConfigVersion, Version: 1, RequestKey: input.RequestKey, InputHash: hash, Status: "dispatching", Snapshot: datatypes.JSON(frozen)}
		record := map[string]any{"id": row.ID, "owner_id": row.OwnerID, "agent_id": row.AgentID, "config_version": row.ConfigVersion, "version": 1, "request_key": row.RequestKey, "input_hash": row.InputHash, "status": row.Status, "snapshot": row.Snapshot}
		if err := tx.Model(&delegationRecord{}).Create(record).Error; err != nil {
			return err
		}
		if err := tx.Where("id=?", row.ID).First(&row).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !created {
		current, err := s.ownedDelegation(ctx, actor.ID, row.ID)
		if err != nil {
			return nil, err
		}
		return delegationDTO(current), nil
	}
	// Reserve before network, then fence edits/revocation at dispatch. A repeated
	// request key never dispatches again, even if the first process disappears.
	if _, err = s.svcs.activeAdmin(ctx); err == nil {
		current, e := s.get(ctx, agentID)
		if e != nil {
			err = e
		} else if current.ConfigVersion != agent.ConfigVersion || current.Enabled == nil || !*current.Enabled {
			err = errcode.ErrRunConflict.WithMsg("Agent在发送前已变化")
		}
	}
	if err != nil {
		s.finishDelegation(ctx, &row, nil, "failed", "委派权限或配置在发送前变化，未发送。")
		return nil, err
	}
	task, sendErr := client.SendRequest(ctx, agent.BaseURL, &card, input.Text, row.ID, input.ReturnImmediately)
	if sendErr != nil {
		if err = s.finishDelegation(ctx, &row, nil, "unknown", "远端结果未确认；请使用相同请求号核对，勿重复发送。"); err != nil {
			return nil, err
		}
	} else if err = s.finishDelegation(ctx, &row, task, "", ""); err != nil {
		return nil, err
	}
	return delegationDTO(&row), nil
}
func (s *A2AService) finishDelegation(parent context.Context, row *delegationRecord, task *a2a.Task, state, message string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	updates := map[string]any{"version": gorm.Expr("version+1"), "updated_at": gorm.Expr("now()"), "error": message}
	if task != nil {
		updates["status"] = task.State
		updates["remote_task_id"] = task.ID
		updates["output"] = a2a.ExtractText(task)
	} else {
		updates["status"] = state
	}
	result := s.svcs.Repos.DB.WithContext(ctx).Model(&delegationRecord{}).Where("id=? AND owner_id=? AND version=? AND status NOT IN ?", row.ID, row.OwnerID, row.Version, []string{"completed", "failed", "canceled", "rejected"}).Updates(updates)
	if result.Error != nil {
		return errcode.ErrA2ATaskFail.WithMsg("委派结果尚未确认入库，请使用相同请求号查询")
	}
	// A concurrent cancellation or refresh may have won. Return the stored state,
	// never claim that this response replaced the authoritative terminal state.
	return s.svcs.Repos.DB.WithContext(ctx).Where("id=? AND owner_id=?", row.ID, row.OwnerID).First(row).Error
}
func (s *A2AService) delegationClient(ctx context.Context, row *delegationRecord) (*a2a.Client, *sdk.AgentCard, error) {
	if !s.svcs.Cfg.A2A.Enabled {
		return nil, nil, errcode.ErrNotImplement
	}
	current, err := s.get(ctx, row.AgentID)
	if err != nil {
		return nil, nil, err
	}
	if current.Enabled == nil || !*current.Enabled {
		return nil, nil, errcode.ErrForbidden.WithMsg("远程Agent已停用")
	}
	var frozen delegationSnapshot
	if json.Unmarshal(row.Snapshot, &frozen) != nil {
		return nil, nil, errcode.ErrRunConflict
	}
	headers := map[string]string{}
	if frozen.Credentials != "" {
		if s.svcs.Encryptor == nil {
			return nil, nil, errcode.ErrInternal
		}
		raw, err := s.svcs.Encryptor.Decrypt(frozen.Credentials)
		if err != nil || len(raw) > 16384 || json.Unmarshal([]byte(raw), &headers) != nil {
			return nil, nil, errcode.ErrInternal
		}
	}
	client, err := s.client(frozen.BaseURL, headers)
	if err != nil {
		return nil, nil, err
	}
	var card sdk.AgentCard
	if json.Unmarshal(frozen.Card, &card) != nil {
		client.Close()
		return nil, nil, errcode.ErrRunConflict
	}
	endpoint, err := client.Endpoint(frozen.BaseURL, &card)
	if err != nil || endpoint.URL != frozen.Endpoint {
		client.Close()
		return nil, nil, errcode.ErrRunConflict
	}
	return client, &card, nil
}
func (s *A2AService) Delegation(ctx context.Context, id string, refresh bool) (*SendTaskResult, error) {
	actor, err := s.svcs.activeAdmin(ctx)
	if err != nil {
		return nil, err
	}
	row, err := s.ownedDelegation(ctx, actor.ID, id)
	if err != nil {
		return nil, err
	}
	if !refresh || row.RemoteTaskID == "" || delegationTerminal(row.Status) {
		return delegationDTO(row), nil
	}
	client, card, err := s.delegationClient(ctx, row)
	if err != nil {
		result := delegationDTO(row)
		result.Error = "远端不可用或访问已停用，显示最近确认状态。"
		result.CanRefresh = false
		result.CanCancel = false
		return result, nil
	}
	defer client.Close()
	claimed, claimErr := s.claimDelegation(ctx, id, actor.ID, false)
	if claimErr != nil {
		return nil, claimErr
	}
	if !claimed {
		return delegationDTO(row), nil
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	task, refreshErr := client.GetTask(bounded, frozenBase(row), card, row.RemoteTaskID)
	if refreshErr == nil {
		err = s.finishDelegation(ctx, row, task, "", "")
	} else {
		result := delegationDTO(row)
		result.Error = "刷新失败，显示最近确认状态。"
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	return delegationDTO(row), nil
}
func frozenBase(row *delegationRecord) string {
	var frozen delegationSnapshot
	_ = json.Unmarshal(row.Snapshot, &frozen)
	return frozen.BaseURL
}

func (s *A2AService) claimDelegation(parent context.Context, id string, owner int64, cancelRequest bool) (bool, error) {
	column := "poll_until"
	seconds := 5
	if cancelRequest {
		column = "cancel_until"
		seconds = 10
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	claim := s.svcs.Repos.DB.WithContext(ctx).Model(&delegationRecord{}).Where("id=? AND owner_id=? AND "+column+"<=now()", id, owner).Update(column, gorm.Expr("now()+?*interval '1 second'", seconds))
	return claim.RowsAffected == 1, claim.Error
}
func (s *A2AService) CancelDelegation(ctx context.Context, id string) (*SendTaskResult, error) {
	actor, err := s.svcs.activeAdmin(ctx)
	if err != nil {
		return nil, err
	}
	row, err := s.ownedDelegation(ctx, actor.ID, id)
	if err != nil {
		return nil, err
	}
	if row.RemoteTaskID == "" || delegationTerminal(row.Status) {
		return nil, errcode.ErrRunConflict.WithMsg("该任务没有可取消的远端执行")
	}
	client, card, err := s.delegationClient(ctx, row)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	claimed, claimErr := s.claimDelegation(ctx, id, actor.ID, true)
	if claimErr != nil {
		return nil, claimErr
	}
	if !claimed {
		return nil, errcode.ErrRunConflict.WithMsg("取消请求正在核对，请稍后查询")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	task, err := client.CancelTask(bounded, frozenBase(row), card, row.RemoteTaskID)
	if err != nil {
		result := delegationDTO(row)
		result.Error = "远端取消结果未确认，请刷新状态核对。"
		return result, nil
	}
	if err = s.finishDelegation(ctx, row, task, "", ""); err != nil {
		return nil, err
	}
	return delegationDTO(row), nil
}
