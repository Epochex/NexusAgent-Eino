package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type A2AService struct{ svcs *Services }

func NewA2AService(svcs *Services) *A2AService { return &A2AService{svcs: svcs} }

type A2ARegisterInput struct {
	Name    string            `json:"name"`
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers"`
	Enabled *bool             `json:"enabled"`
}
type A2AUpdateInput struct {
	ConfigVersion int64             `json:"config_version"`
	Name          *string           `json:"name"`
	BaseURL       *string           `json:"base_url"`
	Headers       map[string]string `json:"headers"`
	ClearHeaders  bool              `json:"clear_headers"`
	Enabled       *bool             `json:"enabled"`
}

func (s *A2AService) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.svcs.Repos.DB.WithContext(ctx).Transaction(fn)
}
func validateA2AName(name string) error {
	if strings.TrimSpace(name) == "" || len(name) > 128 || !utf8.ValidString(name) {
		return errcode.ErrInvalidParam.WithMsg("Agent名称长度必须为1..128字节")
	}
	return nil
}
func (s *A2AService) validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || len(raw) > 512 || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errcode.ErrInvalidParam.WithMsg("Agent地址不能含查询参数、凭据或片段")
	}
	if err = (security.OutboundPolicy{PrivateOrigins: s.svcs.Cfg.Outbound.PrivateOrigins}).Validate(raw); err != nil {
		return errcode.ErrDomainNotAllow.WithCause(err)
	}
	return nil
}
func (s *A2AService) seal(headers map[string]string) (string, error) {
	if headers == nil {
		headers = map[string]string{}
	}
	if s.svcs.Encryptor == nil {
		return "", fmt.Errorf("A2A credential encryption unavailable")
	}
	raw, err := json.Marshal(headers)
	if err != nil || len(raw) > 16384 {
		return "", errcode.ErrInvalidParam
	}
	return s.svcs.Encryptor.Encrypt(string(raw))
}
func (s *A2AService) headers(agent *model.A2AAgent) (map[string]string, error) {
	headers := map[string]string{}
	if agent.EncryptedCredentials == "" {
		return headers, nil
	}
	if s.svcs.Encryptor == nil {
		return nil, fmt.Errorf("A2A credential encryption unavailable")
	}
	raw, err := s.svcs.Encryptor.Decrypt(agent.EncryptedCredentials)
	if err != nil || len(raw) > 16384 || json.Unmarshal([]byte(raw), &headers) != nil {
		return nil, fmt.Errorf("A2A credentials cannot be decrypted")
	}
	if headers == nil {
		headers = map[string]string{}
	}
	return headers, nil
}
func (s *A2AService) client(raw string, headers map[string]string) (*a2a.Client, error) {
	if err := s.validateURL(raw); err != nil {
		return nil, err
	}
	client := a2a.NewClient(a2a.WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: s.svcs.Cfg.Outbound.PrivateOrigins}), a2a.WithCredentials(raw, headers))
	if err := client.ValidationError(); err != nil {
		client.Close()
		return nil, errcode.ErrInvalidParam.WithCause(err)
	}
	return client, nil
}
func lockedA2A(tx *gorm.DB, id int64) (*model.A2AAgent, error) {
	var agent model.A2AAgent
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&agent, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errcode.ErrAgentNotFound.WithMsg("远程Agent不存在")
	}
	return &agent, err
}
func (s *A2AService) get(ctx context.Context, id int64) (*model.A2AAgent, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var agent model.A2AAgent
	err := s.svcs.Repos.DB.WithContext(ctx).First(&agent, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errcode.ErrAgentNotFound.WithMsg("远程Agent不存在")
	}
	return &agent, err
}
func (s *A2AService) Register(ctx context.Context, input *A2ARegisterInput) (*model.A2AAgent, error) {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errcode.ErrInvalidParam
	}
	if !s.svcs.Cfg.A2A.Enabled {
		return nil, errcode.ErrNotImplement.WithMsg("A2A已停用")
	}
	client, err := s.client(input.BaseURL, input.Headers)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	card, err := client.FetchCard(ctx, input.BaseURL)
	if err != nil {
		return nil, errcode.ErrA2ACardFetch.WithCause(err)
	}
	endpoint, err := client.Endpoint(input.BaseURL, card)
	if err != nil {
		return nil, err
	}
	name := input.Name
	if name == "" {
		name = card.Name
	}
	if err = validateA2AName(name); err != nil {
		return nil, err
	}
	encrypted, err := s.seal(input.Headers)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	now := time.Now().UTC()
	agent := &model.A2AAgent{Name: name, BaseURL: input.BaseURL, Card: datatypes.JSON(raw), PinnedEndpoint: endpoint.URL, ConfigVersion: 1, Enabled: &enabled, EncryptedCredentials: encrypted, HeadersConfigured: len(input.Headers) > 0, CardHash: fmt.Sprintf("%x", sha256.Sum256(raw)), Status: "connected", LastHealthAt: &now}
	if _, err = s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':a2a-config',0))").Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Model(&model.A2AAgent{}).Count(&count).Error; err != nil {
			return err
		}
		if count >= 128 {
			return errcode.ErrInvalidParam.WithMsg("远程Agent超过128个上限")
		}
		return tx.Create(agent).Error
	})
	agent.EncryptedCredentials = ""
	return agent, err
}
func (s *A2AService) List(ctx context.Context) ([]model.A2AAgent, error) {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var rows []model.A2AAgent
	err := s.svcs.Repos.DB.WithContext(ctx).Select("id,name,base_url,enabled,config_version,headers_present,pinned_endpoint,card,status,last_health_at,created_at,updated_at").Order("id").Limit(129).Find(&rows).Error
	if len(rows) > 128 {
		return nil, fmt.Errorf("A2A registry limit exceeded")
	}
	return rows, err
}
func (s *A2AService) Update(ctx context.Context, id int64, input *A2AUpdateInput) (*model.A2AAgent, error) {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	if input == nil || input.ConfigVersion < 1 {
		return nil, errcode.ErrInvalidParam
	}
	var result *model.A2AAgent
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		current, err := lockedA2A(tx, id)
		if err != nil {
			return err
		}
		if current.ConfigVersion != input.ConfigVersion {
			return errcode.ErrRunConflict.WithMsg("Agent配置已变化，请刷新")
		}
		if input.Name != nil {
			current.Name = *input.Name
		}
		if err = validateA2AName(current.Name); err != nil {
			return err
		}
		changed := false
		if input.BaseURL != nil && *input.BaseURL != current.BaseURL {
			current.BaseURL = *input.BaseURL
			changed = true
		}
		headers, err := s.headers(current)
		if err != nil {
			return err
		}
		if input.ClearHeaders || len(input.Headers) > 0 {
			next := input.Headers
			if input.ClearHeaders {
				next = map[string]string{}
			}
			before, _ := json.Marshal(headers)
			after, _ := json.Marshal(next)
			changed = changed || string(before) != string(after)
			headers = next
		}
		client, err := s.client(current.BaseURL, headers)
		if err != nil {
			return err
		}
		client.Close()
		encrypted, err := s.seal(headers)
		if err != nil {
			return err
		}
		current.EncryptedCredentials = encrypted
		current.HeadersConfigured = len(headers) > 0
		current.ConfigVersion++
		if input.Enabled != nil {
			current.Enabled = input.Enabled
		}
		if changed {
			disabled := false
			current.Enabled = &disabled
			current.Card = datatypes.JSON("{}")
			current.PinnedEndpoint = ""
			current.CardHash = ""
			current.Status = "requires_sync"
		}
		if current.Enabled != nil && *current.Enabled && current.PinnedEndpoint == "" {
			return errcode.ErrRunConflict.WithMsg("请先同步当前协议名片，再启用Agent")
		}
		if err = tx.Save(current).Error; err != nil {
			return err
		}
		result = current
		return nil
	})
	if result != nil {
		result.EncryptedCredentials = ""
	}
	return result, err
}
func (s *A2AService) Delete(ctx context.Context, id int64) error {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return err
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		agent, err := lockedA2A(tx, id)
		if err != nil {
			return err
		}
		if err = tx.Model(agent).Updates(map[string]any{"enabled": false, "config_version": gorm.Expr("config_version+1")}).Error; err != nil {
			return err
		}
		return tx.Delete(agent).Error
	})
}
func (s *A2AService) Sync(ctx context.Context, id int64) (*model.A2AAgent, error) {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	agent, err := s.get(ctx, id)
	if err != nil {
		return nil, err
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
	card, err := client.FetchCard(ctx, agent.BaseURL)
	if err != nil {
		return nil, errcode.ErrA2ACardFetch.WithCause(err)
	}
	endpoint, err := client.Endpoint(agent.BaseURL, card)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	encrypted, err := s.seal(headers)
	if err != nil {
		return nil, err
	}
	var result *model.A2AAgent
	if _, err = s.svcs.activeAdmin(ctx); err != nil {
		return nil, err
	}
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		current, err := lockedA2A(tx, id)
		if err != nil {
			return err
		}
		if current.ConfigVersion != agent.ConfigVersion {
			return errcode.ErrRunConflict.WithMsg("Agent配置已变化，请重新同步")
		}
		current.Card = datatypes.JSON(raw)
		current.CardHash = hash
		current.PinnedEndpoint = endpoint.URL
		current.EncryptedCredentials = encrypted
		current.HeadersConfigured = len(headers) > 0
		current.ConfigVersion++
		current.Status = "connected"
		now := time.Now().UTC()
		current.LastHealthAt = &now
		if err = tx.Save(current).Error; err != nil {
			return err
		}
		result = current
		return nil
	})
	if result != nil {
		result.EncryptedCredentials = ""
	}
	return result, err
}
func (s *A2AService) HealthCheck(ctx context.Context, id int64) error {
	if _, err := s.svcs.activeAdmin(ctx); err != nil {
		return err
	}
	agent, err := s.get(ctx, id)
	if err != nil {
		return err
	}
	headers, err := s.headers(agent)
	if err != nil {
		return err
	}
	client, err := s.client(agent.BaseURL, headers)
	if err != nil {
		return err
	}
	defer client.Close()
	card, checkErr := client.FetchCard(ctx, agent.BaseURL)
	status := "connected"
	if checkErr != nil {
		status = "error"
	} else {
		raw, _ := json.Marshal(card)
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != agent.CardHash {
			status = "configuration_changed"
			checkErr = errcode.ErrRunConflict.WithMsg("远端名片已变化，请审阅后显式同步")
		}
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = s.svcs.Repos.DB.WithContext(bounded).Model(&model.A2AAgent{}).Where("id=? AND config_version=?", id, agent.ConfigVersion).Updates(map[string]any{"status": status, "last_health_at": time.Now().UTC()}).Error
	return checkErr
}

// SendTask is the compatibility entrypoint. API clients should supply a stable
// request_key through Delegate so a lost response cannot cause another dispatch.
func (s *A2AService) SendTask(ctx context.Context, id int64, text string) (*SendTaskResult, error) {
	return s.Delegate(ctx, id, &A2ADelegateInput{Text: text, RequestKey: rand.Text()})
}
