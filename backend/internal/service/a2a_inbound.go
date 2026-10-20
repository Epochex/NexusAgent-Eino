package service

import (
	"context"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

func (s *A2AService) Inbound(root context.Context, publicBase string) (*a2a.Server, error) {
	if !s.svcs.Cfg.A2A.Enabled || s.svcs.JWT == nil {
		return nil, fmt.Errorf("A2A requires enabled configuration and JWT verification")
	}
	parsed, err := url.Parse(publicBase)
	if err != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid A2A public URL")
	}
	if _, err = security.OutboundOrigin(publicBase); err != nil {
		return nil, fmt.Errorf("invalid A2A public origin")
	}
	card := &sdk.AgentCard{Name: s.svcs.Cfg.A2A.Name, Description: s.svcs.Cfg.A2A.Desc, Version: "0.3",
		SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(strings.TrimRight(publicBase, "/")+"/a2a", sdk.TransportProtocolJSONRPC)},
		Skills:              []sdk.AgentSkill{{ID: "chat", Name: "Authenticated assistant", Description: "Owner-scoped text assistance; durable write approvals use platform workflows", Tags: []string{"text", "assistant"}}},
	}
	timeout := 2 * time.Minute
	if configured := s.svcs.Cfg.Agent.RunTimeout; configured > 0 && configured < timeout {
		timeout = configured
	}
	store, err := a2a.NewTaskStore(s.svcs.Repos.DB, a2a.TaskOwner, timeout+30*time.Second)
	if err != nil {
		return nil, err
	}
	server := a2a.NewServer(card, func(ctx context.Context, text string) (string, error) { return s.InvokeLocal(ctx, 0, text) }, a2a.WithServerConfig(a2a.ServerConfig{
		Root: root, Store: store, MaxConcurrency: s.svcs.Cfg.A2A.MaxConcurrency, RunTimeout: timeout,
		Authenticate: func(ctx context.Context, token string) (int64, error) {
			claims, err := s.svcs.JWT.Parse(token, false)
			if err != nil {
				return 0, err
			}
			actor, err := s.svcs.activeUser(security.WithUserID(ctx, claims.UserID))
			if err != nil {
				return 0, err
			}
			return actor.ID, nil
		},
	}))
	if err = server.ValidationError(); err != nil {
		return nil, err
	}
	return server, nil
}

func (s *A2AService) InvokeLocal(ctx context.Context, agentID int64, text string) (string, error) {
	actor, err := s.svcs.activeUser(ctx)
	if err != nil {
		return "", err
	}
	if !s.svcs.Cfg.A2A.Enabled {
		return "", errcode.ErrNotImplement.WithMsg("A2A服务已停用")
	}
	if len(text) > 32768 || strings.TrimSpace(text) == "" || !utf8.ValidString(text) {
		return "", errcode.ErrInvalidParam
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, err := s.svcs.Chat.Stream(ctx, actor.ID, &ChatInput{
		AgentID: agentID,
		Content: text,
	})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	completed := 0
	for ev := range events {
		if ev.Type == agent.TypeDelta {
			if s, ok := ev.Data["content"].(string); ok {
				if b.Len()+len(s) > 65536 {
					return "", errcode.ErrA2ATaskFail.WithMsg("回答超过64KiB上限")
				}
				b.WriteString(s)
			}
		}
		if ev.Type == agent.TypeError {
			return "", errcode.ErrA2ATaskFail.WithMsg("本地Agent执行失败")
		}
		if ev.Type == agent.TypeDone {
			reason, _ := ev.Data["finish_reason"].(string)
			if reason != "stop" && reason != "end_turn" {
				return "", errcode.ErrA2ATaskFail.WithMsg("Agent未完整完成回答")
			}
			completed++
		}
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if completed != 1 || strings.TrimSpace(b.String()) == "" || !utf8.ValidString(b.String()) {
		return "", errcode.ErrA2ATaskFail.WithMsg("Agent未返回完整有效回答")
	}
	return b.String(), nil
}
