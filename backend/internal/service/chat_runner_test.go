package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

type noTools struct{}

func (noTools) List(string) ([]llm.ToolDef, error)                         { return nil, nil }
func (noTools) ListContext(context.Context, string) ([]llm.ToolDef, error) { return nil, nil }
func (noTools) Invoke(context.Context, string, string, string) tool.ToolResult {
	return tool.ToolResult{Status: "error"}
}
func TestChatSelectsEinoForEveryPath(t *testing.T) {
	cfg := &config.Config{Agent: config.Agent{StepTimeout: time.Second, RunTimeout: 2 * time.Second}}
	svcs := &Services{Deps: Deps{Cfg: cfg, Gateway: llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4}), Executor: noTools{}}}
	chat := NewChatService(svcs)
	r, err := chat.pickRunner(context.Background(), 1, &model.Agent{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.(*runtime.EinoRunner); !ok {
		t.Fatalf("ordinary agent still uses %T", r)
	}
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	events, err := chat.hostDirectRunner()(context.Background(), &runtime.RunInput{AgentName: "host", Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}}, Primary: &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}})
	if err != nil {
		t.Fatal(err)
	}
	done := false
	for ev := range events {
		if ev.Type == agent.TypeError {
			t.Fatalf("host direct failed: %+v", ev)
		}
		if ev.Type == agent.TypeDone {
			done = true
		}
	}
	if !done {
		t.Fatal("host direct did not complete")
	}
}

func TestAgentOutputCannotRaiseModelLimit(t *testing.T) {
	peer := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			MaxTokens int `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.MaxTokens != 1024 {
			t.Errorf("agent raised configured model limit: max_tokens=%d", body.MaxTokens)
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"bounded answer"},"finish_reason":"stop"}]}`)
	})
	configured := &llm.ModelDescriptor{BaseURL: peer.URL, ModelName: "bounded", ContextWindow: 8192, MaxTokens: 1024}
	primary, _ := agentSampling(configured, nil, &model.Agent{MaxTokens: 4096})
	g := llm.NewGateway(&config.LLM{MaxConnsPerProvider: 2})
	if _, _, err := g.Chat(context.Background(), primary, nil, &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "q"}}}); err != nil {
		t.Fatal(err)
	}
	if configured.MaxTokens != 1024 {
		t.Fatal("agent mutated configured model")
	}
}
