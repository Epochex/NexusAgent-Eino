package host

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

type noTools struct{}

func (noTools) ListContext(context.Context, string) ([]llm.ToolDef, error) { return nil, nil }
func (noTools) Invoke(context.Context, string, string, string) tool.ToolResult {
	return tool.ToolResult{}
}

var _ einoadapter.ToolSource = noTools{}

func TestHostEmptyExpertAnswerDoesNotMerge(t *testing.T) {
	model := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	runner, err := runtime.NewEinoRunner(llm.NewGateway(&config.LLM{}), noTools{}, runtime.EinoConfig{StepTimeout: time.Second, RunTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := New([]Expert{{Name: "expert", Primary: &llm.ModelDescriptor{BaseURL: model.URL, ModelName: "stub"}, Runner: runner}}, routeFirst, nil)
	events, err := h.Run(context.Background(), hostInput())
	if err != nil {
		t.Fatal(err)
	}
	var failed bool
	for ev := range events {
		if ev.Type == agent.TypeDone || ev.Type == agent.TypeAgentEvent && ev.Data["type"] == "merge" {
			t.Fatalf("empty expert reported success: %+v", ev)
		}
		if ev.Type == agent.TypeError {
			failed = true
		}
	}
	if !failed {
		t.Fatal("empty expert failure missing")
	}
}

type runnerFunc func(context.Context, *runtime.RunInput) (<-chan agent.Event, error)

func (f runnerFunc) Run(ctx context.Context, in *runtime.RunInput) (<-chan agent.Event, error) {
	return f(ctx, in)
}
func routeFirst(context.Context, *llm.ModelDescriptor, *llm.ModelDescriptor, string) (*llm.ChatResponse, *llm.ModelDescriptor, error) {
	return &llm.ChatResponse{Content: "1"}, nil, nil
}
func eventsOf(events ...agent.Event) <-chan agent.Event {
	out := make(chan agent.Event, len(events))
	for _, ev := range events {
		out <- ev
	}
	close(out)
	return out
}
func hostInput() *runtime.RunInput {
	return &runtime.RunInput{AgentName: "host", Primary: &llm.ModelDescriptor{ModelName: "host"}, Messages: []llm.Message{{Role: llm.RoleUser, Content: "q"}}, DangerousTools: map[string]bool{"host": true}}
}

func TestHostExpertUsesOwnToolsAndDangerousFlags(t *testing.T) {
	in := hostInput()
	expert := Expert{Name: "expert", ToolsJSON: `[{"tool_code":"expert"}]`, DangerousTools: map[string]bool{"expert": true}, Runner: runnerFunc(func(ctx context.Context, got *runtime.RunInput) (<-chan agent.Event, error) {
		if !got.DangerousTools["expert"] || got.DangerousTools["host"] || got.ToolsJSON != `[{"tool_code":"expert"}]` {
			t.Errorf("expert inherited host authority: %+v", got)
		}
		return eventsOf(agent.EvtDone("stop")), nil
	})}
	h := New([]Expert{expert}, routeFirst, nil)
	events, err := h.Run(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	if in.AgentName != "host" {
		t.Fatal("caller input mutated")
	}
}
func TestHostDoesNotMergeAfterExpertFailure(t *testing.T) {
	h := New([]Expert{{Name: "expert", Runner: runnerFunc(func(context.Context, *runtime.RunInput) (<-chan agent.Event, error) {
		return eventsOf(agent.EvtDelta("partial"), agent.EvtError(50001, "failed")), nil
	})}}, routeFirst, nil)
	events, _ := h.Run(context.Background(), hostInput())
	count := 0
	for ev := range events {
		if ev.Type == agent.TypeAgentEvent && ev.Data["type"] == "merge" {
			t.Fatal("merged failed result")
		}
		if ev.Type == agent.TypeError {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("failure terminal count %d", count)
	}
}
func TestHostForwardsOneTerminal(t *testing.T) {
	h := New([]Expert{{Name: "expert", Runner: runnerFunc(func(context.Context, *runtime.RunInput) (<-chan agent.Event, error) {
		return eventsOf(agent.EvtDelta("ok"), agent.EvtDone("stop")), nil
	})}}, routeFirst, nil)
	events, _ := h.Run(context.Background(), hostInput())
	var last agent.Event
	terminals := 0
	for ev := range events {
		last = ev
		if ev.Type == agent.TypeDone || ev.Type == agent.TypeError {
			terminals++
		}
	}
	if terminals != 1 || last.Type != agent.TypeDone {
		t.Fatal("merge was emitted after terminal")
	}
}
func TestHostCancellationPropagates(t *testing.T) {
	started, closed := make(chan struct{}), make(chan struct{})
	h := New([]Expert{{Name: "expert", Runner: runnerFunc(func(ctx context.Context, _ *runtime.RunInput) (<-chan agent.Event, error) {
		out := make(chan agent.Event)
		go func() { close(started); <-ctx.Done(); close(closed); close(out) }()
		return out, nil
	})}}, routeFirst, nil)
	ctx, cancel := context.WithCancel(context.Background())
	events, _ := h.Run(ctx, hostInput())
	<-started
	cancel()
	for range events {
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("expert ignored host cancellation")
	}
}

func TestHostRouteHasRunDeadline(t *testing.T) {
	h := New([]Expert{{Name: "expert"}}, func(ctx context.Context, _ *llm.ModelDescriptor, _ *llm.ModelDescriptor, _ string) (*llm.ChatResponse, *llm.ModelDescriptor, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}, nil).WithRunTimeout(20 * time.Millisecond)
	events, _ := h.Run(context.Background(), hostInput())
	var last agent.Event
	for ev := range events {
		last = ev
	}
	if last.Type != agent.TypeError {
		t.Fatalf("route deadline did not terminate with error: %+v", last)
	}
}
