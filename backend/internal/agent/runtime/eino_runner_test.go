package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

type runtimeTools struct{ invoke func(context.Context) string }

func (t runtimeTools) ListContext(ctx context.Context, bindings string) ([]llm.ToolDef, error) {
	if bindings == "" || bindings == "[]" {
		return nil, ctx.Err()
	}
	return []llm.ToolDef{{Name: "calculate", Parameters: map[string]any{"type": "object"}}}, ctx.Err()
}
func (t runtimeTools) Invoke(ctx context.Context, _, name, _ string) tool.ToolResult {
	out := "42"
	if t.invoke != nil {
		out = t.invoke(ctx)
	}
	return tool.ToolResult{Output: out, Status: "ok", ToolName: name}
}

func testRunner(t *testing.T, handler http.HandlerFunc, source runtimeTools, iterations int) (*EinoRunner, *RunInput) {
	t.Helper()
	s := llmstub.New(t, handler)
	g := llm.NewGateway(&config.LLM{MaxConnsPerProvider: 16, CircuitErrorRate: 0.5, CircuitOpen: time.Second})
	r, err := NewEinoRunner(g, source, EinoConfig{StepTimeout: time.Second, RunTimeout: 3 * time.Second, SendTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return r, &RunInput{SessionID: "session", AgentName: "test", Messages: []llm.Message{{Role: llm.RoleUser, Content: "calculate"}}, ToolsJSON: `[{"tool_code":"calculate"}]`, Primary: &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, MaxIterations: iterations}
}

func answer(w http.ResponseWriter, text string) {
	frame := map[string]any{"model": "stub", "choices": []any{map[string]any{"delta": map[string]any{"content": text}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 2}}
	raw, _ := json.Marshal(frame)
	fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
}
func toolAnswer(w http.ResponseWriter, count int) {
	var calls []any
	for i := range count {
		calls = append(calls, map[string]any{"index": i, "id": fmt.Sprintf("call-%d", i), "type": "function", "function": map[string]string{"name": "calculate", "arguments": "{}"}})
	}
	frame := map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": calls}, "finish_reason": "tool_calls"}}, "usage": map[string]int{"prompt_tokens": 3, "completion_tokens": 1}}
	raw, _ := json.Marshal(frame)
	fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
}
func collect(t *testing.T, r *EinoRunner, in *RunInput) []agent.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	events, err := r.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	var out []agent.Event
	for ev := range events {
		out = append(out, ev)
	}
	return out
}
func terminal(t *testing.T, events []agent.Event, want string) {
	t.Helper()
	count := 0
	for _, ev := range events {
		if ev.Type == agent.TypeDone || ev.Type == agent.TypeError {
			count++
			if ev.Type != want {
				t.Fatalf("wrong terminal: %+v", events)
			}
		}
	}
	if count != 1 {
		t.Fatalf("want one terminal: %+v", events)
	}
}

func TestEinoRunnerReActRoundTrip(t *testing.T) {
	var requests atomic.Int32
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			toolAnswer(w, 1)
			return
		}
		var req struct {
			Messages []llm.Message `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		last := req.Messages[len(req.Messages)-1]
		if last.Role != llm.RoleTool || last.Content != "42" || last.ToolCallID != "call-0" {
			t.Errorf("tool feedback missing: %+v", req.Messages)
		}
		answer(w, "result 42")
	}, runtimeTools{}, 4)
	in.Trace = observability.NewAgentTrace()
	before := append([]llm.Message(nil), in.Messages...)
	events := collect(t, r, in)
	terminal(t, events, agent.TypeDone)
	var text string
	var calls, results int
	for _, ev := range events {
		switch ev.Type {
		case agent.TypeDelta:
			text += ev.Data["content"].(string)
		case agent.TypeToolCall:
			calls++
		case agent.TypeToolResult:
			results++
		case agent.TypeUsage:
			if ev.Data["prompt_tokens"] != 8 || ev.Data["completion_tokens"] != 3 {
				t.Errorf("usage lost/doubled: %+v", ev)
			}
		}
	}
	if text != "result 42" || calls != 1 || results != 1 || requests.Load() != 2 {
		t.Fatalf("incomplete ReAct: %q calls=%d results=%d requests=%d", text, calls, results, requests.Load())
	}
	if !reflect.DeepEqual(before, in.Messages) {
		t.Fatal("runner mutated caller messages")
	}
	steps := in.Trace.Snapshot()
	if len(steps) < 2 || steps[0].Agent != "test" || steps[0].Tool != "calculate" || steps[0].Result != "42" {
		t.Fatalf("tool trace lost identity/result: %+v", steps)
	}
}

func TestEinoRunnerInputNotMutated(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) { answer(w, "ok") }, runtimeTools{}, 2)
	original := *in
	collect(t, r, in)
	if !reflect.DeepEqual(*in, original) {
		t.Fatal("run mutated shared input")
	}
}
func TestEinoRunnerEmptySuccessfulAnswer(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) { answer(w, "") }, runtimeTools{}, 2)
	terminal(t, collect(t, r, in), agent.TypeDone)
}
func TestEinoRunnerIterationLimit(t *testing.T) {
	var calls atomic.Int32
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); toolAnswer(w, 1) }, runtimeTools{}, 2)
	terminal(t, collect(t, r, in), agent.TypeError)
	if calls.Load() != 2 {
		t.Fatalf("wrong model iteration bound: %d", calls.Load())
	}
}
func TestEinoRunnerUpstreamPartialFailure(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	}, runtimeTools{}, 2)
	terminal(t, collect(t, r, in), agent.TypeError)
}

func TestEinoRunnerCancellationClosesModel(t *testing.T) {
	closed := make(chan struct{})
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}, runtimeTools{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	events, err := r.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if (<-events).Type != agent.TypeDelta {
		t.Fatal("missing first delta")
	}
	cancel()
	var output []agent.Event
	for ev := range events {
		output = append(output, ev)
	}
	terminal(t, output, agent.TypeDone)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("model connection leaked on cancel")
	}
}

func TestEinoRunnerToolConcurrencyBound(t *testing.T) {
	var requests, active, peak atomic.Int32
	gate := make(chan struct{})
	entered := make(chan struct{}, 8)
	source := runtimeTools{invoke: func(ctx context.Context) string {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return "42"
	}}
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			toolAnswer(w, 8)
		} else {
			answer(w, "ok")
		}
	}, source, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := r.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("tools not scheduled")
		}
	}
	close(gate)
	for range events {
	}
	if peak.Load() > 4 {
		t.Fatalf("unbounded tools: peak %d", peak.Load())
	}
}

func TestEinoEventsSlowConsumerTerminates(t *testing.T) {
	closed := make(chan struct{})
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		for range 200 {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}, runtimeTools{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := r.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("slow consumer did not release model")
	}
	for range events {
	}
}

func TestEinoRunnerRejectsOversizedToolFanout(t *testing.T) {
	var calls atomic.Int32
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) { toolAnswer(w, 33) }, runtimeTools{invoke: func(context.Context) string { calls.Add(1); return "bad" }}, 2)
	terminal(t, collect(t, r, in), agent.TypeError)
	if calls.Load() != 0 {
		t.Fatal("oversized fanout executed tools")
	}
}

func TestEinoRunnerRunDeadline(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, runtimeTools{}, 2)
	r.cfg.RunTimeout = 30 * time.Millisecond
	terminal(t, collect(t, r, in), agent.TypeError)
}

func TestEinoRunnerBoundsGeneratedOutput(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, r *http.Request) { answer(w, strings.Repeat("x", 2048)) }, runtimeTools{}, 2)
	r.cfg.MaxModelOutputBytes = 1024
	terminal(t, collect(t, r, in), agent.TypeError)
}
