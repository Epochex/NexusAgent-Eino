package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/cloudwego/eino/compose"
)

type memoryCheckpoints struct {
	mu   sync.Mutex
	data map[string][]byte
}

type graphTools struct {
	dangerous bool
	calls     atomic.Int32
}

func (s *graphTools) ListContext(context.Context, string) ([]llm.ToolDef, error) {
	return []llm.ToolDef{{Name: "write", Dangerous: s.dangerous, Parameters: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []any{"value"}, "additionalProperties": false}}}, nil
}
func (s *graphTools) List(raw string) ([]llm.ToolDef, error) {
	return s.ListContext(context.Background(), raw)
}
func (s *graphTools) Invoke(_ context.Context, _ string, _ string, args string) tool.ToolResult {
	s.calls.Add(1)
	return tool.ToolResult{Status: "ok", Output: args}
}

func TestGraphSensitiveToolRequiresExactResume(t *testing.T) {
	cp := &memoryCheckpoints{}
	source := &graphTools{dangerous: true}
	definition, err := ParseDSL("name: write\nnodes: [{key: write, type: tool, tool_code: write, args: {value: original}}]")
	if err != nil {
		t.Fatal(err)
	}
	r, err := Compile(context.Background(), definition, Deps{Tools: source}, cp)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Invoke(context.Background(), map[string]any{}, compose.WithCheckPointID("write"))
	resume := pending(t, err)
	if source.calls.Load() != 0 {
		t.Fatal("sensitive tool ran before approval")
	}
	// Changing live metadata cannot implicitly approve an already interrupted node.
	source.dangerous = false
	r, err = Compile(context.Background(), definition, Deps{Tools: source}, cp)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Invoke(context.Background(), nil, compose.WithCheckPointID("write"))
	resume = pending(t, err) // Eino may issue a new exact ID when re-interrupting.
	if source.calls.Load() != 0 {
		t.Fatal("metadata change bypassed pending approval")
	}
	_, err = r.Invoke(compose.BatchResumeWithData(context.Background(), toAny(resume)), nil, compose.WithCheckPointID("write"))
	if err != nil || source.calls.Load() != 1 {
		t.Fatalf("approved tool not executed exactly once: %v calls=%d", err, source.calls.Load())
	}
}

func (c *memoryCheckpoints) Get(_ context.Context, id string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[id]
	return append([]byte(nil), v...), ok, nil
}
func (c *memoryCheckpoints) Set(_ context.Context, id string, v []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = map[string][]byte{}
	}
	c.data[id] = append([]byte(nil), v...)
	return nil
}
func graphFixture(t *testing.T, text string, cp *memoryCheckpoints, calls *sync.Map, lookup func(context.Context, int64) (*DSL, error)) compose.Runnable[map[string]any, map[string]any] {
	t.Helper()
	d, err := ParseDSL(text)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Compile(context.Background(), d, Deps{LookupWorkflow: lookup, ExecuteNode: func(ctx context.Context, path string, n *Node, state map[string]any) (any, error) {
		counter, _ := calls.LoadOrStore(path, &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		return "value-" + n.Key, nil
	}}, cp)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func pending(t *testing.T, err error) map[string]ApprovalDecision {
	t.Helper()
	info, ok := compose.ExtractInterruptInfo(err)
	if !ok {
		t.Fatalf("missing checkpoint interrupt: %v", err)
	}
	decisions := map[string]ApprovalDecision{}
	for _, ic := range info.InterruptContexts {
		if !ic.IsRootCause {
			continue
		}
		data, ok := ic.Info.(*ApprovalInfo)
		if !ok {
			t.Fatalf("approval payload type: %T", ic.Info)
		}
		decisions[ic.ID] = ApprovalDecision{Approved: true, ArgsHash: data.ArgsHash}
	}
	if len(decisions) == 0 {
		t.Fatal("empty interrupt contexts")
	}
	return decisions
}
func assertOnce(t *testing.T, calls *sync.Map, count int) {
	t.Helper()
	n := 0
	calls.Range(func(k, v any) bool {
		n++
		if v.(*atomic.Int32).Load() != 1 {
			t.Errorf("replayed completed node %v %v", k, v)
		}
		return true
	})
	if n != count {
		t.Fatalf("executed %d nodes, want %d", n, count)
	}
}

func TestGraphSequentialApprovals(t *testing.T) {
	text := `name: sequential
nodes:
 - {key: before, type: llm, prompt: first}
 - {key: review1, type: human, prompt: "review {{before.result}}"}
 - {key: middle, type: llm, prompt: middle}
 - {key: review2, type: human, prompt: review-again}
 - {key: after, type: llm, prompt: last}
`
	cp := &memoryCheckpoints{}
	calls := &sync.Map{}
	r := graphFixture(t, text, cp, calls, nil)
	_, err := r.Invoke(context.Background(), map[string]any{"topic": "test"}, compose.WithCheckPointID("run"))
	first := pending(t, err)
	if len(first) != 1 {
		t.Fatal("not first approval")
	}
	r = graphFixture(t, text, cp, calls, nil) // reconstruct the graph as a fresh worker would
	_, err = r.Invoke(compose.BatchResumeWithData(context.Background(), toAny(first)), nil, compose.WithCheckPointID("run"))
	second := pending(t, err)
	if len(second) != 1 {
		t.Fatal("not second approval")
	}
	for id := range second {
		if _, old := first[id]; old {
			t.Fatal("resumed first approval again")
		}
	}
	r = graphFixture(t, text, cp, calls, nil)
	result, err := r.Invoke(compose.BatchResumeWithData(context.Background(), toAny(second)), nil, compose.WithCheckPointID("run"))
	if err != nil {
		t.Fatal(err)
	}
	if result["after"] != "value-after" || result["topic"] != "test" {
		t.Fatalf("state lost: %+v", result)
	}
	assertOnce(t, calls, 3)
}
func toAny(in map[string]ApprovalDecision) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func TestGraphParallelApprovals(t *testing.T) {
	text := `name: parallel
nodes:
 - {key: fork, type: parallel, branches: [a,b,ready]}
 - {key: a, type: human, prompt: review-a}
 - {key: b, type: human, prompt: review-b}
 - {key: ready, type: llm, prompt: run-once}
 - {key: after, type: llm, prompt: after-join}
edges: [fork,after]
`
	cp := &memoryCheckpoints{}
	calls := &sync.Map{}
	r := graphFixture(t, text, cp, calls, nil)
	_, err := r.Invoke(context.Background(), map[string]any{}, compose.WithCheckPointID("parallel"))
	all := pending(t, err)
	if len(all) != 2 {
		t.Fatalf("lost concurrent approvals: %+v", all)
	}
	one := map[string]any{}
	for id, d := range all {
		one[id] = d
		break
	}
	r = graphFixture(t, text, cp, calls, nil)
	_, err = r.Invoke(compose.BatchResumeWithData(context.Background(), one), nil, compose.WithCheckPointID("parallel"))
	remaining := pending(t, err)
	if len(remaining) != 1 {
		t.Fatalf("wrong remaining approvals: %+v", remaining)
	}
	for id := range remaining {
		if _, approved := one[id]; approved {
			t.Fatal("approved branch asked again")
		}
	}
	r = graphFixture(t, text, cp, calls, nil)
	result, err := r.Invoke(compose.BatchResumeWithData(context.Background(), toAny(remaining)), nil, compose.WithCheckPointID("parallel"))
	if err != nil {
		t.Fatal(err)
	}
	if result["after"] != "value-after" {
		t.Fatal("join never executed")
	}
	assertOnce(t, calls, 2)
}

func TestGraphNestedApproval(t *testing.T) {
	child := `name: child
nodes:
 - {key: first, type: llm, prompt: x}
 - {key: review, type: human, prompt: child-review}
 - {key: last, type: llm, prompt: y}
`
	text := `name: parent
nodes:
 - {key: nested, type: subflow, workflow: 9}
 - {key: done, type: llm, prompt: done}
`
	lookup := func(ctx context.Context, id int64) (*DSL, error) {
		if id != 9 {
			return nil, fmt.Errorf("bad child")
		}
		return ParseDSL(child)
	}
	cp := &memoryCheckpoints{}
	calls := &sync.Map{}
	r := graphFixture(t, text, cp, calls, lookup)
	_, err := r.Invoke(context.Background(), map[string]any{"parent_input": "kept"}, compose.WithCheckPointID("nested"))
	resume := pending(t, err)
	r = graphFixture(t, text, cp, calls, lookup)
	out, err := r.Invoke(compose.BatchResumeWithData(context.Background(), toAny(resume)), nil, compose.WithCheckPointID("nested"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(out)
	if out["done"] != "value-done" || len(raw) == 0 {
		t.Fatal("subgraph output lost")
	}
	childState, ok := out["nested"].(map[string]any)
	if !ok || childState["last"] != "value-last" || childState["parent_input"] != "kept" {
		t.Fatalf("nested state was not restored: %+v", out)
	}
	assertOnce(t, calls, 3)
}

func TestGraphConditionsAndValidation(t *testing.T) {
	text := `name: route
nodes:
 - {key: route, type: condition, expr: "allowed == true", then: yes, else: no}
 - {key: yes, type: llm, prompt: accepted}
 - {key: no, type: llm, prompt: denied}
edges: [{from: route, to: yes, label: then}, {from: route, to: no, label: else}]
`
	cp := &memoryCheckpoints{}
	calls := &sync.Map{}
	r := graphFixture(t, text, cp, calls, nil)
	out, err := r.Invoke(context.Background(), map[string]any{"allowed": false}, compose.WithCheckPointID("route"))
	if err != nil {
		t.Fatal(err)
	}
	if out["no"] != "value-no" || out["yes"] != nil {
		t.Fatalf("wrong condition: %+v", out)
	}
	assertOnce(t, calls, 1)
	for _, invalid := range []string{
		"name: bad\nnodes: [{key: a, type: parallel, branches: [missing]}]",
		"name: bad\nnodes: [{key: a, type: condition, then: missing}]",
		"name: bad\nnodes: [{key: a, type: llm, timeout_s: -1}]",
		"name: bad\nnodes: [{key: a, type: parallel, branches: [a]}]",
		"name: bad\nunknown_field: hidden\nnodes: [{key: a, type: llm}]",
		"name: bad\nnodes: [{key: a, type: llm}, {key: b, type: llm}, {key: orphan, type: llm}]\nedges: [a,b]",
	} {
		if _, err := ParseDSL(invalid); err == nil {
			t.Errorf("accepted malformed graph: %s", invalid)
		}
	}
}

func TestGraphExplicitOutputAndTypedArguments(t *testing.T) {
	text := "name: output\nnodes: [{key: a, type: llm, prompt: x}, {key: b, type: llm, prompt: y}]\nout: a"
	cp := &memoryCheckpoints{}
	calls := &sync.Map{}
	r := graphFixture(t, text, cp, calls, nil)
	out, err := r.Invoke(context.Background(), map[string]any{}, compose.WithCheckPointID("out"))
	if err != nil || out["result"] != "value-a" {
		t.Fatalf("out node ignored: %+v %v", out, err)
	}
	args, err := RenderArgs(map[string]any{"count": "{{data.count}}", "nested": []any{"{{data.flag}}"}}, map[string]any{"data": map[string]any{"count": int64(9007199254740993), "flag": true}})
	if err != nil || args["count"] != int64(9007199254740993) || args["nested"].([]any)[0] != true {
		t.Fatalf("typed template changed arguments: %+v %v", args, err)
	}
}
