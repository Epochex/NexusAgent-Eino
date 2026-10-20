//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
)

func TestDriverRestoresExactApprovalsAndFrozenGraph(t *testing.T) {
	s, db, owner, wf := fixture(t)
	definition := Definition{DSL: `name: approval-flow
nodes:
 - {key: before, type: llm, prompt: first}
 - {key: review1, type: human, prompt: first-review}
 - {key: middle, type: llm, prompt: middle}
 - {key: review2, type: human, prompt: second-review}
 - {key: done, type: llm, prompt: final}
`}
	raw, _ := json.Marshal(definition)
	queued, err := s.Enqueue(context.Background(), owner, wf, raw, json.RawMessage(`{"original":"preserved"}`), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int32
	deps := workflow.Deps{ExecuteNode: func(ctx context.Context, path string, n *workflow.Node, state map[string]any) (any, error) {
		effects.Add(1)
		return "result-" + n.Key, nil
	}}
	if err := NewDriver(s, deps).Execute(context.Background(), claim(t, s, "initial")); err != nil {
		t.Fatal(err)
	}
	first, err := s.Get(context.Background(), owner, queued.ID)
	if err != nil || first.Status != Waiting || len(first.Approvals) != 1 {
		t.Fatalf("initial approval not durable: %+v %v", first, err)
	}
	// Editing the live definition cannot change the frozen run or its child paths.
	db.Exec(`UPDATE workflows SET dsl='name: malicious-new-version' WHERE id=?`, wf)
	a := first.Approvals[0]
	if err = s.Decide(context.Background(), owner, queued.ID, a.InterruptID, a.ArgsHash, true); err != nil {
		t.Fatal(err)
	}
	if err = NewDriver(s, deps).Execute(context.Background(), claim(t, s, "restarted")); err != nil {
		t.Fatal(err)
	}
	second, err := s.Get(context.Background(), owner, queued.ID)
	if err != nil || second.Status != Waiting || len(second.Approvals) != 1 || second.Approvals[0].InterruptID == a.InterruptID {
		t.Fatalf("resumed wrong approval: %+v %v", second, err)
	}
	a = second.Approvals[0]
	if err = s.Decide(context.Background(), owner, queued.ID, a.InterruptID, a.ArgsHash, true); err != nil {
		t.Fatal(err)
	}
	if err = NewDriver(s, deps).Execute(context.Background(), claim(t, s, "third-process")); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(context.Background(), owner, queued.ID)
	if err != nil || done.Status != Succeeded || effects.Load() != 3 {
		t.Fatalf("completed work replayed or lost: %+v effects=%d %v", done, effects.Load(), err)
	}
	var out map[string]any
	json.Unmarshal(done.Output, &out)
	if out["done"] != "result-done" || out["original"] != "preserved" {
		t.Fatalf("restored output incorrect: %s", done.Output)
	}
}

func TestDriverParallelAndNestedCheckpoints(t *testing.T) {
	for _, scenario := range []struct {
		name, root, child string
		leaves            int
	}{
		{"parallel", `name: parallel
nodes:
 - {key: fork, type: parallel, branches: [a,b,ready]}
 - {key: a, type: human, prompt: approve-a}
 - {key: b, type: human, prompt: approve-b}
 - {key: ready, type: llm, prompt: once}
 - {key: final, type: llm, prompt: after-join}
edges: [fork,final]`, "", 2},
		{"nested", `name: parent
nodes:
 - {key: child, type: subflow, workflow: 9}
 - {key: final, type: llm, prompt: final}`, `name: child
nodes:
 - {key: before, type: llm, prompt: before}
 - {key: review1, type: human, prompt: child-one}
 - {key: review2, type: human, prompt: child-two}
 - {key: after, type: llm, prompt: after}`, 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, _, owner, wf := fixture(t)
			definition := Definition{DSL: scenario.root}
			if scenario.child != "" {
				definition.Subflows = map[int64]string{9: scenario.child}
			}
			raw, _ := json.Marshal(definition)
			run, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			var calls sync.Map
			deps := workflow.Deps{ExecuteNode: func(_ context.Context, path string, n *workflow.Node, _ map[string]any) (any, error) {
				counter, _ := calls.LoadOrStore(path, &atomic.Int32{})
				counter.(*atomic.Int32).Add(1)
				return "value-" + n.Key, nil
			}}
			for turn := 0; turn < 6; turn++ {
				if err := NewDriver(s, deps).Execute(context.Background(), claim(t, s, "worker")); err != nil {
					t.Fatal(err)
				}
				got, err := s.Get(context.Background(), owner, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Status == Succeeded {
					count := 0
					calls.Range(func(key, value any) bool {
						count++
						if value.(*atomic.Int32).Load() != 1 {
							t.Errorf("replayed %v", key)
						}
						return true
					})
					if count != scenario.leaves {
						t.Fatalf("missing leaf work: %d", count)
					}
					return
				}
				if got.Status != Waiting {
					t.Fatalf("wrong intermediate state %s: %s", got.Status, got.Error)
				}
				var selected *Approval
				for i := range got.Approvals {
					if got.Approvals[i].Decision == "pending" {
						selected = &got.Approvals[i]
						break
					}
				}
				if selected == nil {
					t.Fatal("waiting without pending approval")
				}
				if err = s.Decide(context.Background(), owner, run.ID, selected.InterruptID, selected.ArgsHash, true); err != nil {
					t.Fatal(err)
				}
			}
			t.Fatal("approval loop did not complete")
		})
	}
}

type driverTools struct{}

func (driverTools) ListContext(context.Context, string) ([]llm.ToolDef, error) {
	return []llm.ToolDef{{Name: "write", Parameters: map[string]any{"type": "object"}, ReplayPolicy: "manual"}}, nil
}
func (d driverTools) List(s string) ([]llm.ToolDef, error) {
	return d.ListContext(context.Background(), s)
}
func (driverTools) Invoke(context.Context, string, string, string) tool.ToolResult {
	return tool.ToolResult{Status: "error"}
}

func TestDriverReconciliationAfterApprovalDoesNotLoseResume(t *testing.T) {
	s, _, owner, wf := fixture(t)
	raw, _ := json.Marshal(Definition{DSL: `name: approve-write
nodes:
 - {key: review, type: human, prompt: approve-write}
 - {key: write, type: tool, tool_code: write}
 - {key: done, type: llm, prompt: completed}
`})
	run, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	deps := workflow.Deps{Tools: driverTools{}, ExecuteNode: func(_ context.Context, _ string, n *workflow.Node, _ map[string]any) (any, error) {
		if n.Type == "tool" {
			writes.Add(1)
			return nil, errors.New("connection lost after external write")
		}
		return "finished", nil
	}}
	driver := NewDriver(s, deps)
	if err = driver.Execute(context.Background(), claim(t, s, "first")); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(context.Background(), owner, run.ID)
	a := got.Approvals[0]
	if err = s.Decide(context.Background(), owner, run.ID, a.InterruptID, a.ArgsHash, true); err != nil {
		t.Fatal(err)
	}
	if err = driver.Execute(context.Background(), claim(t, s, "approved")); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(context.Background(), owner, run.ID)
	if got.Status != NeedsAttention {
		t.Fatalf("unknown effect not blocked: %+v", got)
	}
	ops, err := s.Operations(context.Background(), owner, run.ID)
	if err != nil || len(ops) != 1 || ops[0].Status != "unknown" {
		t.Fatalf("missing unknown operation: %+v %v", ops, err)
	}
	if err = s.ResolveOperation(context.Background(), owner, run.ID, ops[0].ID, ops[0].ArgsHash, `"actual remote result"`, "remote audit record checked"); err != nil {
		t.Fatal(err)
	}
	if err = NewDriver(s, deps).Execute(context.Background(), claim(t, s, "reconciled")); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(context.Background(), owner, run.ID)
	if got.Status != Succeeded || writes.Load() != 1 {
		t.Fatalf("reconciliation lost original approval or repeated effect: status=%s writes=%d", got.Status, writes.Load())
	}
}

func TestDriverRejectsIncompatibleRuntime(t *testing.T) {
	s, db, owner, wf := fixture(t)
	raw, _ := json.Marshal(Definition{DSL: "name: test\nnodes: [{key: review, type: human, prompt: review}]"})
	run, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE workflow_runs SET engine_version='eino-old' WHERE id=?`, run.ID)
	if err = NewDriver(s, workflow.Deps{}).Execute(context.Background(), claim(t, s, "new-runtime")); err != nil {
		t.Fatalf("incompatible run not classified: %v", err)
	}
	got, err := s.Get(context.Background(), owner, run.ID)
	if err != nil || got.Status != NeedsAttention {
		t.Fatalf("incompatible runtime remained executable: %+v %v", got, err)
	}
	if err = s.Cancel(context.Background(), owner, run.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(context.Background(), owner, run.ID)
	if got.Status != Canceled {
		t.Fatal("blocked run cannot be canceled")
	}
}

func TestRunFailureBoundsUTF8(t *testing.T) {
	s, _, owner, wf := fixture(t)
	enqueue(t, s, owner, wf)
	run := claim(t, s, "worker")
	if err := s.Finish(context.Background(), run, Failed, nil, strings.Repeat("故障", 200)); err != nil {
		t.Fatalf("failure recording broke on Unicode: %v", err)
	}
}
