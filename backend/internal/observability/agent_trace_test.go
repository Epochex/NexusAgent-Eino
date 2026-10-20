package observability

import (
	"sync"
	"testing"
)

func TestTraceCompletionSurvivesGrowth(t *testing.T) {
	trace := NewAgentTrace()
	first := trace.Add(EventAction, "agent", "first")
	for range 100 {
		trace.Add(EventAction, "agent", "next")
	}
	trace.Done(first, "done", "ok")
	if got := trace.Snapshot()[0]; got.Status != "ok" || got.Result != "done" {
		t.Fatalf("completion updated obsolete slice storage: %+v", got)
	}
}

func TestTraceSnapshotOwnsArguments(t *testing.T) {
	trace := NewAgentTrace()
	trace.AddTool("agent", "search", `{"q":{"text":"original"}}`)
	first := trace.Snapshot()
	first[0].Args["q"].(map[string]any)["text"] = "mutated"
	if trace.Snapshot()[0].Args["q"].(map[string]any)["text"] != "original" {
		t.Fatal("snapshot mutated live trace")
	}
}

func TestTraceConcurrentCompletionsAndSnapshots(t *testing.T) {
	trace := NewAgentTrace()
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() { step := trace.AddTool("agent", "search", `{"q":"go"}`); trace.Done(step, "result", "ok") })
	}
	for range 30 {
		trace.Snapshot()
	}
	wg.Wait()
	steps := trace.Snapshot()
	if len(steps) != 30 {
		t.Fatal("lost steps")
	}
	for _, step := range steps {
		if step.Status != "ok" {
			t.Fatal("lost concurrent completion")
		}
	}
}
