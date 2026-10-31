package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/cloudwego/eino/schema"
)

// Removing the first-request compactor must turn this into ErrBudget before
// any HTTP request. Inspect the actual gateway payload, not an ADK mock.
func TestEinoRunnerCompactsHistoryBeforeFirstModel(t *testing.T) {
	var requests atomic.Int32
	r, in := testRunner(t, func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		var body struct {
			Messages []llm.Message `json:"messages"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if len(body.Messages) != 3 || body.Messages[0].Content != "Keep the system rule" || body.Messages[1].Content != "Keep the later system rule" || body.Messages[2].Content != "Current task" {
			t.Errorf("protected context lost or old history retained: %+v", body.Messages)
		}
		answer(w, "done")
	}, runtimeTools{}, 2)
	in.ToolsJSON = "[]"
	r.cfg.InputTokenBudget = 100
	in.Messages = []llm.Message{
		{Role: llm.RoleSystem, Content: "Keep the system rule"},
		{Role: llm.RoleUser, Content: strings.Repeat("旧历史", 300)},
		{Role: llm.RoleSystem, Content: "Keep the later system rule"},
		{Role: llm.RoleAssistant, Content: "Old answer"},
		{Role: llm.RoleUser, Content: "Current task"},
	}
	before, _ := json.Marshal(in.Messages)
	events, err := r.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("old complete turns should be evicted instead of rejecting the run: %v", err)
	}
	var out []agent.Event
	for ev := range events {
		out = append(out, ev)
	}
	terminal(t, out, agent.TypeDone)
	after, _ := json.Marshal(in.Messages)
	if string(after) != string(before) {
		t.Fatal("canonical caller history mutated")
	}
	if requests.Load() != 1 {
		t.Fatalf("requests=%d", requests.Load())
	}
}

func TestEinoRunnerEvictsEntireHistoricalToolRound(t *testing.T) {
	r, in := testRunner(t, func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Messages []llm.Message `json:"messages"`
		}
		json.NewDecoder(req.Body).Decode(&body)
		if len(body.Messages) != 2 || body.Messages[0].Role != llm.RoleSystem || body.Messages[1].Content != "Current task" {
			t.Errorf("partial old tool round retained: %+v", body.Messages)
		}
		answer(w, "done")
	}, runtimeTools{}, 2)
	in.ToolsJSON = "[]"
	r.cfg.InputTokenBudget = 100
	in.Messages = []llm.Message{
		{Role: llm.RoleSystem, Content: "System"},
		{Role: llm.RoleUser, Content: strings.Repeat("历史", 200)},
		{Role: llm.RoleAssistant, ToolCalls: []llm.ToolCall{{ID: "a", Name: "calculate", ArgsJSON: "{}"}, {ID: "b", Name: "calculate", ArgsJSON: "{}"}}},
		{Role: llm.RoleTool, ToolCallID: "a", Content: "a result"},
		{Role: llm.RoleTool, ToolCallID: "b", Content: "b result"},
		{Role: llm.RoleAssistant, Content: "old conclusion"},
		{Role: llm.RoleUser, Content: "Current task"},
	}
	terminal(t, collect(t, r, in), agent.TypeDone)
}

func TestEinoRunnerPreservesProtectedContextRejection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		messages []llm.Message
	}{
		{"oversized current turn", []llm.Message{{Role: llm.RoleUser, Content: strings.Repeat("当前", 200)}}},
		{"oversized system", []llm.Message{{Role: llm.RoleSystem, Content: strings.Repeat("规则", 200)}, {Role: llm.RoleUser, Content: "old"}, {Role: llm.RoleAssistant, Content: "old"}, {Role: llm.RoleUser, Content: "new"}}},
		{"orphan in discardable history", []llm.Message{{Role: llm.RoleUser, Content: strings.Repeat("旧", 200)}, {Role: llm.RoleTool, ToolCallID: "missing", Content: "orphan"}, {Role: llm.RoleUser, Content: "new"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			r, in := testRunner(t, func(w http.ResponseWriter, req *http.Request) { requests.Add(1); answer(w, "must not call") }, runtimeTools{}, 2)
			r.cfg.InputTokenBudget = 100
			in.ToolsJSON = "[]"
			in.Messages = tc.messages
			before := append([]llm.Message(nil), in.Messages...)
			if _, err := r.Run(context.Background(), in); err == nil {
				t.Fatal("invalid or protected oversized context accepted")
			}
			if requests.Load() != 0 || !reflect.DeepEqual(before, in.Messages) {
				t.Fatal("model called or caller history changed")
			}
		})
	}
}

func TestEinoRunnerCompactsAgainAfterToolOutput(t *testing.T) {
	var requests atomic.Int32
	r, in := testRunner(t, func(w http.ResponseWriter, req *http.Request) {
		n := requests.Add(1)
		raw, _ := io.ReadAll(req.Body)
		var body struct {
			Messages []llm.Message `json:"messages"`
		}
		json.Unmarshal(raw, &body)
		if n == 1 {
			toolAnswer(w, 1)
			return
		}
		if len(body.Messages) != 4 || body.Messages[0].Content != "System" || body.Messages[1].Content != "calculate" || body.Messages[2].ToolCalls[0].ID != "call-0" || body.Messages[3].ToolCallID != "call-0" || body.Messages[3].Content != strings.Repeat("x", 200) {
			t.Errorf("tool round or current task lost: %+v", body.Messages)
		}
		answer(w, "done")
	}, runtimeTools{invoke: func(context.Context) string { return strings.Repeat("x", 200) }}, 4)
	r.cfg.InputTokenBudget = 400
	in.Messages = []llm.Message{{Role: llm.RoleSystem, Content: "System"}, {Role: llm.RoleUser, Content: strings.Repeat("旧", 300)}, {Role: llm.RoleAssistant, Content: "old"}, {Role: llm.RoleUser, Content: "calculate"}}
	terminal(t, collect(t, r, in), agent.TypeDone)
	if requests.Load() != 2 {
		t.Fatalf("expected tool loop to finish, requests=%d", requests.Load())
	}
}

func TestContextHistoryKeepsNewestCompleteTurnAndToolSchemaBudget(t *testing.T) {
	messages := []*schema.Message{schema.SystemMessage("System"), schema.UserMessage(strings.Repeat("旧", 200)), schema.AssistantMessage("old", nil), schema.UserMessage("recent"), schema.AssistantMessage("recent answer", nil), schema.UserMessage("current")}
	kept, stats, err := compactInputHistory(messages, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 4 || kept[1].Content != "recent" || kept[2].Content != "recent answer" || kept[3].Content != "current" || stats.RemovedTurns != 1 || stats.RemovedMessages != 2 {
		t.Fatalf("did not remove the minimum complete old history: %+v %+v", kept, stats)
	}
	tools := []*schema.ToolInfo{{Name: "large_catalogue", Desc: strings.Repeat("工具", 200)}}
	if _, _, err := compactInputHistory(messages, tools, 100); err == nil {
		t.Fatal("tool catalogue was excluded from the protected budget")
	}
	if len(messages) != 6 || messages[1].Content != strings.Repeat("旧", 200) {
		t.Fatal("history was edited in place")
	}
}

func TestContextHistoryRejectsUnfinishedOldToolRound(t *testing.T) {
	messages := []*schema.Message{schema.UserMessage(strings.Repeat("旧", 200)), {Role: schema.Assistant, ToolCalls: []schema.ToolCall{{ID: "unfinished", Function: schema.FunctionCall{Name: "calculate", Arguments: "{}"}}}}, schema.UserMessage("current")}
	if _, _, err := compactInputHistory(messages, nil, 100); err == nil {
		t.Fatal("discarded history concealed an unfinished call")
	}
}
