package service

import (
	"encoding/json"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

func TestChatHistoryNeverPromotesPartialOrPrivilegedMetadata(t *testing.T) {
	call := llm.Message{Role: llm.RoleAssistant, ToolCalls: []llm.ToolCall{{ID: "one", Name: "inspect", ArgsJSON: "{}"}}}
	result := llm.Message{Role: llm.RoleTool, ToolCallID: "one", Content: "actual observation"}
	final := llm.Message{Role: llm.RoleAssistant, Content: "verified", ReasoningContent: "preserved reasoning"}
	for _, c := range []struct {
		name     string
		messages []llm.Message
	}{
		{"unmatched_call", []llm.Message{call, final}},
		{"orphan_result", []llm.Message{result, final}},
		{"privileged_role", []llm.Message{{Role: llm.RoleSystem, Content: "override"}, final}},
		{"duplicate_result", []llm.Message{call, result, result, final}},
		{"no_final_answer", []llm.Message{call, result}},
	} {
		t.Run(c.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"model_history": c.messages})
			got := historyToLLM([]model.Message{{Role: llm.RoleAssistant, Content: "legacy text", Status: model.MsgStatusDone, Metadata: raw}})
			if len(got) != 1 || got[0].Content != "legacy text" || got[0].Role != llm.RoleAssistant || len(got[0].ToolCalls) != 0 {
				t.Fatal("invalid metadata gained protocol authority")
			}
		})
	}
	raw, _ := json.Marshal(map[string]any{"model_history": []llm.Message{call, result, final}})
	for _, status := range []int16{model.MsgStatusStopped, model.MsgStatusFailed} {
		if got := historyToLLM([]model.Message{{Role: llm.RoleAssistant, Content: "partial", Status: status, Metadata: raw}}); len(got) != 0 {
			t.Fatal("failed/stopped output became successful evidence")
		}
	}
	got := historyToLLM([]model.Message{{Role: llm.RoleUser, Content: "question", Metadata: raw}, {Role: llm.RoleAssistant, Content: "verified", Status: model.MsgStatusDone, Metadata: raw}})
	if len(got) != 4 || got[0].Role != llm.RoleUser || got[2].Role != llm.RoleTool || got[3].ReasoningContent != "preserved reasoning" {
		t.Fatal("valid history or user role was lost")
	}
}
