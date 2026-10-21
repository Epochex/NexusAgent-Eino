//go:build integration

package service

import (
	"context"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

func TestChatHistoryRestoresActualToolEvidence(t *testing.T) {
	f := pumpFixture(t)
	out, done := startPump(t, f, context.Background(), []agent.Event{
		agent.EvtReasoning("reason before parallel tools"),
		agent.EvtToolCall("call-a", "inspect_a", `{"region":"cn-east"}`),
		agent.EvtToolCall("call-b", "inspect_b", `{"region":"eu-west"}`),
		agent.EvtToolResult("call-b", `{"permit":"permit-b"}`, 1, "ok"),
		agent.EvtToolResult("call-a", `{"permit":"permit-a"}`, 1, "ok"),
		agent.EvtReasoning("reason after tools"),
		agent.EvtDelta("verified both"), agent.EvtDone("stop"),
	}, 32)
	for range out {
	}
	<-done
	row := assistant(t, f)
	history := historyToLLM([]model.Message{row})
	if len(history) != 4 {
		t.Fatalf("tool evidence lost on DB roundtrip: got %d messages, want assistant/tool/tool/assistant", len(history))
	}
	if history[0].Role != llm.RoleAssistant || len(history[0].ToolCalls) != 2 || history[0].ReasoningContent != "reason before parallel tools" {
		t.Fatalf("assistant protocol history lost: %+v", history[0])
	}
	if history[1].Role != llm.RoleTool || history[1].ToolCallID != "call-b" || history[1].Content != `{"permit":"permit-b"}` || history[2].ToolCallID != "call-a" {
		t.Fatal("parallel tool result linkage lost")
	}
	if history[3].Content != "verified both" || history[3].ReasoningContent != "reason after tools" {
		t.Fatal("final reasoning/content boundaries lost")
	}
}
