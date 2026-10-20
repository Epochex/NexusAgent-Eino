package einoadapter

import (
	"reflect"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/cloudwego/eino/schema"
)

func TestMessageRoundTripToolCalls(t *testing.T) {
	input := []llm.Message{
		{Role: llm.RoleSystem, Content: "system"},
		{Role: llm.RoleUser, Content: "question", Name: "user"},
		{Role: llm.RoleAssistant, Content: "action", ReasoningContent: "reason", ToolCalls: []llm.ToolCall{{ID: "c1", Name: "search", ArgsJSON: `{"q":"go"}`}}},
		{Role: llm.RoleTool, Content: "result", ToolCallID: "c1", Name: "search"},
	}
	converted, err := ToMessages(input)
	if err != nil {
		t.Fatal(err)
	}
	var output []llm.Message
	for _, m := range converted {
		next, err := FromMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		output = append(output, next)
	}
	if !reflect.DeepEqual(output, input) {
		t.Fatalf("round trip changed tool history: %+v", output)
	}
}

func TestMessageRejectsUnsupportedInput(t *testing.T) {
	if _, err := FromMessage(&schema.Message{Role: schema.User, MultiContent: []schema.ChatMessagePart{{Type: schema.ChatMessagePartTypeText, Text: "multimodal"}}}); err == nil {
		t.Fatal("unsupported content silently dropped")
	}
	if _, err := FromMessage(&schema.Message{Role: "invalid"}); err == nil {
		t.Fatal("invalid role accepted")
	}
}
