package runtime

import (
	"github.com/cloudwego/eino/schema"
	"strings"
	"testing"
)

func TestBudgetIncludesSystemRAGAndTools(t *testing.T) {
	if err := validateInputBudget([]*schema.Message{schema.SystemMessage(strings.Repeat("知识", 100)), schema.UserMessage("q")}, nil, 20); err == nil {
		t.Fatal("system/RAG escaped budget")
	}
	tools := []*schema.ToolInfo{{Name: "search", Desc: strings.Repeat("description", 100), ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{})}}
	if err := validateInputBudget([]*schema.Message{schema.UserMessage("q")}, tools, 20); err == nil {
		t.Fatal("tools escaped budget")
	}
}
func TestBudgetKeepsToolPairs(t *testing.T) {
	call := &schema.Message{Role: schema.Assistant, ToolCalls: []schema.ToolCall{{ID: "c", Function: schema.FunctionCall{Name: "tool", Arguments: "{}"}}}}
	if err := validateInputBudget([]*schema.Message{call}, nil, 128000); err == nil {
		t.Fatal("orphan call accepted")
	}
	if err := validateInputBudget([]*schema.Message{call, {Role: schema.Tool, ToolCallID: "c", Content: "ok"}}, nil, 128000); err != nil {
		t.Fatal(err)
	}
}
