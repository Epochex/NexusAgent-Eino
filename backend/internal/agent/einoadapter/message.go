package einoadapter

import (
	"fmt"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/cloudwego/eino/schema"
)

func ToMessages(input []llm.Message) ([]*schema.Message, error) {
	output := make([]*schema.Message, 0, len(input))
	for _, m := range input {
		if !validRole(m.Role) {
			return nil, fmt.Errorf("unsupported message role %q", m.Role)
		}
		next := &schema.Message{Role: schema.RoleType(m.Role), Content: m.Content, ReasoningContent: m.ReasoningContent, Name: m.Name, ToolCallID: m.ToolCallID}
		if m.Role == llm.RoleTool {
			next.ToolName = m.Name
		}
		for i, tc := range m.ToolCalls {
			next.ToolCalls = append(next.ToolCalls, schema.ToolCall{Index: new(i), ID: tc.ID, Type: "function", Function: schema.FunctionCall{Name: tc.Name, Arguments: tc.ArgsJSON}})
		}
		output = append(output, next)
	}
	return output, nil
}

func FromMessage(m *schema.Message) (llm.Message, error) {
	if m == nil {
		return llm.Message{}, fmt.Errorf("message is nil")
	}
	if !validRole(string(m.Role)) {
		return llm.Message{}, fmt.Errorf("unsupported message role %q", m.Role)
	}
	if len(m.MultiContent)+len(m.UserInputMultiContent)+len(m.AssistantGenMultiContent) > 0 {
		return llm.Message{}, fmt.Errorf("multimodal messages are not supported by this gateway")
	}
	next := llm.Message{Role: string(m.Role), Content: m.Content, ReasoningContent: m.ReasoningContent, Name: m.Name, ToolCallID: m.ToolCallID}
	if next.Name == "" {
		next.Name = m.ToolName
	}
	for _, tc := range m.ToolCalls {
		next.ToolCalls = append(next.ToolCalls, llm.ToolCall{ID: tc.ID, Name: tc.Function.Name, ArgsJSON: tc.Function.Arguments})
	}
	return next, nil
}

func validRole(role string) bool {
	switch role {
	case llm.RoleSystem, llm.RoleUser, llm.RoleAssistant, llm.RoleTool:
		return true
	default:
		return false
	}
}
