package service

import (
	"encoding/json"
	"strings"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
)

// Collect the actual model/tool protocol alongside the display transcript.
// Reusing these observations as history never invokes a tool again.
type chatProtocolHistory struct {
	messages           []llm.Message
	turn               llm.Message
	names              map[string]string
	content, reasoning strings.Builder
}

func (h *chatProtocolHistory) flush() {
	if h.content.Len() > 0 || h.reasoning.Len() > 0 || len(h.turn.ToolCalls) > 0 {
		h.turn.Role = llm.RoleAssistant
		h.turn.Content, h.turn.ReasoningContent = h.content.String(), h.reasoning.String()
		h.messages = append(h.messages, h.turn)
		h.turn = llm.Message{}
		h.content.Reset()
		h.reasoning.Reset()
	}
}

func (h *chatProtocolHistory) add(ev agent.Event) {
	switch ev.Type {
	case agent.TypeDelta:
		h.content.WriteString(strVal(ev.Data["content"]))
	case agent.TypeReasoning:
		h.reasoning.WriteString(strVal(ev.Data["content"]))
	case agent.TypeToolCall:
		call := llm.ToolCall{ID: strVal(ev.Data["id"]), Name: strVal(ev.Data["name"]), ArgsJSON: strVal(ev.Data["args"])}
		h.turn.ToolCalls = append(h.turn.ToolCalls, call)
		if h.names == nil {
			h.names = map[string]string{}
		}
		h.names[call.ID] = call.Name
	case agent.TypeToolResult:
		h.flush()
		id := strVal(ev.Data["id"])
		h.messages = append(h.messages, llm.Message{Role: llm.RoleTool, ToolCallID: id, Name: h.names[id], Content: strVal(ev.Data["result"])})
	}
}

func (h *chatProtocolHistory) complete() []llm.Message {
	h.flush()
	if !validChatProtocolHistory(h.messages) {
		return nil
	}
	return h.messages
}

// Reject partial, corrupt or privileged-role metadata rather than synthesizing
// successful observations from a display trace or an interrupted run.
func validChatProtocolHistory(messages []llm.Message) bool {
	if len(messages) == 0 {
		return false
	}
	pending := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range messages {
		switch m.Role {
		case llm.RoleAssistant:
			if len(pending) > 0 {
				return false
			}
			for _, c := range m.ToolCalls {
				if c.ID == "" || c.Name == "" || seen[c.ID] || !json.Valid([]byte(c.ArgsJSON)) {
					return false
				}
				pending[c.ID], seen[c.ID] = true, true
			}
		case llm.RoleTool:
			if !pending[m.ToolCallID] || len(m.ToolCalls) > 0 {
				return false
			}
			delete(pending, m.ToolCallID)
		default:
			return false
		}
	}
	last := messages[len(messages)-1]
	return len(pending) == 0 && last.Role == llm.RoleAssistant && len(last.ToolCalls) == 0 && last.Content != ""
}
