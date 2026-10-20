package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/cloudwego/eino/schema"
)

func validateInputBudget(messages []*schema.Message, tools []*schema.ToolInfo, budget int) error {
	if budget <= 0 {
		return errcode.ErrBudget
	}
	pending := map[string]bool{}
	total := 0
	for _, msg := range messages {
		converted, err := einoadapter.FromMessage(msg)
		if err != nil {
			return err
		}
		if msg.Role == schema.Tool {
			if !pending[msg.ToolCallID] {
				return fmt.Errorf("unpaired tool result")
			}
			delete(pending, msg.ToolCallID)
		} else if len(pending) > 0 {
			return fmt.Errorf("missing tool results before next message")
		}
		if len(msg.ToolCalls) > 0 {
			for _, call := range msg.ToolCalls {
				if call.ID == "" || pending[call.ID] {
					return fmt.Errorf("missing or duplicate tool call ID")
				}
				pending[call.ID] = true
			}
		}
		total += llm.EstimateMessages([]llm.Message{converted}) + 8
	}
	if len(pending) > 0 {
		return fmt.Errorf("tool call without matching result")
	}
	for _, tool := range tools {
		if tool == nil {
			return fmt.Errorf("nil tool definition")
		}
		var params any
		if tool.ParamsOneOf != nil {
			var err error
			params, err = tool.ParamsOneOf.ToJSONSchema()
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(map[string]any{"name": tool.Name, "description": tool.Desc, "parameters": params})
		if err != nil {
			return err
		}
		total += llm.EstimateTokens(string(raw)) + 16
	}
	if total > budget {
		return errcode.ErrBudget
	}
	return nil
}
