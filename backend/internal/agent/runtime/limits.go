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
	total, err := inputBudgetUsage(messages, tools)
	if err != nil {
		return err
	}
	if total > budget {
		return errcode.ErrBudget
	}
	return nil
}

// Validate the entire protocol before any history can be discarded. The same
// accounting is used by both admission and history reduction.
func inputBudgetUsage(messages []*schema.Message, tools []*schema.ToolInfo) (int, error) {
	pending := map[string]bool{}
	total := 0
	for _, msg := range messages {
		converted, err := einoadapter.FromMessage(msg)
		if err != nil {
			return 0, err
		}
		if msg.Role == schema.Tool {
			if !pending[msg.ToolCallID] {
				return 0, fmt.Errorf("unpaired tool result")
			}
			delete(pending, msg.ToolCallID)
		} else if len(pending) > 0 {
			return 0, fmt.Errorf("missing tool results before next message")
		}
		if len(msg.ToolCalls) > 0 {
			for _, call := range msg.ToolCalls {
				if call.ID == "" || pending[call.ID] {
					return 0, fmt.Errorf("missing or duplicate tool call ID")
				}
				pending[call.ID] = true
			}
		}
		total += llm.EstimateMessages([]llm.Message{converted}) + 8
	}
	if len(pending) > 0 {
		return 0, fmt.Errorf("tool call without matching result")
	}
	for _, tool := range tools {
		if tool == nil {
			return 0, fmt.Errorf("nil tool definition")
		}
		var params any
		if tool.ParamsOneOf != nil {
			var err error
			params, err = tool.ParamsOneOf.ToJSONSchema()
			if err != nil {
				return 0, err
			}
		}
		raw, err := json.Marshal(map[string]any{"name": tool.Name, "description": tool.Desc, "parameters": params})
		if err != nil {
			return 0, err
		}
		total += llm.EstimateTokens(string(raw)) + 16
	}
	return total, nil
}
