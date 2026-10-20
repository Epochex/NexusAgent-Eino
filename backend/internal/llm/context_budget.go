package llm

import (
	"encoding/json"
	"fmt"

	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// boundChatDescriptor runs for every model attempt, including fallbacks and
// later tool-result turns. It copies state and reserves the requested output.
// The context gate deliberately uses serialized UTF-8 bytes plus framing as a
// conservative local budget, not an exact/provider-independent token counter.
func boundChatDescriptor(desc *ModelDescriptor, req *ChatRequest) (*ModelDescriptor, error) {
	if desc == nil || req == nil || desc.ContextWindow < 0 || desc.MaxTokens < 0 || desc.MaxOutputTokens < 0 || req.MaxTokens < 0 {
		return nil, fmt.Errorf("invalid model descriptor or request")
	}
	bounded := *desc
	limit := desc.MaxOutputTokens
	if limit == 0 {
		limit = desc.MaxTokens
	} // pre-P4 immutable workflow descriptors
	requested := desc.MaxTokens
	if req.MaxTokens > 0 {
		requested = req.MaxTokens
	}
	if requested == 0 {
		requested = 1024
		if desc.ContextWindow > 0 {
			requested = max(1, min(requested, desc.ContextWindow/4))
		}
	}
	if limit > 0 {
		requested = min(requested, limit)
	}
	bounded.MaxTokens = requested
	if desc.ContextWindow > 0 {
		raw, err := json.Marshal(openaiRequest{Model: desc.ModelName, Messages: toOpenAIMsgs(req.Messages), Tools: toOpenAITools(req.Tools)})
		if err != nil {
			return nil, err
		}
		framing := 64 + 16*len(req.Messages) + 32*len(req.Tools)
		if requested >= desc.ContextWindow || len(raw) > desc.ContextWindow-requested-framing {
			return nil, errcode.ErrBudget.WithMsg("消息、工具和预留输出超过模型上下文预算")
		}
	}
	return &bounded, nil
}
