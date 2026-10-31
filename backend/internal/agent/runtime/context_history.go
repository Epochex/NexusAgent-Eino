package runtime

import (
	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/cloudwego/eino/schema"
)

// Counts are heuristic estimates, not provider billing or context guarantees.
type contextCompaction struct {
	RemovedTurns    int
	RemovedMessages int
	EstimatedBefore int
	EstimatedAfter  int
}

// compactInputHistory changes only the model's working view. It preserves all
// system messages, the prefix before the first user, and the latest user turn.
// Each old user turn is removed as a unit, including every parallel tool result.
// No summary is generated, and no persistent history/checkpoint is deleted.
func compactInputHistory(messages []*schema.Message, tools []*schema.ToolInfo, budget int) ([]*schema.Message, contextCompaction, error) {
	stats := contextCompaction{}
	if budget <= 0 {
		return nil, stats, errcode.ErrBudget
	}
	total, err := inputBudgetUsage(messages, tools)
	if err != nil {
		return nil, stats, err
	}
	stats.EstimatedBefore, stats.EstimatedAfter = total, total
	if total <= budget {
		return messages, stats, nil
	}
	var starts []int
	for i, message := range messages {
		if message.Role == schema.User {
			starts = append(starts, i)
		}
	}
	removed := make([]bool, len(messages))
	for turn := 0; turn+1 < len(starts) && total > budget; turn++ {
		for i := starts[turn]; i < starts[turn+1]; i++ {
			if messages[i].Role == schema.System {
				continue
			}
			converted, err := einoadapter.FromMessage(messages[i])
			if err != nil {
				return nil, stats, err
			}
			total -= llm.EstimateMessages([]llm.Message{converted}) + 8
			removed[i] = true
			stats.RemovedMessages++
		}
		stats.RemovedTurns++
	}
	stats.EstimatedAfter = total
	if total > budget {
		return nil, stats, errcode.ErrBudget
	}
	retained := make([]*schema.Message, 0, len(messages)-stats.RemovedMessages)
	for i, message := range messages {
		if !removed[i] {
			retained = append(retained, message)
		}
	}
	// This also prevents a future change to grouping/accounting from silently
	// producing malformed model input.
	if err := validateInputBudget(retained, tools, budget); err != nil {
		return nil, stats, err
	}
	return retained, stats, nil
}
