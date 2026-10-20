package llm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
)

func TestContextBudgetIncludesToolsResultsAndReservedOutput(t *testing.T) {
	var calls atomic.Int32
	peer := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.Contains(r.Header.Get("Accept"), "event-stream") {
			w.Header().Set("Content-Type", "text/event-stream")
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"must not reach upstream"},"finish_reason":"stop"}]}`)
	})
	for _, stream := range []bool{false, true} {
		for _, kind := range []string{"tools", "tool_result", "output_reservation"} {
			t.Run(fmt.Sprintf("%s/stream=%v", kind, stream), func(t *testing.T) {
				g := NewGateway(&config.LLM{MaxConnsPerProvider: 2})
				desc := &ModelDescriptor{ProviderCode: "budget", BaseURL: peer.URL, ModelName: "budget", ContextWindow: 512, MaxTokens: 128}
				req := &ChatRequest{Messages: []Message{{Role: RoleUser, Content: "q"}}}
				switch kind {
				case "tools":
					req.Tools = []ToolDef{{Name: "inspect", Description: strings.Repeat("large schema ", 200), Parameters: map[string]any{"type": "object"}}}
				case "tool_result":
					req.Messages = append(req.Messages, Message{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "call", Name: "inspect", ArgsJSON: "{}"}}}, Message{Role: RoleTool, ToolCallID: "call", Content: strings.Repeat("result ", 200)})
				case "output_reservation":
					desc.MaxTokens = 512
				}
				before := calls.Load()
				var err error
				if stream {
					_, _, err = g.ChatStream(context.Background(), desc, nil, req)
				} else {
					_, _, err = g.Chat(context.Background(), desc, nil, req)
				}
				if err == nil || calls.Load() != before {
					t.Errorf("budget escaped to upstream: err=%v calls=%d", err, calls.Load()-before)
				}
			})
		}
	}
}
