//go:build integration

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	agentruntime "github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

func TestMCPRestoredRegistryThroughActualEinoAgent(t *testing.T) {
	svcs, ctx, _, srv, calls := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(ctx, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	reviewed, err := svcs.MCP.UpdateToolPolicy(ctx, srv.ID, rows[0].ID, &MCPToolPolicyInput{BindingVersion: rows[0].BindingVersion, ReplayPolicy: "readonly", Evidence: "reviewed diagnostic peer without side effects"})
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	deps := svcs.Deps
	deps.Registry = registry
	deps.Executor = tool.NewExecutor(svcs.Repos.Tool, registry, tool.ExecutorConfig{})
	restarted, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.MCP.RestoreRegistry(ctx); err != nil {
		t.Fatal(err)
	}
	var rounds atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Messages []llm.Message `json:"messages"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if rounds.Add(1) == 1 {
			frame := map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "inspect-call", "type": "function", "function": map[string]string{"name": reviewed.Code, "arguments": "{}"}}}}, "finish_reason": "tool_calls"}}}
			raw, _ := json.Marshal(frame)
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", raw)
			return
		}
		last := input.Messages[len(input.Messages)-1]
		if last.Role != llm.RoleTool || last.Content != "confirmed-status" {
			t.Error("actual MCP response not fed back to model", last)
		}
		fmt.Fprint(w, `data: {"choices":[{"delta":{"content":"Diagnostic confirmed healthy"},"finish_reason":"stop"}]}`+"\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(peer.Close)
	runner, err := agentruntime.NewEinoRunner(llm.NewGateway(&config.LLM{MaxConnsPerProvider: 2}), restarted.Executor, agentruntime.EinoConfig{StepTimeout: 5 * time.Second, RunTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	events, err := runner.Run(runCtx, &agentruntime.RunInput{SessionID: "mcp-acceptance", AgentName: "MCPAcceptance", Messages: []llm.Message{{Role: llm.RoleUser, Content: "Inspect service status"}}, ToolsJSON: fmt.Sprintf(`[{"tool_id":%d}]`, reviewed.ID), Primary: &llm.ModelDescriptor{BaseURL: peer.URL, ModelName: "controlled-peer"}, MaxIterations: 4})
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var results, done int
	for event := range events {
		switch event.Type {
		case agent.TypeDelta:
			if value, ok := event.Data["content"].(string); ok {
				text.WriteString(value)
			}
		case agent.TypeToolResult:
			results++
		case agent.TypeDone:
			done++
		case agent.TypeError:
			t.Fatal("Eino Agent failed", event)
		}
	}
	if calls.Load() != 1 || rounds.Load() != 2 || results != 1 || done != 1 || text.String() != "Diagnostic confirmed healthy" {
		t.Fatal("incomplete model-tool-model Eino execution", calls.Load(), rounds.Load(), results, done, text.String())
	}
}
