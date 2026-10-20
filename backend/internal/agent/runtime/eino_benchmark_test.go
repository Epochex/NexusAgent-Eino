package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
)

func benchmarkEino(b *testing.B, withTool bool) {
	s := llmstub.New(b, func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []llm.Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			b.Error(err)
			return
		}
		if withTool && request.Messages[len(request.Messages)-1].Role != llm.RoleTool {
			toolAnswer(w, 1)
		} else {
			answer(w, "answer")
		}
	})
	r, err := NewEinoRunner(llm.NewGateway(&config.LLM{MaxConnsPerProvider: 16}), runtimeTools{}, EinoConfig{StepTimeout: time.Second, RunTimeout: 3 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	in := &RunInput{AgentName: "benchmark", Messages: []llm.Message{{Role: llm.RoleUser, Content: "question"}}, Primary: &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}}
	if withTool {
		in.ToolsJSON = `[{"tool_code":"calculate"}]`
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		events, err := r.Run(context.Background(), in)
		if err != nil {
			b.Fatal(err)
		}
		done := false
		for ev := range events {
			if ev.Type == agent.TypeError {
				b.Fatalf("runtime failed: %+v", ev)
			}
			if ev.Type == agent.TypeDone {
				done = true
			}
		}
		if !done {
			b.Fatal("missing terminal")
		}
	}
}
func BenchmarkEinoRunnerText(b *testing.B)          { benchmarkEino(b, false) }
func BenchmarkEinoRunnerToolRoundTrip(b *testing.B) { benchmarkEino(b, true) }
