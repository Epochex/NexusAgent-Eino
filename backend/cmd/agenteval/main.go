// agenteval demonstrates independent-oracle evaluation through the real Eino SDK.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent/evaluation"
	aruntime "github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
)

//go:embed demo.jsonl
var demoSuite string

// Fixture documents are independent of task oracles. The controlled peer can only
// read prompt messages and tool results; it cannot access the suite or its expected fields.
type documentTools struct{}

func (documentTools) ListContext(ctx context.Context, bindings string) ([]llm.ToolDef, error) {
	if bindings == "" || bindings == "[]" {
		return nil, ctx.Err()
	}
	return []llm.ToolDef{{Name: "lookup_doc", Description: "Read a development policy document", Parameters: map[string]any{"type": "object", "properties": map[string]any{"document": map[string]any{"type": "string", "enum": []string{"leases", "context", "operations"}}}, "required": []string{"document"}, "additionalProperties": false}}}, ctx.Err()
}
func (documentTools) Invoke(ctx context.Context, _, name, args string) tool.ToolResult {
	docs := map[string]string{"leases": `{"policy":"reject stale fencing token"}`, "context": `{"policy":"keep complete tool turns"}`, "operations": `{"policy":"reconcile before retry"}`}
	var p struct {
		Document string `json:"document"`
	}
	err := json.Unmarshal([]byte(args), &p)
	if err != nil || ctx.Err() != nil || name != "lookup_doc" || docs[p.Document] == "" {
		return tool.ToolResult{ToolName: name, Status: "error", Output: "unknown document or invalid request"}
	}
	return tool.ToolResult{ToolName: name, Status: "ok", Output: docs[p.Document]}
}
func controlledPeer(variant string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req struct {
			Messages []llm.Message `json:"messages"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || len(req.Messages) == 0 {
			http.Error(w, "invalid messages", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		last := req.Messages[len(req.Messages)-1]
		var delta map[string]any
		finish := "stop"
		prompt, completion := 5, 2
		if last.Role == llm.RoleTool {
			text := last.Content
			if variant == "degraded" && strings.Contains(text, "reconcile before retry") {
				text = `{"policy":"retry immediately"}`
			}
			delta = map[string]any{"content": text}
		} else {
			var p struct {
				Document string `json:"document"`
			}
			if err := json.Unmarshal([]byte(last.Content), &p); err != nil {
				http.Error(w, "expected document request", 400)
				return
			}
			args, _ := json.Marshal(p)
			delta = map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": "lookup-0", "type": "function", "function": map[string]string{"name": "lookup_doc", "arguments": string(args)}}}}
			finish = "tool_calls"
			prompt, completion = 3, 1
		}
		frame := map[string]any{"model": "controlled-peer", "choices": []any{map[string]any{"delta": delta, "finish_reason": finish}}, "usage": map[string]int{"prompt_tokens": prompt, "completion_tokens": completion}}
		b, _ := json.Marshal(frame)
		fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", b)
	}
}
func runDemo(ctx context.Context, s *evaluation.Suite, variant, revision string) (evaluation.Report, error) {
	if variant != "baseline" && variant != "degraded" {
		return evaluation.Report{}, errors.New("demo variant must be baseline or degraded")
	}
	server := httptest.NewServer(controlledPeer(variant))
	defer server.Close()
	gateway := llm.NewGateway(&config.LLM{MaxConnsPerProvider: 16})
	factory := func() (aruntime.Runner, error) {
		return aruntime.NewEinoRunner(gateway, documentTools{}, aruntime.EinoConfig{StepTimeout: 2 * time.Second, RunTimeout: 5 * time.Second, SendTimeout: time.Second})
	}
	return evaluation.Evaluate(ctx, s, factory, &llm.ModelDescriptor{BaseURL: server.URL, ModelName: "controlled-peer"}, evaluation.Options{Variant: variant, Revision: revision, Scope: "controlled-integration", Timeout: 6 * time.Second, MaxEvents: 100})
}
func main() {
	if err := command(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func command() error {
	demo := flag.Bool("demo", false, "use a controlled local HTTP model; no model-quality claim")
	variant := flag.String("variant", "baseline", "baseline or degraded controlled peer")
	revision := flag.String("revision", "", "code revision (required for evaluation)")
	suitePath := flag.String("suite", "", "JSONL suite; defaults to embedded integration fixtures")
	output := flag.String("output", "", "private report directory (required)")
	compareA := flag.String("compare-before", "", "first report.json")
	compareB := flag.String("compare-after", "", "second report.json")
	flag.Parse()
	if *compareA != "" || *compareB != "" {
		if *compareA == "" || *compareB == "" {
			return errors.New("both comparison reports required")
		}
		a, err := readReport(*compareA)
		if err != nil {
			return err
		}
		b, err := readReport(*compareB)
		if err != nil {
			return err
		}
		c, err := evaluation.Compare(a, b)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(c)
	}
	if !*demo || *revision == "" || *output == "" {
		return errors.New("use --demo --revision REV --output DIR; or --compare-before and --compare-after")
	}
	input := demoSuite
	if *suitePath != "" {
		raw, err := os.ReadFile(*suitePath)
		if err != nil {
			return err
		}
		input = string(raw)
	}
	s, err := evaluation.LoadSuite(strings.NewReader(input))
	if err != nil {
		return err
	}
	report, err := runDemo(context.Background(), s, *variant, *revision)
	if err != nil {
		return err
	}
	if err = evaluation.WriteReport(*output, report); err != nil {
		return err
	}
	fmt.Printf("scope=%s variant=%s passed=%d/%d suite=%s\n", report.Options.Scope, *variant, report.Passed, report.Total, report.SuiteSHA256)
	return nil
}
func readReport(path string) (evaluation.Report, error) {
	var r evaluation.Report
	raw, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(raw, &r)
	return r, err
}
