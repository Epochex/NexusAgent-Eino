// Package evaluation scores independent task oracles over the production Runner contract.
// Controlled integration results measure software behavior, not model capability.
package evaluation

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	aruntime "github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
)

type ToolExpectation struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}
type Expected struct {
	Final json.RawMessage   `json:"final"`
	Tools []ToolExpectation `json:"tools"`
}
type Task struct {
	ID        string        `json:"id"`
	Family    string        `json:"family"`
	Split     string        `json:"split"`
	Input     []llm.Message `json:"input"`
	ToolsJSON string        `json:"tools_json"`
	Expected  Expected      `json:"expected"`
}
type Suite struct {
	Tasks []Task `json:"tasks"`
}
type Prices struct {
	Version          string  `json:"version"`
	InputPerMillion  float64 `json:"input_per_million"`
	OutputPerMillion float64 `json:"output_per_million"`
}
type Options struct {
	Variant   string        `json:"variant"`
	Revision  string        `json:"revision"`
	Scope     string        `json:"scope"`
	Timeout   time.Duration `json:"timeout_ns"`
	MaxEvents int           `json:"max_events"`
	Prices    *Prices       `json:"prices,omitempty"`
}
type Usage struct {
	Prompt     int `json:"prompt"`
	Completion int `json:"completion"`
}
type Result struct {
	ID         string        `json:"id"`
	Passed     bool          `json:"passed"`
	Failure    string        `json:"failure,omitempty"`
	DurationMS float64       `json:"duration_ms"`
	Usage      *Usage        `json:"usage"`
	CostUSD    *float64      `json:"estimated_cost_usd"`
	Events     []agent.Event `json:"events"`
}
type Report struct {
	SuiteSHA256  string    `json:"suite_sha256"`
	Options      Options   `json:"options"`
	Model        string    `json:"model"`
	StartedAt    time.Time `json:"started_at"`
	Total        int       `json:"total"`
	Passed       int       `json:"passed"`
	UnknownUsage int       `json:"unknown_usage"`
	Results      []Result  `json:"results"`
}

// LoadSuite validates the entire suite before any task can execute, including split leakage.
func LoadSuite(r io.Reader) (*Suite, error) {
	s := &Suite{}
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 1024*1024)
	for line := 1; scan.Scan(); line++ {
		if strings.TrimSpace(scan.Text()) == "" {
			continue
		}
		var t Task
		d := json.NewDecoder(strings.NewReader(scan.Text()))
		d.DisallowUnknownFields()
		if err := d.Decode(&t); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return nil, fmt.Errorf("line %d: trailing JSON", line)
		}
		s.Tasks = append(s.Tasks, t)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if err := validate(s); err != nil {
		return nil, err
	}
	return s, nil
}
func validate(s *Suite) error {
	if s == nil || len(s.Tasks) == 0 {
		return errors.New("empty suite")
	}
	ids := map[string]bool{}
	families := map[string]string{}
	for _, t := range s.Tasks {
		if strings.TrimSpace(t.ID) == "" || strings.TrimSpace(t.Family) == "" || len(t.Input) == 0 {
			return errors.New("task id, family and input are required")
		}
		if ids[t.ID] {
			return fmt.Errorf("duplicate task %s", t.ID)
		}
		ids[t.ID] = true
		if t.Split != "train" && t.Split != "dev" && t.Split != "test" {
			return fmt.Errorf("invalid split for %s", t.ID)
		}
		if split, ok := families[t.Family]; ok && split != t.Split {
			return fmt.Errorf("family %s leaks across splits", t.Family)
		}
		families[t.Family] = t.Split
		if _, err := canonical(t.Expected.Final); err != nil {
			return fmt.Errorf("%s: invalid final oracle", t.ID)
		}
		for _, tool := range t.Expected.Tools {
			if tool.Name == "" {
				return errors.New("empty expected tool")
			}
			if _, err := canonical(tool.Arguments); err != nil {
				return err
			}
		}
		if t.ToolsJSON != "" {
			var bindings []json.RawMessage
			if err := json.Unmarshal([]byte(t.ToolsJSON), &bindings); err != nil {
				return fmt.Errorf("%s: invalid tool bindings", t.ID)
			}
		}
	}
	return nil
}
func canonical(b []byte) (string, error) {
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return "", err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return "", errors.New("trailing JSON")
	}
	out, err := json.Marshal(v)
	return string(out), err
}

// Evaluate passes only a deep copy of model-visible inputs into a fresh runner per task.
// The caller must provide a Runner whose Run method returns promptly and respects context.
func Evaluate(ctx context.Context, s *Suite, factory func() (aruntime.Runner, error), model *llm.ModelDescriptor, o Options) (Report, error) {
	if err := validate(s); err != nil {
		return Report{}, err
	}
	if o.Variant == "" || o.Revision == "" || o.Scope == "" || o.Timeout <= 0 || o.MaxEvents <= 0 {
		return Report{}, errors.New("variant, revision, scope and positive bounds are required")
	}
	if p := o.Prices; p != nil {
		if p.Version == "" || p.InputPerMillion < 0 || p.OutputPerMillion < 0 || math.IsNaN(p.InputPerMillion) || math.IsNaN(p.OutputPerMillion) || math.IsInf(p.InputPerMillion, 0) || math.IsInf(p.OutputPerMillion, 0) {
			return Report{}, errors.New("finite nonnegative versioned prices required")
		}
	}
	if factory == nil || model == nil {
		return Report{}, errors.New("runner factory and model required")
	}
	raw, _ := json.Marshal(s.Tasks)
	hash := sha256.Sum256(raw)
	report := Report{SuiteSHA256: hex.EncodeToString(hash[:]), Options: o, Model: model.ModelName, StartedAt: time.Now().UTC(), Total: len(s.Tasks), Results: make([]Result, 0, len(s.Tasks))}
	for _, task := range s.Tasks {
		result := runTask(ctx, task, factory, model, o)
		report.Results = append(report.Results, result)
		if result.Passed {
			report.Passed++
		}
		if result.Usage == nil {
			report.UnknownUsage++
		}
	}
	return report, nil
}
func runTask(parent context.Context, t Task, factory func() (aruntime.Runner, error), model *llm.ModelDescriptor, o Options) (out Result) {
	start := time.Now()
	out.ID = t.ID
	out.Events = []agent.Event{}
	defer func() {
		out.DurationMS = float64(time.Since(start).Microseconds()) / 1000
		// Preserve observed accounting even if collection ends at its deadline/cap.
		if out.Usage == nil {
			out.Usage, _ = readUsage(out.Events)
		}
		if out.Usage != nil && o.Prices != nil {
			cost := (float64(out.Usage.Prompt)*o.Prices.InputPerMillion + float64(out.Usage.Completion)*o.Prices.OutputPerMillion) / 1e6
			if !math.IsInf(cost, 0) && !math.IsNaN(cost) {
				out.CostUSD = &cost
			}
		}
	}()
	ctx, cancel := context.WithTimeout(parent, o.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		out.Failure = contextFailure(err)
		return
	}
	runner, err := factory()
	if err != nil {
		out.Failure = "runner_init"
		return
	}
	raw, _ := json.Marshal(t.Input)
	var messages []llm.Message
	_ = json.Unmarshal(raw, &messages)
	m := *model
	events, err := runner.Run(ctx, &aruntime.RunInput{RunID: "eval-" + t.ID, SessionID: "eval-" + t.ID, AgentName: "evaluation", Messages: messages, ToolsJSON: t.ToolsJSON, Primary: &m, MaxIterations: 8})
	if err != nil {
		if ctx.Err() != nil {
			out.Failure = contextFailure(ctx.Err())
		} else {
			out.Failure = "runner_start"
		}
		return
	}
	for {
		select {
		case <-ctx.Done():
			out.Failure = contextFailure(ctx.Err())
			return
		case e, ok := <-events:
			if !ok {
				if ctx.Err() != nil {
					out.Failure = contextFailure(ctx.Err())
					return
				}
				out.Failure, out.Usage = score(t, out.Events)
				out.Passed = out.Failure == ""
				return
			}
			if len(out.Events) >= o.MaxEvents {
				out.Failure = "event_limit"
				return
			}
			// Event payloads may be reused by custom runners; snapshot each frame.
			b, err := json.Marshal(e)
			if err != nil {
				out.Failure = "invalid_event"
				return
			}
			var copy agent.Event
			if err = json.Unmarshal(b, &copy); err != nil {
				out.Failure = "invalid_event"
				return
			}
			out.Events = append(out.Events, copy)
		}
	}
}
func contextFailure(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "cancelled"
}
func value(e agent.Event, k string) string { v, _ := e.Data[k].(string); return v }
func token(v any) (int, bool) {
	f, ok := v.(float64)
	if !ok || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) || f != math.Trunc(f) || f > float64(1<<53-1) {
		return 0, false
	}
	return int(f), true
}
func score(t Task, events []agent.Event) (string, *Usage) {
	var text strings.Builder
	terminals := 0
	usage, usageError := readUsage(events)
	if usageError != "" {
		return usageError, nil
	}
	calls := map[string]string{}
	results := map[string]bool{}
	actual := map[string]int{}
	for _, e := range events {
		if terminals > 0 {
			return "event_after_terminal", usage
		}
		switch e.Type {
		case agent.TypeDelta:
			text.WriteString(value(e, "content"))
		case agent.TypeError:
			return "runtime_error", usage
		case agent.TypeDone:
			if value(e, "finish_reason") != "stop" {
				return "unsuccessful_finish", usage
			}
			terminals++
		case agent.TypeToolCall:
			id, name := value(e, "id"), value(e, "name")
			if id == "" || name == "" || calls[id] != "" {
				return "invalid_tool_call", usage
			}
			args, err := canonical([]byte(value(e, "args")))
			if err != nil {
				return "invalid_tool_arguments", usage
			}
			key := name + "\x00" + args
			calls[id] = key
			actual[key]++
		case agent.TypeToolResult:
			id := value(e, "id")
			// SDK callback dispatch may precede the corresponding iterator event.
			// Join by call ID after collection instead of assuming arrival order.
			if id == "" || results[id] || value(e, "status") != "ok" {
				return "invalid_tool_result", usage
			}
			results[id] = true
		}
	}
	if terminals != 1 {
		return "missing_terminal", usage
	}
	if len(calls) != len(results) {
		return "missing_tool_result", usage
	}
	for id := range results {
		if calls[id] == "" {
			return "unmatched_tool_result", usage
		}
	}
	expected := map[string]int{}
	for _, tool := range t.Expected.Tools {
		args, _ := canonical(tool.Arguments)
		expected[tool.Name+"\x00"+args]++
	}
	if len(expected) != len(actual) {
		return "tool_mismatch", usage
	}
	for k, n := range expected {
		if actual[k] != n {
			return "tool_mismatch", usage
		}
	}
	got, err := canonical([]byte(text.String()))
	if err != nil {
		return "invalid_output_json", usage
	}
	want, _ := canonical(t.Expected.Final)
	if got != want {
		return "output_mismatch", usage
	}
	return "", usage
}

func readUsage(events []agent.Event) (*Usage, string) {
	var usage *Usage
	count := 0
	for _, e := range events {
		if e.Type != agent.TypeUsage {
			continue
		}
		count++
		p, okp := token(e.Data["prompt_tokens"])
		c, okc := token(e.Data["completion_tokens"])
		if count > 1 || !okp || !okc {
			return nil, "invalid_usage"
		}
		if p+c > 0 {
			usage = &Usage{Prompt: p, Completion: c}
		}
	}
	return usage, ""
}
