package evaluation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	aruntime "github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
)

type runnerFunc func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error)

func (f runnerFunc) Run(c context.Context, in *aruntime.RunInput) (<-chan agent.Event, error) {
	return f(c, in)
}
func stream(events ...agent.Event) <-chan agent.Event {
	ch := make(chan agent.Event, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	return ch
}
func suite(t *testing.T) *Suite {
	t.Helper()
	s, e := LoadSuite(strings.NewReader(`{"id":"task-a","family":"lease","split":"test","input":[{"role":"user","content":"inspect lease"}],"tools_json":"[]","expected":{"final":{"answer":"oracle-only"},"tools":[]}}`))
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func opts() Options {
	return Options{Variant: "current", Revision: "test-rev", Timeout: 100 * time.Millisecond, MaxEvents: 20, Scope: "controlled-integration"}
}
func execute(t *testing.T, s *Suite, r runnerFunc, o Options) Report {
	t.Helper()
	report, e := Evaluate(context.Background(), s, func() (aruntime.Runner, error) { return r, nil }, &llm.ModelDescriptor{ModelName: "controlled"}, o)
	if e != nil {
		t.Fatal(e)
	}
	return report
}

func TestOracleIsolationAndUnknownUsage(t *testing.T) {
	s := suite(t)
	report := execute(t, s, func(_ context.Context, in *aruntime.RunInput) (<-chan agent.Event, error) {
		b, _ := json.Marshal(in)
		if strings.Contains(string(b), "oracle-only") {
			t.Fatal("oracle leaked into runner input")
		}
		return stream(agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")), nil
	}, opts())
	if report.Total != 1 || report.Passed != 1 || report.Results[0].Usage != nil || report.Results[0].CostUSD != nil {
		t.Fatalf("unknown usage fabricated: %+v", report)
	}
}

func TestSuiteRejectsDuplicateAndFamilyLeakage(t *testing.T) {
	base := `{"id":"a","family":"same","split":"train","input":[{"role":"user","content":"q"}],"expected":{"final":{},"tools":[]}}`
	for _, second := range []string{base, strings.Replace(strings.Replace(base, `"id":"a"`, `"id":"b"`, 1), `"train"`, `"test"`, 1)} {
		if _, e := LoadSuite(strings.NewReader(base + "\n" + second)); e == nil {
			t.Fatal("invalid dataset accepted")
		}
	}
}

func TestToolEvidenceAndFailuresStayInDenominator(t *testing.T) {
	s := suite(t)
	s.Tasks[0].Expected.Tools = []ToolExpectation{{Name: "inspect", Arguments: json.RawMessage(`{"key":"lease"}`)}}
	cases := map[string][]agent.Event{
		"valid":           {agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtToolResult("c", "ok", 1, "ok"), agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")},
		"missing-result":  {agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")},
		"wrong-args":      {agent.EvtToolCall("c", "inspect", `{"key":"other"}`), agent.EvtToolResult("c", "ok", 1, "ok"), agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")},
		"tool-error":      {agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtToolResult("c", "failed", 1, "error"), agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")},
		"double-terminal": {agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop"), agent.EvtDone("stop")},
		"no-terminal":     {agent.EvtDelta(`{"answer":"oracle-only"}`)},
		"duplicate-call":  {agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtToolResult("c", "ok", 1, "ok"), agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtDone("stop")},
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			r := execute(t, s, func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error) { return stream(events...), nil }, opts())
			want := 0
			if name == "valid" {
				want = 1
			}
			if r.Total != 1 || r.Passed != want {
				t.Fatalf("bad denominator/verdict: %+v", r)
			}
		})
	}
}

func TestDeadlineAndEventLimit(t *testing.T) {
	s := suite(t)
	t.Run("deadline", func(t *testing.T) {
		r := execute(t, s, func(ctx context.Context, _ *aruntime.RunInput) (<-chan agent.Event, error) {
			ch := make(chan agent.Event)
			go func() { <-ctx.Done(); close(ch) }()
			return ch, nil
		}, opts())
		if r.Total != 1 || r.Passed != 0 || r.Results[0].Failure != "timeout" {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("limit", func(t *testing.T) {
		o := opts()
		o.MaxEvents = 1
		r := execute(t, s, func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error) {
			return stream(agent.EvtDelta("x"), agent.EvtDone("stop")), nil
		}, o)
		if r.Results[0].Failure != "event_limit" {
			t.Fatalf("%+v", r)
		}
	})
}

func TestTokenAccountingAndExplicitPrices(t *testing.T) {
	s := suite(t)
	o := opts()
	o.Prices = &Prices{Version: "fixture-rate-v1", InputPerMillion: 2, OutputPerMillion: 4}
	r := execute(t, s, func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error) {
		return stream(agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtUsage(100, 50, "", "controlled"), agent.EvtDone("stop")), nil
	}, o)
	if r.Results[0].Usage == nil || r.Results[0].Usage.Prompt != 100 || r.Results[0].CostUSD == nil || *r.Results[0].CostUSD != 0.0004 {
		t.Fatalf("%+v", r.Results[0])
	}
	o.Prices.Version = ""
	if _, e := Evaluate(context.Background(), s, nil, nil, o); e == nil {
		t.Fatal("unversioned pricing accepted")
	}
}

func TestCompareRejectsDifferentSuitesAndRetainsRegressions(t *testing.T) {
	a := Report{SuiteSHA256: "abc", Total: 1, Results: []Result{{ID: "a", Passed: true}}}
	b := a
	b.Results = []Result{{ID: "a", Passed: false, Failure: "output_mismatch"}}
	c, e := Compare(a, b)
	if e != nil || c.Regressions != 1 || c.Improvements != 0 {
		t.Fatalf("%+v %v", c, e)
	}
	b.SuiteSHA256 = "other"
	if _, e = Compare(a, b); e == nil {
		t.Fatal("compared different datasets")
	}
}

func TestConcurrentCallbackOrderAndFailedTaskUsage(t *testing.T) {
	s := suite(t)
	s.Tasks[0].Expected.Tools = []ToolExpectation{{Name: "inspect", Arguments: json.RawMessage(`{"key":"lease"}`)}}
	for _, wrong := range []bool{false, true} {
		answer := `{"answer":"oracle-only"}`
		if wrong {
			answer = `{"answer":"wrong"}`
		}
		r := execute(t, s, func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error) {
			return stream(agent.EvtToolResult("c", "ok", 1, "ok"), agent.EvtToolCall("c", "inspect", `{"key":"lease"}`), agent.EvtDelta(answer), agent.EvtUsage(8, 3, "", "fixture"), agent.EvtDone("stop")), nil
		}, opts())
		if r.Results[0].Passed == wrong || r.Results[0].Usage == nil || r.Results[0].Usage.Prompt != 8 {
			t.Fatalf("callback race or usage lost: %+v", r.Results[0])
		}
	}
}

func TestOnlySuccessfulFinishReasonCanPass(t *testing.T) {
	for _, reason := range []string{"canceled", "cancelled", "length", "content_filter", "tool_calls", ""} {
		t.Run(reason, func(t *testing.T) {
			r := execute(t, suite(t), func(context.Context, *aruntime.RunInput) (<-chan agent.Event, error) {
				return stream(agent.EvtDelta(`{"answer":"oracle-only"}`), agent.EvtUsage(8, 3, "", "fixture"), agent.EvtDone(reason)), nil
			}, opts())
			if r.Results[0].Passed || r.Results[0].Failure != "unsuccessful_finish" || r.Results[0].Usage == nil {
				t.Fatalf("non-success terminal passed or accounting lost: %+v", r.Results[0])
			}
		})
	}
}

func TestTimeoutRetainsAlreadyObservedUsage(t *testing.T) {
	o := opts()
	o.Prices = &Prices{Version: "fixture-v1", InputPerMillion: 2, OutputPerMillion: 4}
	r := execute(t, suite(t), func(ctx context.Context, _ *aruntime.RunInput) (<-chan agent.Event, error) {
		ch := make(chan agent.Event, 1)
		ch <- agent.EvtUsage(100, 50, "", "fixture")
		go func() { <-ctx.Done(); close(ch) }()
		return ch, nil
	}, o)
	if r.Results[0].Failure != "timeout" || r.Results[0].Usage == nil || r.Results[0].CostUSD == nil {
		t.Fatalf("observed accounting lost on timeout: %+v", r.Results[0])
	}
}
