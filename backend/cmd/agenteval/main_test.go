package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/agent/evaluation"
)

func TestRealEinoDemoAndRegressionComparison(t *testing.T) {
	s, err := evaluation.LoadSuite(strings.NewReader(demoSuite))
	if err != nil {
		t.Fatal(err)
	}
	good, err := runDemo(context.Background(), s, "baseline", "test-revision")
	if err != nil {
		t.Fatal(err)
	}
	bad, err := runDemo(context.Background(), s, "degraded", "test-revision")
	if err != nil {
		t.Fatal(err)
	}
	if good.Total != 3 || good.Passed != 3 || bad.Total != 3 || bad.Passed != 2 {
		t.Fatalf("unexpected totals: good=%+v bad=%+v", good, bad)
	}
	for _, r := range good.Results {
		if len(r.Events) < 4 || r.Usage == nil || r.Usage.Prompt != 8 || r.Usage.Completion != 3 || r.CostUSD != nil {
			t.Fatalf("missing actual tool loop/usage: %+v", r)
		}
	}
	c, err := evaluation.Compare(good, bad)
	if err != nil || c.Regressions != 1 || c.Improvements != 0 {
		t.Fatalf("%+v %v", c, err)
	}
	dir := t.TempDir()
	if err = evaluation.WriteReport(dir, good); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved evaluation.Report
	if err = json.Unmarshal(raw, &saved); err != nil || saved.Passed != 3 {
		t.Fatalf("%s %v", raw, err)
	}
	if strings.Contains(string(raw), "APIKey") || strings.Contains(string(raw), `"expected"`) {
		t.Fatal("private descriptor/oracle exported")
	}
	if _, err = os.Stat(filepath.Join(dir, "index.html")); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownDemoVariantRejected(t *testing.T) {
	s, err := evaluation.LoadSuite(strings.NewReader(demoSuite))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runDemo(context.Background(), s, "typo", "rev"); err == nil {
		t.Fatal("unknown variant accepted")
	}
}
