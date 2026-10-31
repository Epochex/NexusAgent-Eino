package evaluation

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
)

type Comparison struct {
	SuiteSHA256  string   `json:"suite_sha256"`
	Total        int      `json:"total"`
	Improvements int      `json:"improvements"`
	Regressions  int      `json:"regressions"`
	Unchanged    int      `json:"unchanged"`
	Changes      []Change `json:"changes"`
}
type Change struct {
	ID              string  `json:"id"`
	Before          bool    `json:"before"`
	After           bool    `json:"after"`
	Failure         string  `json:"failure,omitempty"`
	DurationDeltaMS float64 `json:"duration_delta_ms"`
}

func Compare(a, b Report) (Comparison, error) {
	c := Comparison{SuiteSHA256: a.SuiteSHA256}
	if a.SuiteSHA256 == "" || a.SuiteSHA256 != b.SuiteSHA256 {
		return c, errors.New("suite fingerprints differ")
	}
	if a.Total != len(a.Results) || b.Total != len(b.Results) || a.Total != b.Total {
		return c, errors.New("report denominators differ")
	}
	before := map[string]Result{}
	for _, r := range a.Results {
		if _, ok := before[r.ID]; ok || r.ID == "" {
			return c, errors.New("invalid task IDs")
		}
		before[r.ID] = r
	}
	seen := map[string]bool{}
	for _, r := range b.Results {
		old, ok := before[r.ID]
		if !ok || seen[r.ID] {
			return c, errors.New("task sets differ")
		}
		seen[r.ID] = true
		c.Total++
		switch {
		case old.Passed && !r.Passed:
			c.Regressions++
		case !old.Passed && r.Passed:
			c.Improvements++
		default:
			c.Unchanged++
		}
		c.Changes = append(c.Changes, Change{ID: r.ID, Before: old.Passed, After: r.Passed, Failure: r.Failure, DurationDeltaMS: r.DurationMS - old.DurationMS})
	}
	return c, nil
}

// WriteReport creates a machine-readable trace and escaped human-readable summary.
// The directory must be private: prompts and tool outputs can contain sensitive data.
func WriteReport(dir string, r Report) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err = atomicWrite(filepath.Join(dir, "report.json"), raw); err != nil {
		return err
	}
	const page = `<!doctype html><html lang="en"><meta charset="utf-8"><title>Agent evaluation</title><style>body{font:16px system-ui;max-width:1100px;margin:32px auto}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:10px;text-align:left}code{overflow-wrap:anywhere}</style><h1>Agent evaluation</h1><p>Scope: <strong>{{.Options.Scope}}</strong>. Controlled peers do not measure model capability.</p><p>Variant: {{.Options.Variant}} | Revision: {{.Options.Revision}} | Passed: {{.Passed}} / {{.Total}} | Unknown usage: {{.UnknownUsage}}</p><p>Suite SHA256: <code>{{.SuiteSHA256}}</code></p><table><tr><th>Task</th><th>Passed</th><th>Failure</th><th>Wall ms</th><th>Usage</th><th>Estimated USD</th></tr>{{range .Results}}<tr><td>{{.ID}}</td><td>{{.Passed}}</td><td>{{.Failure}}</td><td>{{printf "%.3f" .DurationMS}}</td><td>{{if .Usage}}{{.Usage.Prompt}} / {{.Usage.Completion}}{{else}}unknown{{end}}</td><td>{{if .CostUSD}}{{.CostUSD}}{{else}}unknown{{end}}</td></tr>{{end}}</table><p>Complete ordered event traces are in report.json. Cost, when present, uses the explicit report price version, not billing data.</p></html>`
	t, err := template.New("report").Parse(page)
	if err != nil {
		return err
	}
	var b buffer
	if err = t.Execute(&b, r); err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, "index.html"), b)
}

type buffer []byte

func (b *buffer) Write(p []byte) (int, error) { *b = append(*b, p...); return len(p), nil }
func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".eval-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("report rename: %w", err)
	}
	return nil
}
