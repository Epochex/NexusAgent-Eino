//go:build integration && infrastructure && realmodel

package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// This test never reads a credential or contacts a model vendor.
func TestRealModelBudgetRejectsBeforeBilling(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"error":{"message":"reasoning_content missing fixture-only"}}`)
			return
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`)
	}))
	defer upstream.Close()
	b := newRealBudgetRelay(upstream.URL, "fixture-only", filepath.Join(t.TempDir(), "ledger.json"))
	b.capCNY = .08
	proxy := httptest.NewServer(b)
	defer proxy.Close()
	post := func(body string, want int) {
		t.Helper()
		r, err := http.Post(proxy.URL+"/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != want {
			t.Fatalf("status=%d want=%d", r.StatusCode, want)
		}
	}
	valid := `{"model":"deepseek-flash","max_tokens":1024,"messages":[{"role":"user","content":"one"}]}`
	post(strings.ReplaceAll(valid, "deepseek-flash", "expensive-other-model"), 400)
	post(strings.ReplaceAll(valid, "1024", "1025"), 400)
	post(string(bytes.Repeat([]byte("x"), 16385)), 413)
	if calls.Load() != 0 {
		t.Fatal("invalid request reached billable upstream")
	}
	post(valid, 200)
	post(valid, 409) // Transport retries cannot make a second billable request.
	post(strings.ReplaceAll(valid, "one", "two"), 402)
	if calls.Load() != 1 {
		t.Fatalf("budget/retry guard forwarded %d calls", calls.Load())
	}
	// Recreating the relay cannot reset the durable spending/duplicate ledger.
	restored := newRealBudgetRelay(upstream.URL, "fixture-only", b.path)
	if len(restored.rows) != 1 || restored.charged <= 0 {
		t.Fatal("ledger reset")
	}
	fail.Store(true)
	errorRelay := newRealBudgetRelay(upstream.URL, "fixture-only", filepath.Join(t.TempDir(), "errors.json"))
	errorProxy := httptest.NewServer(errorRelay)
	defer errorProxy.Close()
	r, err := http.Post(errorProxy.URL+"/chat/completions", "application/json", strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != 400 || !strings.Contains(string(body), "reasoning_content") || strings.Contains(string(body), "fixture-only") {
		t.Fatal("vendor error must remain diagnostic without credentials")
	}
}

func TestRealModelExtendedBoundsRemainFailClosed(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":18000,"completion_tokens":20}}`)
	}))
	defer upstream.Close()
	b := newRealBudgetRelay(upstream.URL, "fixture-only", filepath.Join(t.TempDir(), "ledger.json"))
	b.capCNY, b.maxCalls, b.maxBody, b.maxOutput, b.reserve = 4.8, 180, 65536, 2048, .3
	proxy := httptest.NewServer(b)
	defer proxy.Close()
	post := func(body string, want int) {
		t.Helper()
		r, err := http.Post(proxy.URL+"/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != want {
			t.Fatalf("status=%d want=%d", r.StatusCode, want)
		}
	}
	valid := `{"model":"deepseek-flash","max_tokens":2048,"messages":[{"role":"user","content":"` + strings.Repeat("a", 32000) + `"}]}`
	post(valid, 200)
	post(valid, 409)
	post(strings.ReplaceAll(valid, "2048", "2049"), 400)
	post(strings.Repeat("a", 65537), 413)
	b.capCNY = b.charged + .29
	post(strings.ReplaceAll(valid, `"content":"a`, `"content":"b`), 402)
	if calls.Load() != 1 {
		t.Fatal("invalid extended request consumed upstream calls")
	}
}
