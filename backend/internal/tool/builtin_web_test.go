package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPToolRejectsPrivateTargetWithoutOperatorGrant(t *testing.T) {
	calls := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; fmt.Fprint(w, "private-service") }))
	defer peer.Close()
	registry := NewRegistry()
	closeClients, err := RegisterWeb(registry, SearchConfig{DomainWhitelist: []string{"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer closeClients()
	handler, ok := registry.Get("http_client")
	if !ok {
		t.Fatal("HTTP tool missing")
	}
	if out, err := handler(context.Background(), map[string]any{"url": peer.URL}, ""); err == nil || calls != 0 {
		t.Fatal("private service reached through web tool", out, err, calls)
	}
}

func TestHTTPToolRejectsHTTPFailureAndPartialResponse(t *testing.T) {
	for _, scenario := range []string{"status", "truncated", "oversize"} {
		t.Run(scenario, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch scenario {
				case "status":
					w.WriteHeader(500)
					fmt.Fprint(w, "upstream failed")
				case "truncated":
					w.Header().Set("Content-Length", "100")
					fmt.Fprint(w, "short")
				case "oversize":
					w.Header().Set("Content-Type", "text/plain")
					fmt.Fprint(w, strings.Repeat("x", (256<<10)+1))
				}
			}))
			defer peer.Close()
			cfg, closeClients, err := prepareWebConfig(SearchConfig{HTTPPrivateOrigins: []string{peer.URL}, DomainWhitelist: []string{"127.0.0.1"}})
			if err != nil {
				t.Fatal(err)
			}
			defer closeClients()
			out, err := runHTTPClient(context.Background(), cfg, map[string]any{"url": peer.URL})
			if err == nil {
				t.Fatal("invalid upstream response represented as successful content", out[:min(len(out), 100)])
			}
		})
	}
}

func TestSearchOriginGrantNeverWidensUserHTTPFetch(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("q") != "Go + Agent" {
			t.Error("wrong fixed search endpoint", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"results":[{"title":"Go agent","url":"https://go.dev/doc/","content":"documented result"}]}`)
	}))
	defer peer.Close()
	reg := NewRegistry()
	closeClients, err := RegisterWeb(reg, SearchConfig{SearxngURL: peer.URL + "/search", DomainWhitelist: []string{"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer closeClients()
	search, ok := reg.Get("web_search")
	if !ok {
		t.Fatal("search unavailable")
	}
	result, err := search(context.Background(), map[string]any{"query": "Go + Agent"}, "")
	if err != nil || !strings.Contains(result, "documented result") {
		t.Fatal("fixed internal search endpoint failed", result, err)
	}
	fetch, _ := reg.Get("http_client")
	if _, err = fetch(context.Background(), map[string]any{"url": peer.URL + "/private"}, ""); err == nil {
		t.Fatal("search origin grant leaked into user fetch")
	}
	empty := NewRegistry()
	cleanup, err := RegisterWeb(empty, SearchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if len(empty.Codes()) != 0 {
		t.Fatal("unconfigured web tools advertised as available")
	}
}

func TestHTTPToolExplicitGrantAndRedirectBoundary(t *testing.T) {
	var reached atomic.Int32
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { reached.Add(1); fmt.Fprint(w, "private data") }))
	defer private.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, private.URL, 302)
			return
		}
		if r.Header.Get("Accept-Language") != "zh-CN" {
			t.Error("allowed request header lost")
		}
		fmt.Fprint(w, "<p>Verified content</p>")
	}))
	defer peer.Close()
	cfg, cleanup, err := prepareWebConfig(SearchConfig{DomainWhitelist: []string{"127.0.0.1"}, HTTPPrivateOrigins: []string{peer.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	result, err := runHTTPClient(context.Background(), cfg, map[string]any{"url": peer.URL, "headers": map[string]any{"Accept-Language": "zh-CN"}})
	var decoded struct {
		Status  int
		Content string
	}
	if err != nil || json.Unmarshal([]byte(result), &decoded) != nil || decoded.Status != 200 || decoded.Content != "Verified content" {
		t.Fatal("explicit HTTP grant unusable", result, err)
	}
	if _, err = runHTTPClient(context.Background(), cfg, map[string]any{"url": peer.URL + "/redirect"}); err == nil || reached.Load() != 0 {
		t.Fatal("cross-origin redirect reached other private service", err, reached.Load())
	}
	if _, err = runHTTPClient(context.Background(), cfg, map[string]any{"url": peer.URL, "headers": map[string]any{"Authorization": "Bearer public-test-only"}}); err == nil {
		t.Fatal("model could attach authentication headers")
	}
}

func TestHTTPToolHonorsCancellation(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer peer.Close()
	cfg, cleanup, err := prepareWebConfig(SearchConfig{DomainWhitelist: []string{"127.0.0.1"}, HTTPPrivateOrigins: []string{peer.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err = runHTTPClient(ctx, cfg, map[string]any{"url": peer.URL}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("caller deadline not preserved", err)
	}
}

func TestSearchRejectsMissingResults(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"error":"engine unavailable"}`) }))
	defer peer.Close()
	cfg, cleanup, err := prepareWebConfig(SearchConfig{SearxngURL: peer.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if out, err := runWebSearch(context.Background(), cfg, map[string]any{"query": "Go"}); err == nil {
		t.Fatal("malformed search response became no-results success", out)
	}
}
