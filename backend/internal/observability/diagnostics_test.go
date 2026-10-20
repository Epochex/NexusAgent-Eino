package observability

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPrivateDiagnosticsRuntimeProfilesAndClose(t *testing.T) {
	d, err := StartDiagnostics(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer d.Close(ctx)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	for _, path := range []string{"/runtime", "/metrics", "/debug/pprof/goroutine?debug=1"} {
		res, err := client.Get("http://" + d.Addr() + path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("diagnostics %s: status=%d err=%v", path, res.StatusCode, err)
		}
		if path == "/runtime" {
			var stats struct {
				Goroutines int    `json:"goroutines"`
				HeapAlloc  uint64 `json:"heap_alloc"`
				NumGC      uint32 `json:"num_gc"`
			}
			if json.Unmarshal(data, &stats) != nil || stats.Goroutines < 1 || stats.HeapAlloc < 1 {
				t.Fatal("missing real runtime observations", string(data))
			}
		}
		if path == "/metrics" && !strings.Contains(string(data), "go_goroutines") {
			t.Fatal("metrics were not actually gathered")
		}
	}
	if err = d.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Get("http://" + d.Addr() + "/runtime"); err == nil {
		t.Fatal("diagnostic listener survived Close")
	}
	if err = d.Close(ctx); err != nil {
		t.Fatal("Close is not idempotent", err)
	}
	if d, err = StartDiagnostics(context.Background(), "", nil); err != nil || d != nil {
		t.Fatal("disabled diagnostics opened a surface", d, err)
	}
}
