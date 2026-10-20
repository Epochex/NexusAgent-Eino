package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type acceptanceRoundTrip func(*http.Request) (*http.Response, error)

func (f acceptanceRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenAcceptanceBody struct{}

func (brokenAcceptanceBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenAcceptanceBody) Close() error             { return nil }
func TestStreamCancellationDuringHeadersIsNotTransportFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := newAPIClient("http://fixture.invalid")
	api.http.Transport = acceptanceRoundTrip(func(*http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	pool := newStreamPool(ctx, api, []streamTarget{{id: 1}}, func(string, float64) {})
	pool.subscribe(ctx, streamTarget{id: 1})
	if pool.failures.Load() != 0 {
		t.Fatal("intentional subscription cancellation counted as an availability failure")
	}
}

func TestInitialSubscriptionRampBoundsPendingHeaders(t *testing.T) {
	var waiting, peak atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active := waiting.Add(1)
		for old := peak.Load(); active > old; old = peak.Load() {
			if peak.CompareAndSwap(old, active) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: queued\ndata: {}\n\n")
		waiting.Add(-1)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seeds := make([]streamTarget, 64)
	for i := range seeds {
		seeds[i].id = int64(i + 1)
	}
	pool := newStreamPool(ctx, newAPIClient(server.URL), seeds, func(string, float64) {})
	if err := pool.start(8, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if pool.active.Load() != 64 || peak.Load() > 8 || pool.failures.Load() != 0 {
		t.Fatal("unbounded or incomplete SSE ramp", pool.active.Load(), peak.Load(), pool.failures.Load())
	}
}
func TestStreamUncancelledBrokenBodyRemainsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := newAPIClient("http://fixture.invalid")
	api.http.Transport = acceptanceRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: brokenAcceptanceBody{}}, nil
	})
	pool := newStreamPool(ctx, api, []streamTarget{{id: 1}}, func(string, float64) {})
	pool.subscribe(ctx, streamTarget{id: 1})
	if pool.failures.Load() != 1 {
		t.Fatal("genuine broken event stream was hidden")
	}
}
