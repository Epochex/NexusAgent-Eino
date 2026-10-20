package runtime

import (
	"context"
	"fmt"
	"net/http"
	stdRuntime "runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
)

func TestEinoRunnerLifecycleResources(t *testing.T) {
	var active atomic.Int64
	closed := make(chan struct{}, 1)
	r, in := testRunner(t, func(w http.ResponseWriter, request *http.Request) {
		active.Add(1)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-request.Context().Done()
		active.Add(-1)
		closed <- struct{}{}
	}, runtimeTools{}, 2)
	one := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		events, err := r.Run(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		gotDelta := false
		for ev := range events {
			if ev.Type == agent.TypeDelta {
				gotDelta = true
				cancel()
			}
		}
		if !gotDelta {
			t.Fatal("no generated text")
		}
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("request not released")
		}
	}
	for range 5 {
		one()
	}
	stdRuntime.GC()
	base := stdRuntime.NumGoroutine()
	for cycle := 1; cycle <= 3; cycle++ {
		for range 34 {
			one()
		}
		stdRuntime.GC()
		var stats stdRuntime.MemStats
		stdRuntime.ReadMemStats(&stats)
		count := stdRuntime.NumGoroutine()
		t.Logf("cycle=%d runs=%d goroutines=%d baseline=%d heap_alloc=%d active_model_connections=%d", cycle, cycle*34, count, base, stats.HeapAlloc, active.Load())
		if active.Load() != 0 {
			t.Fatal("active model connection retained")
		}
		if count > base+8 {
			t.Fatalf("goroutines grew across completed cycles: %d -> %d", base, count)
		}
	}
}
