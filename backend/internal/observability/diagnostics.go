package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Diagnostics is an explicit private operations listener, disabled by an empty
// address. Do not publish its port through the application's public ingress.
type Diagnostics struct {
	server   *http.Server
	listener net.Listener
	stop     context.CancelFunc
	errors   chan error
	once     sync.Once
	closeErr error
}

func StartDiagnostics(parent context.Context, address string, db *sql.DB) (*Diagnostics, error) {
	if address == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	root, stop := context.WithCancel(parent)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/runtime", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		stats := map[string]any{"observed_at": time.Now().UTC(), "pid": os.Getpid(), "goroutines": runtime.NumGoroutine(), "gomaxprocs": runtime.GOMAXPROCS(0), "heap_alloc": memory.HeapAlloc, "heap_inuse": memory.HeapInuse, "heap_sys": memory.HeapSys, "stack_sys": memory.StackSys, "num_gc": memory.NumGC, "pause_total_ns": memory.PauseTotalNs, "gc_cpu_fraction": memory.GCCPUFraction}
		if db != nil {
			stats["database_pool"] = db.Stats()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})
	d := &Diagnostics{listener: listener, stop: stop, errors: make(chan error, 1)}
	d.server = &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return root }}
	go func() {
		err := d.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		d.errors <- err
		close(d.errors)
	}()
	return d, nil
}

func (d *Diagnostics) Addr() string         { return d.listener.Addr().String() }
func (d *Diagnostics) Errors() <-chan error { return d.errors }
func (d *Diagnostics) Close(ctx context.Context) error {
	d.once.Do(func() {
		d.stop()
		d.closeErr = d.server.Shutdown(ctx)
		if d.closeErr != nil {
			_ = d.server.Close()
		}
	})
	return d.closeErr
}
