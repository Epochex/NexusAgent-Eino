// loadtest is a controlled acceptance tool, not a production application route.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "peer", "controlled peer or open-load stage")
	listen := flag.String("listen", "127.0.0.1:18080", "controlled peer listener")
	fixture := flag.String("fixture", "output/capacity/fixture.json", "private generated fixture file")
	api := flag.String("api", "http://127.0.0.1:18090", "actual application API")
	peerAPI := flag.String("peer-api", "http://127.0.0.1:58080", "controlled peer observations")
	migrations := flag.String("migrations", "backend/migrations", "schema migration source directory")
	stage := flag.String("stage", "pilot", "unique stage name")
	output := flag.String("output", "output/capacity/stage.json", "credential-free report path")
	sse := flag.Int("sse", 50, "target simultaneous SSE connections")
	slots := flag.Int("slots", 8, "global active run/provider limit")
	rate := flag.Int("rate", 1, "new primary requests per second (80% workflow/20% Chat)")
	duration := flag.Duration("duration", 30*time.Second, "absolute open-arrival window")
	idle := flag.Duration("idle", 0, "idle observation after drain")
	flag.Parse()
	if *mode == "stage" {
		root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := runStage(root, *fixture, *output, stageConfig{Name: *stage, SSE: *sse, Slots: *slots, Rate: *rate, Duration: *duration, Idle: *idle}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *mode == "prepare" || *mode == "bootstrap" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var err error
		if *mode == "prepare" {
			err = prepareFixture(ctx, *fixture, *migrations, *api, *peerAPI)
		} else {
			err = bootstrapFixture(ctx, *fixture)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("isolated fixture " + *mode + " completed; credentials remain in the private fixture file")
		return
	}
	if *mode != "peer" {
		fmt.Fprintln(os.Stderr, "unsupported loadtest mode")
		os.Exit(1)
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	peer := newControlledPeer(250*time.Millisecond, 100*time.Millisecond)
	server := &http.Server{Addr: *listen, Handler: peer.handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return root }}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case <-root.Done():
	case err := <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "controlled peer listener failed")
			os.Exit(1)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if server.Shutdown(ctx) != nil {
		_ = server.Close()
	}
}
