package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenArrivalsDoNotWaitForSlowRequests(t *testing.T) {
	block := make(chan struct{})
	var invoked atomic.Int64
	start := time.Now()
	run, err := scheduleArrivals(context.Background(), 20, 200*time.Millisecond, 2, func(context.Context, int64, time.Time) error { invoked.Add(1); <-block; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		close(block)
		t.Fatalf("closed-loop generator waited for requests: %s", elapsed)
	}
	before := run.snapshot()
	if before.Target != 4 || before.Scheduled != 4 || before.Attempted != 2 || before.SkippedBusy != 2 || invoked.Load() != 2 {
		close(block)
		t.Fatalf("lost offered/skipped work accounting: %+v invoked=%d", before, invoked.Load())
	}
	close(block)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = run.wait(ctx); err != nil {
		t.Fatal(err)
	}
	after := run.snapshot()
	if after.Succeeded != 2 || after.Failed != 0 || after.Attempted != after.Succeeded+after.Failed {
		t.Fatal("completion accounting omitted attempted work", after)
	}
}

func TestOpenArrivalCancellationAndFailureAreVisible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run, err := scheduleArrivals(ctx, 20, time.Second, 2, func(context.Context, int64, time.Time) error { t.Error("canceled scheduler emitted work"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got := run.snapshot(); got.Target != 20 || got.Attempted != 0 || got.NotScheduled != 20 {
		t.Fatal("cancellation silently lost scheduled work", got)
	}
	run, err = scheduleArrivals(context.Background(), 20, 100*time.Millisecond, 2, func(context.Context, int64, time.Time) error { return errors.New("actual request failed") })
	if err != nil {
		t.Fatal(err)
	}
	if err = run.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := run.snapshot(); got.Failed != 2 || got.Succeeded != 0 {
		t.Fatal("request failures disappeared from totals", got)
	}
}
