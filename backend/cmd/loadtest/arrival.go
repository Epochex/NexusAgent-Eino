package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type arrivalSnapshot struct {
	Target           int64   `json:"target"`
	Scheduled        int64   `json:"scheduled"`
	NotScheduled     int64   `json:"not_scheduled"`
	Attempted        int64   `json:"attempted"`
	SkippedBusy      int64   `json:"skipped_client_busy"`
	SkippedLate      int64   `json:"skipped_scheduler_late"`
	Succeeded        int64   `json:"callbacks_ok"`
	Failed           int64   `json:"callback_errors"`
	MaxScheduleLagMS float64 `json:"max_schedule_lag_ms"`
}
type arrivalRun struct {
	target                                                                      int64
	scheduled, attempted, skippedBusy, skippedLate, succeeded, failed, maxLagNS atomic.Int64
	done                                                                        chan struct{}
}

func (r *arrivalRun) snapshot() arrivalSnapshot {
	s := r.scheduled.Load()
	return arrivalSnapshot{Target: r.target, Scheduled: s, NotScheduled: r.target - s, Attempted: r.attempted.Load(), SkippedBusy: r.skippedBusy.Load(), SkippedLate: r.skippedLate.Load(), Succeeded: r.succeeded.Load(), Failed: r.failed.Load(), MaxScheduleLagMS: float64(r.maxLagNS.Load()) / 1e6}
}
func (r *arrivalRun) wait(ctx context.Context) error {
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The absolute offer schedule never waits for a response or for a free client
// slot. HTTP WroteRequest and application/DB outcome accounting are separate
// measurements; an attempted callback is not a claim of wire delivery/success.
func scheduleArrivals(ctx context.Context, rate int, duration time.Duration, maxParallel int, call func(context.Context, int64, time.Time) error) (*arrivalRun, error) {
	if rate < 1 || rate > 10000 || duration <= 0 || duration > 6*time.Hour || maxParallel < 1 || maxParallel > 4096 || call == nil {
		return nil, fmt.Errorf("invalid open-arrival configuration")
	}
	r := &arrivalRun{target: duration.Nanoseconds() * int64(rate) / int64(time.Second), done: make(chan struct{})}
	start := time.Now()
	slots := make(chan struct{}, maxParallel)
	var calls sync.WaitGroup
	defer func() { go func() { calls.Wait(); close(r.done) }() }()
	waitUntil := func(at time.Time) bool {
		if ctx.Err() != nil {
			return false
		}
		delay := time.Until(at)
		if delay <= 0 {
			return true
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return ctx.Err() == nil
		}
	}
	for i := int64(0); i < r.target; i++ {
		at := start.Add(time.Duration(i * int64(time.Second) / int64(rate)))
		if !waitUntil(at) {
			break
		}
		r.scheduled.Add(1)
		lag := time.Since(at).Nanoseconds()
		for old := r.maxLagNS.Load(); lag > old; old = r.maxLagNS.Load() {
			if r.maxLagNS.CompareAndSwap(old, lag) {
				break
			}
		}
		if lag > int64(500*time.Millisecond) {
			r.skippedLate.Add(1)
			continue
		}
		select {
		case slots <- struct{}{}:
			r.attempted.Add(1)
			calls.Add(1)
			go func(index int64, scheduled time.Time) {
				defer calls.Done()
				defer func() { <-slots }()
				if call(ctx, index, scheduled) != nil {
					r.failed.Add(1)
				} else {
					r.succeeded.Add(1)
				}
			}(i, at)
		default:
			r.skippedBusy.Add(1)
		}
	}
	_ = waitUntil(start.Add(duration))
	return r, nil
}
