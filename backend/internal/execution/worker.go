package execution

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Executor interface {
	Execute(context.Context, *Run) error
}
type WorkerConfig struct {
	Capabilities                         []string
	ID                                   string
	Concurrency                          int
	Lease, PollInterval, ShutdownTimeout time.Duration
}
type Worker struct {
	store    *Store
	executor Executor
	cfg      WorkerConfig
	slots    chan struct{}
	wake     chan struct{}
	running  atomic.Bool
}

func NewWorker(store *Store, executor Executor, cfg WorkerConfig) (*Worker, error) {
	if store == nil || executor == nil {
		return nil, ErrInvalidInput
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	if cfg.Lease == 0 {
		cfg.Lease = 15 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 64 || cfg.Lease < time.Second || cfg.Lease > time.Minute || cfg.PollInterval < 5*time.Millisecond || cfg.PollInterval > 5*time.Second || cfg.ShutdownTimeout < time.Second || cfg.ShutdownTimeout > 30*time.Second {
		return nil, ErrInvalidInput
	}
	if cfg.ID == "" {
		host, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), rand.Text())
	}
	if len(cfg.ID) > 128 {
		return nil, ErrInvalidInput
	}
	return &Worker{store: store, executor: executor, cfg: cfg, slots: make(chan struct{}, cfg.Concurrency), wake: make(chan struct{}, 1)}, nil
}
func (w *Worker) Active() int { return len(w.slots) }

// Notify is only an accelerator. Run keeps polling PostgreSQL without it.
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("worker already running")
	}
	defer w.running.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	maintenance := time.NewTicker(time.Second)
	defer maintenance.Stop()
	var jobs sync.WaitGroup
	w.Notify()
	for {
		select {
		case <-ctx.Done():
			drained := make(chan struct{})
			go func() { jobs.Wait(); close(drained) }()
			select {
			case <-drained:
				return nil
			case <-time.After(w.cfg.ShutdownTimeout):
				return fmt.Errorf("worker shutdown exceeded %s", w.cfg.ShutdownTimeout)
			}
		case <-w.wake:
		case <-ticker.C:
		case <-maintenance.C:
			if _, err := w.store.ExpireRuns(ctx, 64); err != nil && ctx.Err() == nil {
				log.Printf("execution deadline sweep: %v", err)
			}
		}
		for ctx.Err() == nil && len(w.slots) < cap(w.slots) {
			w.slots <- struct{}{}
			run, err := w.store.ClaimSupported(ctx, w.cfg.ID, w.cfg.Lease, w.cfg.Capabilities)
			if err != nil || run == nil {
				<-w.slots
				if err != nil && ctx.Err() == nil {
					log.Printf("execution claim: %v", err)
				}
				break
			}
			jobs.Add(1)
			go func(run *Run) { defer jobs.Done(); defer func() { <-w.slots; w.Notify() }(); w.execute(ctx, run) }(run)
		}
	}
}

func (w *Worker) execute(parent context.Context, run *Run) {
	ctx, cancel := context.WithDeadline(parent, run.DeadlineAt)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(w.cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.store.Renew(ctx, run, w.cfg.Lease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := w.executor.Execute(ctx, run)
	cancel()
	<-heartbeatDone
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrLeaseLost) {
		log.Printf("execution run=%d generation=%d: %v", run.ID, run.Generation, err)
	}
}
