package long_term

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

type WorkerConfig struct {
	Concurrency                          int
	ID                                   string
	Lease, PollInterval, ShutdownTimeout time.Duration
}
type Worker struct {
	manager *Manager
	cfg     WorkerConfig
	wake    chan struct{}
	running atomic.Bool
}

func NewWorker(manager *Manager, cfg WorkerConfig) (*Worker, error) {
	if manager == nil || !manager.valid() {
		return nil, ErrMemoryInvalid
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
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
	if cfg.Concurrency < 1 || cfg.Concurrency > 8 || cfg.Lease < time.Second || cfg.Lease > time.Minute || cfg.PollInterval < 5*time.Millisecond || cfg.PollInterval > 5*time.Second || cfg.ShutdownTimeout < time.Second || cfg.ShutdownTimeout > 30*time.Second {
		return nil, ErrMemoryInvalid
	}
	if cfg.ID == "" {
		host, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), rand.Text())
	}
	if len(cfg.ID) > 128 {
		return nil, ErrMemoryInvalid
	}
	return &Worker{manager: manager, cfg: cfg, wake: make(chan struct{}, 1)}, nil
}
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *Worker) Run(parent context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("memory worker already running")
	}
	defer w.running.Store(false)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	slots := make(chan struct{}, w.cfg.Concurrency)
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
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
				return fmt.Errorf("memory worker shutdown exceeded budget")
			}
		case <-ticker.C:
		case <-w.wake:
		}
		for ctx.Err() == nil && len(slots) < cap(slots) {
			slots <- struct{}{}
			job, err := w.manager.repo.Claim(ctx, w.cfg.ID, w.cfg.Lease)
			if err != nil || job == nil {
				<-slots
				if err != nil && ctx.Err() == nil {
					log.Printf("memory job claim failed: %v", err)
				}
				break
			}
			jobs.Add(1)
			go func() { defer jobs.Done(); defer func() { <-slots; w.Notify() }(); w.execute(ctx, job) }()
		}
	}
}
func (w *Worker) execute(parent context.Context, job *Job) {
	deadline := job.DeadlineAt
	if cap := time.Now().Add(5 * time.Minute); deadline.After(cap) {
		deadline = cap
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	identity := *job
	heartbeat := make(chan struct{})
	go func() {
		defer close(heartbeat)
		ticker := time.NewTicker(w.cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.manager.repo.Renew(ctx, &identity, w.cfg.Lease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := func() (err error) {
		defer func() {
			if recover() != nil {
				err = PermanentMemory(fmt.Errorf("memory processor panic"))
			}
		}()
		return w.manager.Process(ctx, job)
	}()
	cancel()
	<-heartbeat
	if err != nil && parent.Err() == nil && !errors.Is(err, ErrMemoryLeaseLost) {
		final, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if failed := w.manager.repo.Fail(final, job, err); failed != nil && !errors.Is(failed, ErrMemoryLeaseLost) {
			log.Printf("memory job=%d finalize failed: %v", job.ID, failed)
		}
	}
}
