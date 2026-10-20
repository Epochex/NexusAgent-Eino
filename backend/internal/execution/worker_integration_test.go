//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
)

func waitStatus(t *testing.T, s *Store, owner, id int64, status string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := s.Get(context.Background(), owner, id)
		if err == nil && run.Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	run, err := s.Get(context.Background(), owner, id)
	t.Fatalf("want %s, got %+v %v", status, run, err)
}
func TestWorkerPollsAuthorityAndReleasesApprovalSlot(t *testing.T) {
	s, _, owner, wf := fixture(t)
	raw, _ := json.Marshal(Definition{DSL: "name: waiting\nnodes: [{key: review, type: human, prompt: review}]"})
	first, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(s, NewDriver(s, workflow.Deps{}), WorkerConfig{Concurrency: 1, Lease: time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker did not drain")
		}
	}()
	waitStatus(t, s, owner, first.ID, Waiting)
	waitStatus(t, s, owner, second.ID, Waiting)
	if worker.Active() != 0 {
		deadline := time.Now().Add(time.Second)
		for worker.Active() != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if worker.Active() != 0 {
			t.Fatal("approval wait retained execution slot")
		}
	}
}

func TestRecoveryWorkerSkipsUnsupportedCapabilities(t *testing.T) {
	s, _, owner, wf := fixture(t)
	definition := Definition{DSL: "name: needs-rag\nnodes: [{key: review,type: human,prompt: review}]", RequiredCapabilities: []string{"rag", "tool:kb_search"}}
	raw, _ := json.Marshal(definition)
	blocked, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(Definition{DSL: "name: plain\nnodes: [{key: review,type: human,prompt: review}]"})
	ready, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ClaimSupported(context.Background(), "recovery", time.Minute, []string{})
	if err != nil || got == nil || got.ID != ready.ID {
		t.Fatalf("claimed unsupported job %+v %v", got, err)
	}
	got, err = s.ClaimSupported(context.Background(), "full", time.Minute, []string{"rag", "tool:kb_search"})
	if err != nil || got == nil || got.ID != blocked.ID {
		t.Fatalf("capable worker could not claim %+v %v", got, err)
	}
}

type executorFunc func(context.Context, *Run) error

func (f executorFunc) Execute(ctx context.Context, run *Run) error { return f(ctx, run) }

func TestWorkerRenewsLeaseAndCancelsLostOwner(t *testing.T) {
	s, _, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	started, stopped := make(chan struct{}), make(chan struct{})
	runner := executorFunc(func(ctx context.Context, run *Run) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	worker, err := NewWorker(s, runner, WorkerConfig{Concurrency: 1, Lease: time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("worker leaked")
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker not started")
	}
	time.Sleep(1300 * time.Millisecond) // deliberately cross the original real lease TTL
	if duplicate, err := s.Claim(context.Background(), "competing-worker", time.Minute); err != nil || duplicate != nil {
		t.Fatalf("healthy lease not renewed: %+v %v", duplicate, err)
	}
	if err = s.Cancel(context.Background(), owner, run.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("fenced worker continued executing")
	}
}
