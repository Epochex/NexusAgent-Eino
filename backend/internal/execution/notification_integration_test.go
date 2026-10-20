//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
	"github.com/redis/go-redis/v9"
)

type outageConn struct {
	net.Conn
	down *atomic.Bool
}

func (c *outageConn) Read(p []byte) (int, error) {
	if c.down.Load() {
		_ = c.Conn.Close()
		return 0, errors.New("injected Redis network outage")
	}
	return c.Conn.Read(p)
}
func (c *outageConn) Write(p []byte) (int, error) {
	if c.down.Load() {
		_ = c.Conn.Close()
		return 0, errors.New("injected Redis network outage")
	}
	return c.Conn.Write(p)
}

func TestRedisOutageRetainsNotificationsAndPGProgress(t *testing.T) {
	address := os.Getenv("NX_TEST_REDIS_ADDR")
	if address == "" {
		t.Fatal("explicit NX_TEST_REDIS_ADDR required")
	}
	var down atomic.Bool
	client := redis.NewClient(&redis.Options{Addr: address, DB: 14, MaxRetries: -1, ReadTimeout: time.Second, WriteTimeout: time.Second, Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
		if down.Load() {
			return nil, errors.New("injected Redis network outage")
		}
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &outageConn{Conn: conn, down: &down}, nil
	}})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal("real Redis unavailable: ", err)
	}
	baseline, err := client.XLen(context.Background(), NotificationStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	s, db, owner, wf := fixture(t)
	raw, _ := json.Marshal(Definition{DSL: "name: notification\nnodes: [{key: review,type: human,prompt: review},{key: work,type: llm,prompt: finish}]"})
	run, err := s.Enqueue(context.Background(), owner, wf, raw, nil, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int32
	driver := NewDriver(s, workflow.Deps{ExecuteNode: func(context.Context, string, *workflow.Node, map[string]any) (any, error) {
		effects.Add(1)
		return "completed", nil
	}})
	worker, err := NewWorker(s, driver, WorkerConfig{Concurrency: 1, Lease: time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	workersDone, notificationsDone := make(chan error, 1), make(chan error, 1)
	down.Store(true)
	go func() { workersDone <- worker.Run(ctx) }()
	go func() { notificationsDone <- s.RunNotifications(ctx, client, worker.Notify) }()
	defer func() {
		cancel()
		for _, done := range []chan error{workersDone, notificationsDone} {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(4 * time.Second):
				t.Error("notification/worker shutdown exceeded bound")
			}
		}
	}()
	waitStatus(t, s, owner, run.ID, Waiting)
	time.Sleep(1100 * time.Millisecond) // cross two actual publication attempts
	var pending int64
	if err = db.Table("execution_outbox").Where("published_at IS NULL AND run_id=?", run.ID).Count(&pending).Error; err != nil || pending < 3 {
		t.Fatalf("outage lost pending notifications: %d %v", pending, err)
	}
	down.Store(false)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		db.Table("execution_outbox").Where("published_at IS NULL AND run_id=?", run.ID).Count(&pending)
		if pending == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pending != 0 {
		t.Fatalf("notification recovery did not catch up: %d", pending)
	}
	length, err := client.XLen(context.Background(), NotificationStream).Result()
	if err != nil || length <= baseline {
		t.Fatalf("real Redis stream did not receive hints: %d %v", length, err)
	}
	waiting, err := s.Get(context.Background(), owner, run.ID)
	if err != nil || len(waiting.Approvals) != 1 {
		t.Fatal(err)
	}
	a := waiting.Approvals[0]
	if err = s.Decide(context.Background(), owner, run.ID, a.InterruptID, a.ArgsHash, true); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err = client.Publish(context.Background(), notificationChannel, "duplicate").Err(); err != nil {
			t.Fatal(err)
		}
	}
	waitStatus(t, s, owner, run.ID, Succeeded)
	done, err := s.Get(context.Background(), owner, run.ID)
	if err != nil || effects.Load() != 1 || done.Generation != 2 {
		t.Fatalf("duplicate hints repeated work: effects=%d run=%+v %v", effects.Load(), done, err)
	}
}
