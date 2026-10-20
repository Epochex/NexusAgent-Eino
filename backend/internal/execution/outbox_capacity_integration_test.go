//go:build integration

package execution

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestOutboxContinuouslyDrainsFullBatches(t *testing.T) {
	if os.Getenv("NX_TEST_REDIS_ADDR") == "" {
		t.Fatal("explicit Redis acceptance endpoint required")
	}
	s, db, owner, flow := fixture(t)
	run := enqueue(t, s, owner, flow)
	// A durable, committed backlog independent from the publisher's counters.
	if err := db.Exec(`INSERT INTO execution_outbox(run_id,seq,topic,payload)
	 SELECT ?,n,'run.progress','{}'::jsonb FROM generate_series(2,512) n`, run.ID).Error; err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 14, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.RunNotifications(ctx, rdb, func() {}) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("publisher did not stop")
		}
	}()
	start := time.Now()
	var pending int64
	for until := start.Add(3500 * time.Millisecond); ; {
		if err := db.Table("execution_outbox").Where("published_at IS NULL AND run_id=?", run.ID).Count(&pending).Error; err != nil {
			t.Fatal(err)
		}
		if pending == 0 || time.Now().After(until) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pending != 0 {
		t.Fatalf("full batches waited for fixed 500ms polling ticks: pending=%d elapsed=%s", pending, time.Since(start))
	}
	var published int64
	if err := db.Table("execution_outbox").Where("published_at IS NOT NULL AND run_id=?", run.ID).Count(&published).Error; err != nil || published != 512 {
		t.Fatal("lost durable notifications", published, err)
	}
	t.Logf("committed notifications=%d drained=%s", published, time.Since(start))
}

func TestOutboxBatchCommitFailurePreservesStableRedelivery(t *testing.T) {
	s, db, owner, flow := fixture(t)
	run := enqueue(t, s, owner, flow)
	if err := db.Exec("INSERT INTO execution_outbox(run_id,seq,topic,payload) VALUES(?,2,'run.progress','{}')", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ALTER TABLE execution_outbox ADD CONSTRAINT reject_batch_ack CHECK(published_at IS NULL)").Error; err != nil {
		t.Fatal(err)
	}
	var first []Notification
	count, err := s.PublishOutbox(context.Background(), func(_ context.Context, n Notification) error { first = append(first, n); return nil }, 32)
	if err == nil || count != 0 || len(first) != 2 {
		t.Fatalf("failed commit acknowledged notifications: count=%d sent=%d err=%v", count, len(first), err)
	}
	var pending int64
	if err = db.Table("execution_outbox").Where("published_at IS NULL").Count(&pending).Error; err != nil || pending != 2 {
		t.Fatal("failed commit lost rows", pending, err)
	}
	if err = db.Exec("ALTER TABLE execution_outbox DROP CONSTRAINT reject_batch_ack").Error; err != nil {
		t.Fatal(err)
	}
	failure := errors.New("second peer fails after first accepted")
	sent := 0
	count, err = s.PublishOutbox(context.Background(), func(context.Context, Notification) error {
		sent++
		if sent == 2 {
			return failure
		}
		return nil
	}, 32)
	if !errors.Is(err, failure) || count != 0 {
		t.Fatal("partial callback failure acknowledged batch", count, err)
	}
	var retry []Notification
	count, err = s.PublishOutbox(context.Background(), func(_ context.Context, n Notification) error { retry = append(retry, n); return nil }, 32)
	if err != nil || count != 2 || len(retry) != 2 {
		t.Fatal("retry failed", count, err)
	}
	for i := range first {
		if first[i].ID != retry[i].ID || first[i].RunID != retry[i].RunID || first[i].Seq != retry[i].Seq {
			t.Fatal("redelivery changed deduplication identity")
		}
	}
}
