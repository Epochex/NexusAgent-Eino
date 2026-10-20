//go:build integration

package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOutboxPublishFailureRemainsPending(t *testing.T) {
	s, db, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	failed := errors.New("notification peer unavailable")
	if _, err := s.PublishOutbox(context.Background(), func(context.Context, Notification) error { return failed }, 16); !errors.Is(err, failed) {
		t.Fatalf("publish failure %v", err)
	}
	var pending int64
	db.Table("execution_outbox").Where("published_at IS NULL").Count(&pending)
	if pending != 1 {
		t.Fatalf("lost failed notification: %d", pending)
	}
	var seen Notification
	count, err := s.PublishOutbox(context.Background(), func(_ context.Context, n Notification) error { seen = n; return nil }, 16)
	if err != nil || count != 1 || seen.RunID != run.ID || seen.Seq != 1 {
		t.Fatalf("publish %d %+v %v", count, seen, err)
	}
	db.Table("execution_outbox").Where("published_at IS NULL").Count(&pending)
	if pending != 0 {
		t.Fatalf("not marked published %d", pending)
	}
	count, err = s.PublishOutbox(context.Background(), func(context.Context, Notification) error { t.Error("republished confirmed notification"); return nil }, 16)
	if err != nil || count != 0 {
		t.Fatal(err)
	}
}
func TestEventRetentionPreservesActiveAndExpiresCursor(t *testing.T) {
	s, db, owner, wf := fixture(t)
	finished := enqueue(t, s, owner, wf)
	active := enqueue(t, s, owner, wf)
	claimed := claim(t, s, "retention")
	if claimed.ID != finished.ID {
		t.Fatal("unexpected claim order")
	}
	if err := s.Finish(context.Background(), claimed, Succeeded, []byte(`{"result":"saved"}`), ""); err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE execution_events SET created_at=NOW()-INTERVAL '8 days'`)
	db.Exec(`UPDATE execution_outbox SET created_at=NOW()-INTERVAL '8 days',published_at=NOW()`)
	db.Exec(`UPDATE workflow_runs SET finished_at=NOW()-INTERVAL '8 days' WHERE id=?`, finished.ID)
	if err := s.PruneEvents(context.Background(), 7*24*time.Hour, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Events(context.Background(), owner, finished.ID, 0, 128); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("old cursor not explicit: %v", err)
	}
	events, err := s.Events(context.Background(), owner, active.ID, 0, 128)
	if err != nil || len(events) != 1 {
		t.Fatalf("active replay lost %d %v", len(events), err)
	}
	if snapshot, err := s.Get(context.Background(), owner, finished.ID); err != nil || snapshot.Status != Succeeded {
		t.Fatalf("snapshot removed %v", err)
	}
}

func TestRunDeadlineExpiresWaitingApproval(t *testing.T) {
	s, db, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	db.Exec(`UPDATE workflow_runs SET status='waiting_approval',deadline_at=NOW()-INTERVAL '1 second' WHERE id=?`, run.ID)
	if _, err := s.ExpireRuns(context.Background(), 64); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(context.Background(), owner, run.ID)
	if err != nil || got.Status != Failed || got.FinishedAt == nil {
		t.Fatalf("expired approval stranded %+v %v", got, err)
	}
}
