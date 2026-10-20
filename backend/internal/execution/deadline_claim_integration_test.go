//go:build integration

package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDeadlineEntriesRetireUncertainOperationsAtomically(t *testing.T) {
	for _, entry := range []string{"claim", "sweep"} {
		for _, policy := range []string{"manual", "readonly"} {
			t.Run(entry+"/"+policy, func(t *testing.T) {
				s, db, owner, wf := fixture(t)
				ctx := context.Background()
				enqueue(t, s, owner, wf)
				run := claim(t, s, "original")
				op, _, err := s.BeginOperation(ctx, run, "root/external", "tool", "hash", policy)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Exec(`UPDATE workflow_runs SET lease_until=NOW()-INTERVAL '1 second',deadline_at=NOW()-INTERVAL '1 second' WHERE id=?`, run.ID).Error; err != nil {
					t.Fatal(err)
				}
				if entry == "claim" {
					got, err := s.ClaimSupported(ctx, "replacement", time.Minute, nil)
					if err != nil || got != nil {
						t.Fatalf("expired run claimed: %+v %v", got, err)
					}
				} else if n, err := s.ExpireRuns(ctx, 64); err != nil || n != 1 {
					t.Fatalf("initial sweep: %d %v", n, err)
				}
				want := "abandoned"
				if policy == "manual" {
					want = "unknown"
				}
				var saved Operation
				if err := db.First(&saved, "id=?", op.ID).Error; err != nil {
					t.Fatal(err)
				}
				var attempt string
				if err := db.Raw(`SELECT status FROM execution_attempts WHERE operation_id=? AND attempt=?`, op.ID, op.Attempt).Scan(&attempt).Error; err != nil {
					t.Fatal(err)
				}
				if saved.Status != want || attempt != want || saved.FinishedAt == nil {
					t.Errorf("deadline hid external outcome: operation=%s attempt=%s finished=%v; want %s", saved.Status, attempt, saved.FinishedAt, want)
				}
				got, err := s.Get(ctx, owner, run.ID)
				if err != nil || got.Status != Failed || got.Generation != run.Generation+1 || got.LeaseOwner != "" || got.FinishedAt == nil {
					t.Fatalf("unfenced deadline terminal: %+v %v", got, err)
				}
				if err := s.CompleteOperation(ctx, run, op, "late", nil); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("late external result committed: %v", err)
				}
				if n, err := s.ExpireRuns(ctx, 64); err != nil || n != 0 {
					t.Fatalf("terminal run re-expired: %d %v", n, err)
				}
				events, err := s.Events(ctx, owner, run.ID, 0, 128)
				if err != nil {
					t.Fatal(err)
				}
				unknown, failed := 0, 0
				for _, event := range events {
					if event.Type == "operation_unknown" {
						unknown++
					}
					if event.Type == Failed {
						failed++
					}
				}
				wantUnknown := 0
				if policy == "manual" {
					wantUnknown = 1
				}
				if unknown != wantUnknown || failed != 1 || events[len(events)-1].Type != Failed {
					t.Errorf("deadline audit events: unknown=%d failed=%d last=%s", unknown, failed, events[len(events)-1].Type)
				}
				var outbox int64
				if err := db.Table("execution_outbox").Where("run_id=?", run.ID).Count(&outbox).Error; err != nil || outbox != int64(len(events)) {
					t.Fatalf("event/outbox mismatch: %d/%d %v", outbox, len(events), err)
				}
			})
		}
	}
}
