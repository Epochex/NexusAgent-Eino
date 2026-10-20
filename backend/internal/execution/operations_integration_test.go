//go:build integration

package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCancelRecordsUncertainExternalOperation(t *testing.T) {
	s, db, owner, wf := fixture(t)
	enqueue(t, s, owner, wf)
	run := claim(t, s, "cancel-write")
	op, _, err := s.BeginOperation(context.Background(), run, "root/write", "tool", "hash", "manual")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cancel(context.Background(), owner, run.ID); err != nil {
		t.Fatal(err)
	}
	var saved Operation
	if err = db.First(&saved, "id=?", op.ID).Error; err != nil {
		t.Fatal(err)
	}
	if saved.Status != "unknown" {
		t.Fatalf("cancel hid uncertain write: %s", saved.Status)
	}
	var status string
	db.Raw(`SELECT status FROM execution_attempts WHERE operation_id=? AND attempt=1`, op.ID).Scan(&status)
	if status != "unknown" {
		t.Fatalf("attempt status %s", status)
	}
	if err = s.CompleteOperation(context.Background(), run, op, "late", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late completion %v", err)
	}
	// Auditing an applied write may not resurrect a deliberately canceled job.
	if err = s.ResolveOperation(context.Background(), owner, run.ID, op.ID, "hash", `"confirmed"`, "checked externally"); !errors.Is(err, ErrConflict) {
		t.Fatalf("resurrected canceled run: %v", err)
	}
}

func TestOperationUnknownNotReplayed(t *testing.T) {
	s, db, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	run = claim(t, s, "first")
	op, cached, err := s.BeginOperation(context.Background(), run, "root/write", "tool", "args-hash", "manual")
	if err != nil || cached {
		t.Fatalf("cannot record operation intent: %v", err)
	}
	db.Exec(`UPDATE workflow_runs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, run.ID)
	fresh := claim(t, s, "after-kill")
	if _, _, err = s.BeginOperation(context.Background(), fresh, "root/write", "tool", "args-hash", "manual"); !errors.Is(err, ErrUnknownOperation) {
		t.Fatalf("unknown external write would replay: %v", err)
	}
	if err = s.CompleteOperation(context.Background(), run, op, "late result", nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale operation committed: %v", err)
	}
	if err = s.Finish(context.Background(), fresh, NeedsAttention, nil, "external outcome unknown"); err != nil {
		t.Fatal(err)
	}
	if err = s.ResolveOperation(context.Background(), owner+1, run.ID, op.ID, "args-hash", `"confirmed"`, "remote record checked"); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign operator resolved write")
	}
	if err = s.ResolveOperation(context.Background(), owner, run.ID, op.ID, "changed", `"confirmed"`, "remote record checked"); !errors.Is(err, ErrConflict) {
		t.Fatal("changed operation resolved")
	}
	if err = s.ResolveOperation(context.Background(), owner, run.ID, op.ID, "args-hash", `"confirmed"`, "remote record checked"); err != nil {
		t.Fatal(err)
	}
	resumed := claim(t, s, "resolved")
	done, cached, err := s.BeginOperation(context.Background(), resumed, "root/write", "tool", "args-hash", "manual")
	if err != nil || !cached || string(done.Output) != `"confirmed"` {
		t.Fatalf("confirmed output not reused: %+v %v", done, err)
	}
}

func TestOperationSafeReplayAndCommittedReuse(t *testing.T) {
	s, db, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	run = claim(t, s, "first")
	first, _, err := s.BeginOperation(context.Background(), run, "root/read", "llm", "hash", "readonly")
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE workflow_runs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, run.ID)
	fresh := claim(t, s, "replacement")
	retry, cached, err := s.BeginOperation(context.Background(), fresh, "root/read", "llm", "hash", "readonly")
	if err != nil || cached || retry.ID != first.ID || retry.Attempt != 2 {
		t.Fatalf("safe logical operation identity changed: %+v %v", retry, err)
	}
	if err = s.CompleteOperation(context.Background(), fresh, retry, map[string]any{"result": "saved"}, nil); err != nil {
		t.Fatal(err)
	}
	done, cached, err := s.BeginOperation(context.Background(), fresh, "root/read", "llm", "hash", "readonly")
	if err != nil || !cached || done.Attempt != 2 {
		t.Fatalf("completed step replayed: %+v %v", done, err)
	}
	if _, _, err = s.BeginOperation(context.Background(), fresh, "root/read", "llm", "different", "readonly"); !errors.Is(err, ErrConflict) {
		t.Fatal("path reused with different arguments")
	}
	if err = s.Retry(context.Background(), fresh, time.Minute, "retry later"); err != nil {
		t.Fatal(err)
	}
	if early, err := s.Claim(context.Background(), "too-early", time.Minute); err != nil || early != nil {
		t.Fatal("backoff occupied ready work")
	}
}
