//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/gorm"
)

func fixture(t *testing.T) (*Store, *gorm.DB, int64, int64) {
	t.Helper()
	db := testpg.Open(t)
	var owner, id int64
	if err := db.Raw(`INSERT INTO users(username,email,password_hash) VALUES ('owner','owner@test.invalid','test-only') RETURNING id`).Scan(&owner).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw(`INSERT INTO workflows(name,dsl,creator_id) VALUES ('frozen','name: frozen',?) RETURNING id`, owner).Scan(&id).Error; err != nil {
		t.Fatal(err)
	}
	return NewStore(db), db, owner, id
}
func enqueue(t *testing.T, s *Store, owner, wf int64) *Run {
	t.Helper()
	r, err := s.Enqueue(context.Background(), owner, wf, json.RawMessage(`{"dsl":"original","engine":"eino-v0.9.22","graph_schema":1}`), json.RawMessage(`{"q":"hello"}`), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func claim(t *testing.T, s *Store, name string) *Run {
	t.Helper()
	r, err := s.Claim(context.Background(), name, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("missing claim")
	}
	return r
}

func TestExecutionClaimFencing(t *testing.T) {
	s, db, owner, wf := fixture(t)
	r := enqueue(t, s, owner, wf)
	var wg sync.WaitGroup
	results := make(chan *Run, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Claim(context.Background(), fmt.Sprint(i), time.Minute)
			if err != nil {
				t.Error(err)
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	var first *Run
	for got := range results {
		if got != nil {
			if first != nil {
				t.Fatal("double claim")
			}
			first = got
		}
	}
	if first == nil {
		t.Fatal("no worker claimed run")
	}
	if err := db.Exec(`UPDATE workflow_runs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, r.ID).Error; err != nil {
		t.Fatal(err)
	}
	second := claim(t, s, "replacement")
	if second.Generation <= first.Generation {
		t.Fatal("generation did not advance")
	}
	if err := s.Renew(context.Background(), first, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renewal accepted: %v", err)
	}
	if err := s.Finish(context.Background(), first, Succeeded, json.RawMessage(`{}`), ""); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale result accepted: %v", err)
	}
	if err := s.Finish(context.Background(), second, Succeeded, json.RawMessage(`{"answer":42}`), ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), second, Failed, nil, "late"); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("terminal regressed")
	}
	got, err := s.Get(context.Background(), owner, r.ID)
	if err != nil || got.Status != Succeeded {
		t.Fatalf("result lost: %+v %v", got, err)
	}
	if _, err = s.Get(context.Background(), owner+1, r.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign result disclosed: %v", err)
	}
}

func TestExecutionApprovalCAS(t *testing.T) {
	s, db, owner, wf := fixture(t)
	r := enqueue(t, s, owner, wf)
	r = claim(t, s, "worker")
	pending := []Approval{{InterruptID: "first", Path: "root/a", ArgsHash: "hash-a", Prompt: "Review A"}, {InterruptID: "second", Path: "root/b", ArgsHash: "hash-b", Prompt: "Review B"}}
	if err := s.Suspend(context.Background(), r, pending); err != nil {
		t.Fatal(err)
	}
	if err := s.Decide(context.Background(), owner+1, r.ID, "first", "hash-a", true); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign approval accepted")
	}
	if err := s.Decide(context.Background(), owner, r.ID, "first", "changed", true); !errors.Is(err, ErrConflict) {
		t.Fatal("changed arguments accepted")
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Decide(context.Background(), owner, r.ID, "first", "hash-a", true); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := s.Decide(context.Background(), owner, r.ID, "first", "hash-a", false); !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting decision overwritten")
	}
	var count int64
	db.Table("execution_events").Where("run_id=? AND type='approval_decided'", r.ID).Count(&count)
	if count != 1 {
		t.Fatalf("duplicate approval event: %d", count)
	}
	got, err := s.Get(context.Background(), owner, r.ID)
	if err != nil || got.Status != Queued {
		t.Fatalf("approved run not ready: %+v %v", got, err)
	}
	if len(got.Approvals) != 2 {
		t.Fatalf("parallel approval lost: %+v", got.Approvals)
	}
	next := claim(t, s, "next")
	if next.Generation <= r.Generation {
		t.Fatal("resume claim reused generation")
	}
}

func TestExecutionTransactionEvents(t *testing.T) {
	s, db, owner, wf := fixture(t)
	r := enqueue(t, s, owner, wf)
	r = claim(t, s, "w")
	if err := db.Exec(`ALTER TABLE execution_outbox ADD CONSTRAINT reject_success CHECK (topic <> 'run.succeeded')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), r, Succeeded, json.RawMessage(`{}`), ""); err == nil {
		t.Fatal("failed outbox transaction reported success")
	}
	got, err := s.Get(context.Background(), owner, r.ID)
	if err != nil || got.Status != Running {
		t.Fatal("partial state committed")
	}
	events, err := s.Events(context.Background(), owner, r.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Seq != int64(i+1) {
			t.Fatal("event sequence gap")
		}
		if e.Type == "succeeded" {
			t.Fatal("uncommitted success visible")
		}
	}
	if err := s.Cancel(context.Background(), owner, r.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Renew(context.Background(), r, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("cancel did not fence worker")
	}
}

func TestCheckpointVersionAndLease(t *testing.T) {
	s, db, owner, wf := fixture(t)
	r := enqueue(t, s, owner, wf)
	r = claim(t, s, "w")
	cp := s.Checkpoints(r)
	if err := cp.Set(context.Background(), "run", []byte("opaque-eino-checkpoint")); err != nil {
		t.Fatal(err)
	}
	body, ok, err := cp.Get(context.Background(), "run")
	if err != nil || !ok || string(body) != "opaque-eino-checkpoint" {
		t.Fatal("checkpoint not durable")
	}
	changed := *r
	changed.DefinitionHash = "different"
	if _, _, err = s.Checkpoints(&changed).Get(context.Background(), "run"); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("wrong graph accepted: %v", err)
	}
	if err = s.Checkpoints(&changed).Set(context.Background(), "run", []byte("wrong-graph")); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("wrong graph write reported success: %v", err)
	}
	db.Exec(`UPDATE workflow_runs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, r.ID)
	replacement := claim(t, s, "new")
	if err = cp.Set(context.Background(), "run", []byte("stale")); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("stale checkpoint overwritten")
	}
	if body, ok, err = s.Checkpoints(replacement).Get(context.Background(), "run"); err != nil || !ok || string(body) != "opaque-eino-checkpoint" {
		t.Fatal("restart lost checkpoint")
	}
}

func TestExecutionPreservesJSONNumbers(t *testing.T) {
	s, _, owner, wf := fixture(t)
	r, err := s.Enqueue(context.Background(), owner, wf, json.RawMessage(`{"large_id":9007199254740993}`), json.RawMessage(`{"large_id":9007199254740993}`), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(context.Background(), owner, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Input) != `{"large_id": 9007199254740993}` || string(got.Definition) != `{"large_id": 9007199254740993}` {
		t.Fatalf("input/snapshot number changed: %s %s", got.Input, got.Definition)
	}
}
