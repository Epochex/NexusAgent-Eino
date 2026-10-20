//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSharedWorkflowAdmissionFairnessAndRelease(t *testing.T) {
	s, db, owner, wf := fixture(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(16)
	if os.Getenv("NX_ADMISSION_TRACE") == "1" {
		db = db.Session(&gorm.Session{Logger: logger.New(log.New(os.Stdout, "admission ", 0), logger.Config{SlowThreshold: 20 * time.Millisecond, LogLevel: logger.Warn})})
		s = NewStore(db)
	}
	owners := []int64{owner}
	flows := []int64{wf}
	for i := 1; i < 10; i++ {
		var actor, flow int64
		if err := db.Raw("INSERT INTO users(username,email,password_hash) VALUES(?,?,'public-test-only') RETURNING id", fmt.Sprintf("capacity-owner-%d", i), fmt.Sprintf("capacity-%d@test.invalid", i)).Scan(&actor).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Raw("INSERT INTO workflows(name,dsl,creator_id) VALUES('capacity','name: capacity',?) RETURNING id", actor).Scan(&flow).Error; err != nil {
			t.Fatal(err)
		}
		owners = append(owners, actor)
		flows = append(flows, flow)
	}
	// A flooded first owner must not retain the entire claim order.
	for i := range 2 {
		for range 5 {
			enqueue(t, s, owners[i], flows[i])
		}
	}
	first := claim(t, s, "first-node")
	second := claim(t, NewStore(db.Session(&gorm.Session{NewDB: true})), "second-node")
	if first.OwnerID == second.OwnerID {
		t.Error("a flooded owner starved another eligible owner")
	}
	for i := 2; i < 10; i++ {
		for range 5 {
			enqueue(t, s, owners[i], flows[i])
		}
	}
	var wg sync.WaitGroup
	for i := range 48 {
		wg.Go(func() {
			node := NewStore(db.Session(&gorm.Session{NewDB: true}))
			if _, err := node.Claim(context.Background(), fmt.Sprintf("competing-node-%d", i), time.Minute); err != nil {
				t.Error("shared claim", err)
			}
		})
	}
	wg.Wait()
	if os.Getenv("NX_ADMISSION_TRACE") == "1" {
		pool, _ := db.DB()
		t.Logf("actual pool stats: %+v", pool.Stats())
	}
	if t.Failed() || os.Getenv("NX_ADMISSION_PLAN") == "1" {
		var plan []string
		if err := db.Raw(`EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) SELECT * FROM workflow_runs
		 WHERE definition IS NOT NULL AND available_at<=now() AND (status IN ('queued','retry_wait') OR (status='running' AND lease_until<now()))
		 AND (SELECT count(*) FROM execution_active_capacity c WHERE c.owner_id=workflow_runs.owner_id)<4
		 ORDER BY COALESCE((SELECT last_claimed_at FROM execution_owner_fairness f WHERE f.owner_id=workflow_runs.owner_id),'epoch'::timestamptz),available_at,id
		 LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&plan).Error; err != nil {
			t.Log(err)
		} else {
			t.Log("actual admission plan:\n" + strings.Join(plan, "\n"))
		}
	}
	var counts []struct {
		OwnerID int64
		Count   int64
	}
	if err := db.Table("workflow_runs").Select("owner_id,count(*) AS count").Where("status=? AND lease_until>clock_timestamp()", Running).Group("owner_id").Scan(&counts).Error; err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, row := range counts {
		total += row.Count
		if row.Count > 4 {
			t.Errorf("shared owner quota exceeded: owner=%d active=%d", row.OwnerID, row.Count)
		}
	}
	if total != 32 {
		t.Errorf("shared global quota: want32 active, got%d", total)
	}
	if err := s.Suspend(context.Background(), second, []Approval{{InterruptID: "quota-review", Path: "root/review", ArgsHash: "quota-hash", Prompt: "review"}}); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.Claim(context.Background(), "after-approval-release", time.Minute)
	if err != nil || resumed == nil {
		t.Fatal("approval wait did not release global capacity", resumed, err)
	}
	if err = s.Retry(context.Background(), resumed, time.Minute, "provider capacity backoff"); err != nil {
		t.Fatal(err)
	}
	if next, err := s.Claim(context.Background(), "after-backoff-release", time.Minute); err != nil || next == nil {
		t.Fatal("backoff retained global execution capacity", next, err)
	}
	if err = db.Exec("UPDATE workflow_runs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=?", first.ID).Error; err != nil {
		t.Fatal(err)
	}
	if next, err := s.Claim(context.Background(), "after-expired-node", time.Minute); err != nil || next == nil {
		t.Fatal("expired node retained capacity", next, err)
	}
	if err = s.Renew(context.Background(), first, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired executor renewed shared work", err)
	}
	if err = s.Finish(context.Background(), first, Succeeded, json.RawMessage(`{}`), ""); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired executor published completion", err)
	}
}

func TestSharedQueueBoundPreservesRequestIdentity(t *testing.T) {
	s, _, owner, wf := fixture(t)
	ctx := context.Background()
	definition := json.RawMessage(`{"dsl":"original","engine":"eino-v0.9.22","graph_schema":1}`)
	input := json.RawMessage(`{"q":"bounded"}`)
	var initial *Run
	for i := range 100 {
		run, err := s.EnqueueRequest(ctx, owner, wf, definition, input, fmt.Sprintf("bounded-%d", i), time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal("valid bounded queue item rejected", i, err)
		}
		if i == 0 {
			initial = run
		}
	}
	if _, err := s.EnqueueRequest(ctx, owner, wf, definition, input, "bounded-overload", time.Now().Add(time.Hour)); err == nil {
		t.Error("new work exceeded shared owner outstanding limit")
	}
	cached, err := s.EnqueueRequest(ctx, owner, wf, definition, input, "bounded-0", time.Now().Add(time.Hour))
	if err != nil || cached.ID != initial.ID {
		t.Fatal("full queue hid existing idempotent request", cached, err)
	}
	if _, err = s.EnqueueRequest(ctx, owner, wf, definition, json.RawMessage(`{"q":"changed"}`), "bounded-0", time.Now().Add(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatal("full queue accepted changed-body request identity", err)
	}
}
