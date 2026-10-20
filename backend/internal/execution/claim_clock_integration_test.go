//go:build integration

package execution

import (
	"context"
	"testing"
	"time"
)

func TestClaimUsesCurrentClockAfterCapacityLockWait(t *testing.T) {
	s, db, owner, flow := fixture(t)
	run := enqueue(t, s, owner, flow)
	if err := db.Exec("UPDATE workflow_runs SET available_at=clock_timestamp()+interval '200 milliseconds' WHERE id=?", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const key = "hashtextextended(current_schema()||':capacity:run:',0)"
	if _, err = conn.ExecContext(context.Background(), "SELECT pg_advisory_lock("+key+")"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock("+key+")")
	var blocker int
	if err = conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		run *Run
		err error
	}
	done := make(chan answer, 1)
	go func() { r, e := s.Claim(context.Background(), "clock-wait", time.Minute); done <- answer{r, e} }()
	waiting := false
	for until := time.Now().Add(time.Second); time.Now().Before(until); {
		var count int64
		if err = db.Raw("SELECT count(*) FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))", blocker).Scan(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			waiting = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("claim did not enter the intended capacity lock wait")
	}
	for until := time.Now().Add(time.Second); ; {
		var ready bool
		if err = db.Raw("SELECT available_at<=clock_timestamp() FROM workflow_runs WHERE id=?", run.ID).Scan(&ready).Error; err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		if time.Now().After(until) {
			t.Fatal("DB availability time did not pass")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock("+key+")"); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.run == nil || result.run.ID != run.ID {
			t.Fatalf("claim used transaction-start time after waiting: run=%+v err=%v", result.run, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("claim failed to return after unlocking")
	}
}
