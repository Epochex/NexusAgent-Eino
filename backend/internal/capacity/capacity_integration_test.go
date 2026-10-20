//go:build integration

package capacity_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/gorm"
)

func actors(t *testing.T) (*gorm.DB, []int64, []int64) {
	t.Helper()
	db := testpg.Open(t)
	owners, flows := make([]int64, 3), make([]int64, 3)
	for i := range owners {
		if err := db.Raw("INSERT INTO users(username,email,password_hash) VALUES(?,?,'public-test-only') RETURNING id", fmt.Sprintf("mixed-%d", i), fmt.Sprintf("mixed-%d@test.invalid", i)).Scan(&owners[i]).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Raw("INSERT INTO workflows(name,dsl,creator_id) VALUES('mixed','name: mixed',?) RETURNING id", owners[i]).Scan(&flows[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db, owners, flows
}
func TestInteractiveAndWorkflowUseOneSharedQuota(t *testing.T) {
	db, owners, flows := actors(t)
	if err := db.Exec("UPDATE execution_capacity_policies SET active_limit=4,owner_active_limit=2,version=version+1 WHERE scope='run'").Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := capacity.New(db)
	other := capacity.New(db)
	_, release1, err := first.HoldRun(ctx, owners[0])
	if err != nil {
		t.Fatal(err)
	}
	defer release1()
	_, release2, err := other.HoldRun(ctx, owners[0])
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	if _, _, err = other.HoldRun(ctx, owners[0]); !errors.Is(err, capacity.ErrBusy) {
		t.Fatal("shared interactive owner limit", err)
	}
	store := execution.NewStore(db)
	for i := range 2 {
		for range 2 {
			if _, err = store.Enqueue(ctx, owners[i], flows[i], json.RawMessage(`{"dsl":"original"}`), nil, time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for range 2 {
		run, err := store.Claim(ctx, "mixed-worker", time.Minute)
		if err != nil || run == nil || run.OwnerID != owners[1] {
			t.Fatal("workflow did not respect interactive owner usage", run, err)
		}
	}
	if _, _, err = other.HoldRun(ctx, owners[2]); !errors.Is(err, capacity.ErrBusy) {
		t.Fatal("interactive caller ignored active workflow quota", err)
	}
	if run, err := store.Claim(ctx, "mixed-overload", time.Minute); err != nil || run != nil {
		t.Fatal("workflow ignored active interactive quota", run, err)
	}
	release1()
	if run, err := store.Claim(ctx, "mixed-reclaimed", time.Minute); err != nil || run == nil || run.OwnerID != owners[0] {
		t.Fatal("interactive release did not free workflow capacity", run, err)
	}
}

func TestExpiredLeaseCannotRenewOrReleaseReplacement(t *testing.T) {
	db, owners, _ := actors(t)
	if err := db.Exec("UPDATE execution_capacity_policies SET active_limit=2,owner_active_limit=1,lease_ms=1000,version=version+1 WHERE scope='run'").Error; err != nil {
		t.Fatal(err)
	}
	store := capacity.New(db)
	old, releaseOld, err := store.HoldRun(context.Background(), owners[0])
	if err != nil {
		t.Fatal(err)
	}
	defer releaseOld()
	if err = db.Exec("UPDATE execution_capacity_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='run' AND owner_id=?", owners[0]).Error; err != nil {
		t.Fatal(err)
	}
	replacement, releaseNew, err := capacity.New(db).HoldRun(context.Background(), owners[0])
	if err != nil {
		t.Fatal("expired capacity not reclaimed", err)
	}
	defer releaseNew()
	select {
	case <-old.Done():
		if !errors.Is(context.Cause(old), capacity.ErrLeaseLost) {
			t.Fatal("old owner loss not explicit", context.Cause(old))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expired holder kept working")
	}
	releaseOld()
	if replacement.Err() != nil {
		t.Fatal("old release canceled replacement")
	}
	var count int64
	if err = db.Table("execution_active_capacity").Where("source='chat' AND owner_id=?", owners[0]).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("old release removed replacement quota", count, err)
	}
}
