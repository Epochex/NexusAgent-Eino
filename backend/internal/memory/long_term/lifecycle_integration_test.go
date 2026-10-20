//go:build integration

package long_term

import (
	"context"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"testing"
	"time"
)

func TestMemoryAgentRevocationFencesQueuedExtraction(t *testing.T) {
	store, db, owner, session, profile := memoryFixture(t)
	agent := &model.Agent{OwnerID: &owner, Name: "revocable", ModelConfigID: profile.EmbeddingConfigID}
	if err := db.Create(agent).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(session).Update("agent_id", agent.ID).Error; err != nil {
		t.Fatal(err)
	}
	memoryMessages(t, db, session.ID, 2)
	if _, _, err := enqueueMemory(t, store, owner, session.ID, true); err != nil {
		t.Fatal(err)
	}
	job := claimMemory(t, store, "agent-worker")
	if err := store.SaveDecision(context.Background(), job, Decision{Ops: []Operation{{Op: "ADD", Content: "must not survive revocation"}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(agent).Update("status", 2).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), job); err == nil {
		t.Fatal("revoked agent extraction still applied")
	}
	var count int64
	if err := db.Model(&model.Memory{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("revoked extraction created facts", count, err)
	}
}

func TestMemoryArchiveFencesInflightVectorAndPersistsDelete(t *testing.T) {
	store, db, owner, session, _ := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	enqueueMemory(t, store, owner, session.ID, true)
	extract := claimMemory(t, store, "extract")
	if err := store.SaveDecision(context.Background(), extract, Decision{Ops: []Operation{{Op: "ADD", Content: "old fact"}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(context.Background(), extract); err != nil {
		t.Fatal(err)
	}
	stale := claimMemory(t, store, "vector-old")
	var memory model.Memory
	db.First(&memory, *stale.MemoryID)
	if err := store.Archive(context.Background(), owner+1, memory.ID, false); err == nil {
		t.Fatal("foreign archive accepted")
	}
	if err := store.Archive(context.Background(), owner, memory.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteVector(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	db.First(&memory, memory.ID)
	if memory.Status != 2 || memory.Revision != 2 || memory.IndexStatus == "ready" {
		t.Fatalf("old vector resurrected archived memory %+v", memory)
	}
	cleanup := claimMemory(t, store, "cleanup")
	if cleanup.VectorAction != "delete" || cleanup.MemoryRevision != 2 {
		t.Fatal("delete intent lost", cleanup)
	}
	if err := store.CompleteVector(context.Background(), cleanup); err != nil {
		t.Fatal(err)
	}
	db.First(&memory, memory.ID)
	if memory.IndexStatus != "removed" || memory.IndexedRevision != 2 {
		t.Fatal("delete did not confirm remote removal")
	}
}
func TestMemoryLeaseRecoveryIsFiniteAndExplicitRetryRetainsHistory(t *testing.T) {
	store, db, owner, session, _ := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	enqueueMemory(t, store, owner, session.ID, true)
	first := claimMemory(t, store, "first")
	for i := 0; i < 3; i++ {
		var current Job
		db.First(&current, first.ID)
		db.Model(&Job{}).Where("id=?", first.ID).Update("lease_until", time.Now().Add(-time.Second))
		if i < 2 {
			next := claimMemory(t, store, "replacement")
			if next.EpochAttempts != i+2 {
				t.Fatal("crash budget lost")
			}
		}
	}
	if next, err := store.Claim(context.Background(), "exhausted", time.Minute); err != nil || next != nil {
		t.Fatal("infinite crash replay", next, err)
	}
	var exhausted Job
	db.First(&exhausted, first.ID)
	if exhausted.Status != "failed" || exhausted.Attempts != 3 {
		t.Fatal("crash exhaustion not visible", exhausted)
	}
	if err := store.SaveDecision(context.Background(), first, Decision{}); !errors.Is(err, ErrMemoryLeaseLost) {
		t.Fatal("stale extraction persisted decision", err)
	}
	if err := store.Retry(context.Background(), owner, first.ID); err != nil {
		t.Fatal(err)
	}
	retried := claimMemory(t, store, "manual-retry")
	if retried.Attempts != 4 || retried.RetryEpoch != 1 || retried.EpochAttempts != 1 {
		t.Fatalf("retry history lost %+v", retried)
	}
}
