//go:build integration

package long_term

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"strings"
	"testing"
)

func TestMemoryManagementOwnerStatusAndPrivateJob(t *testing.T) {
	store, db, owner, session, profile := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	queued, _, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetJob(context.Background(), owner+1, queued.ID); !errors.Is(err, ErrMemoryNotFound) {
		t.Fatal("foreign job exposed", err)
	}
	job := claimMemory(t, store, "management")
	if err = store.SaveDecision(context.Background(), job, Decision{Ops: []Operation{{Op: "ADD", Content: "user prefers Go"}}}); err != nil {
		t.Fatal(err)
	}
	if err = store.Apply(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	summary, err := store.JobStatus(context.Background(), owner, queued.ID)
	if err != nil || summary.Status != "done" || summary.VectorPending != 1 || summary.VectorDone != 0 {
		t.Fatalf("committed facts falsely ready: %+v %v", summary, err)
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "Payload") || strings.Contains(string(encoded), "extractor\"") || strings.Contains(string(encoded), "LeaseOwner") || !strings.Contains(string(encoded), "\"epoch_attempts\"") {
		t.Fatalf("private/unstable job DTO: %s", encoded)
	}
	vectorJob := claimMemory(t, store, "vector")
	if err = store.CompleteVector(context.Background(), vectorJob); err != nil {
		t.Fatal(err)
	}
	summary, err = store.JobStatus(context.Background(), owner, queued.ID)
	if err != nil || summary.VectorPending != 0 || summary.VectorDone != 1 {
		t.Fatalf("projection completion missing %+v %v", summary, err)
	}
	scope, err := store.ReadyScope(context.Background(), owner, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RecordHits(context.Background(), owner, nil, profile, scope.MemoryRevisions); err != nil {
		t.Fatal(err)
	}
	var memory model.Memory
	if err = db.First(&memory, *vectorJob.MemoryID).Error; err != nil {
		t.Fatal(err)
	}
	if memory.HitCount != 1 || memory.LastHitAt == nil {
		t.Fatal("recall stats were not persisted")
	}
	if err = store.Archive(context.Background(), owner, memory.ID, true); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordHits(context.Background(), owner, nil, profile, scope.MemoryRevisions); err != nil {
		t.Fatal(err)
	}
	if err = db.First(&memory, memory.ID).Error; err != nil {
		t.Fatal(err)
	}
	if memory.HitCount != 1 {
		t.Fatal("stale recall updated archived row")
	}
}
