//go:build integration

package long_term

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/datatypes"
	"strings"
	"testing"
)

func TestMemoryProfileRebuildIsIdempotentAndFencesOldSpace(t *testing.T) {
	store, db, owner, session, profile := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	enqueueMemory(t, store, owner, session.ID, true)
	initial := claimMemory(t, store, "initial")
	store.SaveDecision(context.Background(), initial, Decision{Ops: []Operation{{Op: "ADD", Content: "stable fact"}}})
	if err := store.Apply(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	vector := claimMemory(t, store, "initial-vector")
	if err := store.CompleteVector(context.Background(), vector); err != nil {
		t.Fatal(err)
	}
	var memory model.Memory
	db.First(&memory, *vector.MemoryID)
	memoryMessages(t, db, session.ID, 2)
	enqueueMemory(t, store, owner, session.ID, true)
	stale := claimMemory(t, store, "old-extractor")
	if err := store.SaveDecision(context.Background(), stale, Decision{Ops: []Operation{{Op: "ADD", Content: "must not appear in old space"}}}); err != nil {
		t.Fatal(err)
	}
	candidate := &Profile{UserID: owner, EmbeddingConfigID: profile.EmbeddingConfigID, EmbeddingVersion: strings.Repeat("b", 64), EmbeddingDimension: 4, EmbeddingSnapshot: datatypes.JSON(`{"model":"new"}`)}
	result, err := store.RebuildProfile(context.Background(), candidate, "rebuild-1")
	if err != nil || result.Reindexed != 1 {
		t.Fatal("profile rebuild failed", result, err)
	}
	same, err := store.RebuildProfile(context.Background(), candidate, "rebuild-1")
	if err != nil || same.ID != result.ID {
		t.Fatal("rebuild retry duplicated work", err)
	}
	if err = store.Apply(context.Background(), stale); err == nil {
		t.Fatal("old-space extraction committed after rebuild")
	}
	db.First(&memory, memory.ID)
	if memory.Revision != 2 || memory.EmbeddingVersion != candidate.EmbeddingVersion || memory.IndexStatus != "pending" {
		t.Fatal("rebuilt memory visible before new vector", memory)
	}
	var current Profile
	db.Where("user_id=? AND active", owner).First(&current)
	if current.EmbeddingVersion != candidate.EmbeddingVersion {
		t.Fatal("active profile not switched")
	}
	// Same message boundary under the new space is an explicit fresh extraction,
	// while ordinary repeated requests under one space still coalesce.
	fresh, created, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil || !created || fresh.EmbeddingVersion != candidate.EmbeddingVersion {
		t.Fatal("new space could not re-extract same boundary", fresh, err)
	}
	var jobs int64
	db.Model(&Job{}).Where("kind='vector' AND memory_revision=2").Count(&jobs)
	if jobs != 1 {
		t.Fatal("rebuild retry duplicated vector jobs")
	}
}
