//go:build integration

package long_term

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func memoryFixture(t *testing.T) (*Store, *gorm.DB, int64, *model.Session, *Profile) {
	t.Helper()
	db := testpg.Open(t)
	owner := &model.User{Username: "memory-owner", Email: "memory@test.invalid", PasswordHash: "test"}
	if err := db.Create(owner).Error; err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "memory", Name: "memory", BaseURL: "http://127.0.0.1:1", Status: 1}
	if err := db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "memory", ModelName: "memory", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}
	if err := db.Create(configured).Error; err != nil {
		t.Fatal(err)
	}
	session := &model.Session{UserID: owner.ID}
	if err := db.Create(session).Error; err != nil {
		t.Fatal(err)
	}
	store := NewStore(db)
	profile, err := store.EnsureProfile(context.Background(), &Profile{UserID: owner.ID, EmbeddingConfigID: configured.ID, EmbeddingVersion: strings.Repeat("a", 64), EmbeddingDimension: 3, EmbeddingSnapshot: datatypes.JSON(`{"model":"pinned"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return store, db, owner.ID, session, profile
}
func memoryMessages(t *testing.T, db *gorm.DB, session string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if err := db.Create(&model.Message{SessionID: session, Role: "user", Content: "explicit user preference"}).Error; err != nil {
			t.Fatal(err)
		}
	}
}
func enqueueMemory(t *testing.T, store *Store, owner int64, session string, force bool) (*Job, bool, error) {
	t.Helper()
	return store.Enqueue(context.Background(), EnqueueInput{OwnerID: owner, SessionID: session, Force: force, Every: 6, Extractor: json.RawMessage(`{"config_id":1,"descriptor":{"ModelName":"test"}}`)})
}
func claimMemory(t *testing.T, store *Store, owner string) *Job {
	t.Helper()
	job, err := store.Claim(context.Background(), owner, time.Minute)
	if err != nil || job == nil {
		t.Fatal("claim memory job", err)
	}
	return job
}
func TestMemoryEnqueueCoalescesAndRespectsNewMessageThreshold(t *testing.T) {
	store, db, owner, session, _ := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	if job, created, err := enqueueMemory(t, store, owner, session.ID, false); err != nil || created || job != nil {
		t.Fatal("everyN threshold ignored", job, err)
	}
	first, created, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil || !created || first == nil {
		t.Fatal("manual enqueue failed", err)
	}
	duplicate, created, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil || created || duplicate == nil || duplicate.ID != first.ID {
		t.Fatal("same source was not coalesced", err)
	}
	if _, _, err = enqueueMemory(t, store, owner+1, session.ID, true); err == nil {
		t.Fatal("foreign session accepted")
	}
	memoryMessages(t, db, session.ID, 4)
	if job, created, err := enqueueMemory(t, store, owner, session.ID, false); err != nil || created || job != nil {
		t.Fatal("old source counted toward new threshold", job, err)
	}
	memoryMessages(t, db, session.ID, 2)
	next, created, err := enqueueMemory(t, store, owner, session.ID, false)
	if err != nil || !created || next.ID == first.ID {
		t.Fatal("new batch not queued", err)
	}
}
func TestMemoryStableOperationsPublishOnlyAfterVectorSync(t *testing.T) {
	store, db, owner, session, _ := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	first, _, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	job := claimMemory(t, store, "extractor")
	if job.ID != first.ID {
		t.Fatal("unexpected extraction")
	}
	decision := Decision{Ops: []Operation{{Op: "ADD", Content: "second fact"}, {Op: "ADD", Content: "first fact"}}, Candidates: map[int64]int64{}}
	if err = store.SaveDecision(context.Background(), job, decision); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveDecision(context.Background(), job, decision); err != nil {
		t.Fatal("same decision not idempotent", err)
	}
	if err = store.Apply(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	var memories []model.Memory
	db.Where("user_id=?", owner).Order("id").Find(&memories)
	if len(memories) != 2 || memories[0].Content != "second fact" || memories[1].Content != "first fact" {
		t.Fatalf("model op order/content corrupted %+v", memories)
	}
	for _, memory := range memories {
		if memory.IndexStatus != "pending" || memory.IndexedRevision != 0 {
			t.Fatal("unsynchronized memory published")
		}
	}
	_ = store.Apply(context.Background(), job)
	var count int64
	db.Model(&model.Memory{}).Where("user_id=?", owner).Count(&count)
	if count != 2 {
		t.Fatal("replayed extraction duplicated memories")
	}
	vectorJob := claimMemory(t, store, "vector")
	if vectorJob.Kind != "vector" || vectorJob.MemoryID == nil || *vectorJob.MemoryID != memories[0].ID {
		t.Fatalf("missing vector intent %+v", vectorJob)
	}
	if err = store.CompleteVector(context.Background(), vectorJob); err != nil {
		t.Fatal(err)
	}
	db.First(&memories[0], memories[0].ID)
	if memories[0].IndexStatus != "ready" || memories[0].IndexedRevision != 1 {
		t.Fatal("confirmed vector did not publish memory")
	}
}
func TestMemoryDecisionCannotMutateForeignOwner(t *testing.T) {
	store, db, owner, session, profile := memoryFixture(t)
	memoryMessages(t, db, session.ID, 2)
	foreign := &model.User{Username: "other", Email: "other@test.invalid", PasswordHash: "test"}
	db.Create(foreign)
	target := &model.Memory{UserID: foreign.ID, Content: "private", VectorID: "", Revision: 1, IndexedRevision: 1, EmbeddingVersion: profile.EmbeddingVersion, IndexStatus: "ready", Status: 1}
	if err := db.Create(target).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := enqueueMemory(t, store, owner, session.ID, true); err != nil {
		t.Fatal(err)
	}
	job := claimMemory(t, store, "extractor")
	decision := Decision{Candidates: map[int64]int64{target.ID: 1}, Ops: []Operation{{Op: "UPDATE", ID: target.ID, Content: "attacker"}}}
	if err := store.SaveDecision(context.Background(), job, decision); err == nil {
		t.Fatal("invented candidate authority accepted")
	}
	db.First(target, target.ID)
	if target.Content != "private" {
		t.Fatal("foreign content changed")
	}
}
