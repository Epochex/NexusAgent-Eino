//go:build integration

package ingestion

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func fixture(t *testing.T) (*Store, *gorm.DB, int64, *model.KnowledgeBase, *model.Document) {
	t.Helper()
	db := testpg.Open(t)
	owner := &model.User{Username: "ingest-owner", Email: "ingest@test.invalid", PasswordHash: "test"}
	if err := db.Create(owner).Error; err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "embed", Name: "embed", BaseURL: "http://127.0.0.1:1", Status: 1}
	if err := db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "embed", ModelName: "embed", Type: model.ModelTypeEmbed, Status: 1}
	if err := db.Create(configured).Error; err != nil {
		t.Fatal(err)
	}
	kb := &model.KnowledgeBase{Name: "ingest", UserID: owner.ID, EmbeddingConfigID: configured.ID, EmbeddingVersion: strings.Repeat("a", 64), EmbeddingDimension: 3, EmbeddingSnapshot: datatypes.JSON(`{}`), MilvusCollection: "ingest"}
	if err := db.Create(kb).Error; err != nil {
		t.Fatal(err)
	}
	doc := &model.Document{KBID: kb.ID, Filename: "fixture.txt", ObjectKey: "test/fixture", FileSize: 4, ContentHash: strings.Repeat("b", 64), IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: model.DocStatusPending}
	if err := db.Create(doc).Error; err != nil {
		t.Fatal(err)
	}
	return NewStore(db), db, owner.ID, kb, doc
}
func claimed(t *testing.T, s *Store, name string) *Job {
	t.Helper()
	job, err := s.Claim(context.Background(), name, time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim %+v %v", job, err)
	}
	return job
}

func TestIngestionLeaseFencesPublication(t *testing.T) {
	s, db, owner, kb, doc := fixture(t)
	first, err := s.Enqueue(context.Background(), owner, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := s.Enqueue(context.Background(), owner, doc.ID)
	if err != nil || duplicate.ID != first.ID {
		t.Fatal("enqueue duplicated job: ", err)
	}
	if _, err = s.Enqueue(context.Background(), owner+1, doc.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign enqueue %v", err)
	}
	old := claimed(t, s, "old")
	db.Exec(`UPDATE document_jobs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, old.ID)
	fresh := claimed(t, s, "replacement")
	if fresh.Generation <= old.Generation {
		t.Fatal("generation did not advance")
	}
	if err = s.Stage(context.Background(), old, model.DocStatusEmbedding, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late stage accepted: %v", err)
	}
	if err = s.Complete(context.Background(), old, Stats{ChunkCount: 2, TokenCount: 5}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late publication accepted: %v", err)
	}
	chunks := []model.DocumentChunk{{KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 0, Content: "first", TokenCount: 2}, {KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 1, Content: "second", TokenCount: 3}}
	if err = db.Create(&chunks).Error; err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(context.Background(), fresh, Stats{ChunkCount: 2, TokenCount: 5, Warnings: []string{"OCR not performed"}}); err != nil {
		t.Fatal(err)
	}
	var saved model.Document
	if err = db.First(&saved, doc.ID).Error; err != nil {
		t.Fatal(err)
	}
	if saved.Status != model.DocStatusReady || saved.ReadyVersion != 1 || saved.TokenCount != 5 || !strings.Contains(string(saved.ParseWarnings), "OCR not performed") {
		t.Fatalf("incomplete publication %+v", saved)
	}
	var currentKB model.KnowledgeBase
	db.First(&currentKB, kb.ID)
	if currentKB.DocCount != 1 {
		t.Fatalf("KB count not published: %d", currentKB.DocCount)
	}
	if err = s.Complete(context.Background(), fresh, Stats{ChunkCount: 2}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("repeated completion %v", err)
	}
}
func TestIngestionRetryAndArchive(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	first := claimed(t, s, "retry")
	if err := s.Fail(context.Background(), first, errors.New("temporary embed failure")); err != nil {
		t.Fatal(err)
	}
	if next, err := s.Claim(context.Background(), "too-early", time.Minute); err != nil || next != nil {
		t.Fatalf("backoff ignored %+v %v", next, err)
	}
	db.Exec(`UPDATE document_jobs SET available_at=NOW()-INTERVAL '1 second' WHERE id=?`, first.ID)
	next := claimed(t, s, "next")
	if next.Attempts != 2 {
		t.Fatalf("attempt count %+v", next)
	}
	if err := s.Cancel(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(context.Background(), next, model.DocStatusReady, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("archived job resurrected %v", err)
	}
	var count int64
	db.Model(&model.Document{}).Where("id=?", doc.ID).Count(&count)
	if count != 0 {
		t.Fatal("archived source still visible")
	}
}
func TestIngestionOutboxIsAtomic(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	if err := db.Exec(`ALTER TABLE execution_outbox ADD CONSTRAINT reject_document_notification CHECK(topic<>'document.index')`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err == nil {
		t.Fatal("ignored failed notification intent")
	}
	var count int64
	db.Table("document_jobs").Count(&count)
	if count != 0 {
		t.Fatal("job escaped failed transaction")
	}
}

func TestIncompleteIndexCannotPublish(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	job := claimed(t, s, "partial")
	if err := s.Complete(context.Background(), job, Stats{ChunkCount: 1, TokenCount: 10}); !errors.Is(err, ErrConflict) {
		t.Fatalf("partial index published %v", err)
	}
	var current model.Document
	db.First(&current, doc.ID)
	if current.ReadyVersion != 0 || current.Status == model.DocStatusReady {
		t.Fatal("partial content became visible")
	}
}
func TestDeletionWorkRemainsExecutable(t *testing.T) {
	s, _, owner, _, doc := fixture(t)
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	cleanup := claimed(t, s, "cleanup")
	if cleanup.Kind != "delete" {
		t.Fatalf("wrong cleanup work %+v", cleanup)
	}
	if err := s.CompleteDelete(context.Background(), cleanup); err != nil {
		t.Fatalf("archived source blocked cleanup %v", err)
	}
}
func TestCrashingDocumentHasFiniteRetryBudget(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		job := claimed(t, s, "crash")
		db.Exec(`UPDATE document_jobs SET lease_until=NOW()-INTERVAL '1 second' WHERE id=?`, job.ID)
	}
	if job, err := s.Claim(context.Background(), "fourth-crash", time.Minute); err != nil || job != nil {
		t.Fatalf("crash loop exceeded budget %+v %v", job, err)
	}
	var current model.Document
	db.First(&current, doc.ID)
	if current.Status != model.DocStatusFailed {
		t.Fatal("poison document not stopped")
	}
	if err := s.Retry(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	retried := claimed(t, s, "explicit-retry")
	if retried.Attempts != 4 || retried.EpochAttempts != 1 || retried.RetryEpoch != 1 {
		t.Fatalf("manual retry erased history %+v", retried)
	}
}

func TestUploadReservationFinalizesOneJob(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	reserved, err := s.Reserve(context.Background(), owner, doc.ID)
	if err != nil || reserved.Status != "awaiting_source" {
		t.Fatalf("reservation %+v %v", reserved, err)
	}
	if early, err := s.Claim(context.Background(), "before-put", time.Minute); err != nil || early != nil {
		t.Fatalf("unconfirmed upload ran early %+v %v", early, err)
	}
	finalized, err := s.Enqueue(context.Background(), owner, doc.ID)
	if err != nil || finalized.ID != reserved.ID || finalized.Status != "queued" {
		t.Fatalf("finalize duplicated/lost reservation %+v %v", finalized, err)
	}
	job := claimed(t, s, "confirmed")
	source, kb, err := s.Source(context.Background(), job)
	if err != nil || source.ID != doc.ID || kb.ID != doc.KBID {
		t.Fatalf("fenced source inaccessible %+v %v", source, err)
	}
	var count int64
	db.Table("document_jobs").Count(&count)
	if count != 1 {
		t.Fatal("reservation created multiple jobs")
	}
}
func TestUploadReservationRecoversAfterLostHTTPFinalize(t *testing.T) {
	s, db, owner, _, doc := fixture(t)
	reserved, err := s.Reserve(context.Background(), owner, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE document_jobs SET available_at=NOW()-INTERVAL '1 second' WHERE id=?`, reserved.ID)
	recovered := claimed(t, s, "lost-http")
	finalized, err := s.Enqueue(context.Background(), owner, doc.ID)
	if err != nil || finalized.ID != recovered.ID || finalized.Status != "running" {
		t.Fatalf("late HTTP reset live recovery %+v %v", finalized, err)
	}
	if err = s.Stage(context.Background(), recovered, model.DocStatusParsing, nil); err != nil {
		t.Fatal("finalize invalidated recovery lease: ", err)
	}
}
