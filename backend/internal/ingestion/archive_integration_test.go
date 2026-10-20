//go:build integration

package ingestion

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"testing"
	"time"
)

func TestArchiveKnowledgeBaseFencesWorkAndSchedulesCleanup(t *testing.T) {
	s, db, owner, kb, doc := fixture(t)
	if _, err := s.Enqueue(context.Background(), owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	stale := claimed(t, s, "archive-race")
	if err := s.ArchiveKB(context.Background(), owner+1, kb.ID); err == nil {
		t.Fatal("foreign archive accepted")
	}
	if err := s.ArchiveKB(context.Background(), owner, kb.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(context.Background(), stale, Stats{ChunkCount: 1}); err == nil {
		t.Fatal("archived executor published")
	}
	var archived model.KnowledgeBase
	if err := db.Unscoped().First(&archived, kb.ID).Error; err != nil || !archived.ArchivedAt.Valid {
		t.Fatal("KB not archived", err)
	}
	var source model.Document
	db.Unscoped().First(&source, doc.ID)
	if !source.ArchivedAt.Valid || source.IndexVersion != 2 {
		t.Fatalf("source not fenced %+v", source)
	}
	var cleanup Job
	db.Where("document_id=? AND kind='delete'", doc.ID).First(&cleanup)
	if cleanup.Status != "queued" {
		t.Fatalf("cleanup missing %+v", cleanup)
	}
	got, err := s.Claim(context.Background(), "cleanup", time.Minute)
	if err != nil || got == nil || got.Kind != "delete" {
		t.Fatal("archived KB cleanup not executable", err)
	}
	if _, err = s.Enqueue(context.Background(), owner, doc.ID); err == nil {
		t.Fatal("archived source accepted work")
	}
}
