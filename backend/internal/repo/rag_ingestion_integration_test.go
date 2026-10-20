//go:build integration

package repo

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func chunkFixture(t *testing.T) (*KBRepo, *ingestion.Store, *gorm.DB, int64, *model.KnowledgeBase, *model.Document, *ingestion.Job) {
	t.Helper()
	db := testpg.Open(t)
	owner := &model.User{Username: "chunk-owner", Email: "chunk@test.invalid", PasswordHash: "test"}
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
	kb := &model.KnowledgeBase{Name: "chunks", UserID: owner.ID, EmbeddingConfigID: configured.ID, EmbeddingVersion: strings.Repeat("a", 64), EmbeddingDimension: 3, MilvusCollection: "chunks"}
	if err := db.Create(kb).Error; err != nil {
		t.Fatal(err)
	}
	doc := &model.Document{KBID: kb.ID, Filename: "fixture.txt", ObjectKey: "fixture", ContentHash: strings.Repeat("b", 64), IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: model.DocStatusPending}
	if err := db.Create(doc).Error; err != nil {
		t.Fatal(err)
	}
	store := ingestion.NewStore(db)
	if _, err := store.Enqueue(context.Background(), owner.ID, doc.ID); err != nil {
		t.Fatal(err)
	}
	job, err := store.Claim(context.Background(), "chunk-writer", time.Minute)
	if err != nil || job == nil {
		t.Fatal(err)
	}
	return New(db).KB, store, db, owner.ID, kb, doc, job
}
func TestChunksRetryKeepsIDsAndHidesIncompleteIndex(t *testing.T) {
	repository, store, db, _, kb, doc, job := chunkFixture(t)
	ctx := ingestion.WithJob(context.Background(), job)
	rows := []model.DocumentChunk{{KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 0, Content: "reliable first", TokenCount: 3, Meta: datatypes.JSON(`{"page":1}`)}, {KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 1, Content: "reliable second", TokenCount: 4, Meta: datatypes.JSON(`{"page":2}`)}}
	if err := repository.BatchCreate(ctx, rows); err != nil {
		t.Fatal(err)
	}
	ids := []int64{rows[0].ID, rows[1].ID}
	retry := append([]model.DocumentChunk(nil), rows...)
	for i := range retry {
		retry[i].ID = 0
	}
	if err := repository.BatchCreate(ctx, retry); err != nil {
		t.Fatalf("retry inserted duplicate chunks: %v", err)
	}
	if retry[0].ID != ids[0] || retry[1].ID != ids[1] {
		t.Fatal("retry changed vector primary keys")
	}
	var count int64
	db.Model(&model.DocumentChunk{}).Count(&count)
	if count != 2 {
		t.Fatalf("duplicates %d", count)
	}
	found, err := repository.ListByIDs(ctx, ids)
	if err != nil || len(found) != 0 {
		t.Fatalf("partial index visible: %+v %v", found, err)
	}
	sparse, err := repository.KeywordSearch(ctx, kb.ID, []string{"reliable"}, 10)
	if err != nil || len(sparse) != 0 {
		t.Fatalf("partial sparse index visible: %+v %v", sparse, err)
	}
	retry[0].Content = "changed source"
	if err = repository.BatchCreate(ctx, retry); err == nil {
		t.Fatal("same generation accepted different content")
	}
	if err = store.Complete(ctx, job, ingestion.Stats{ChunkCount: 2, TokenCount: 7}); err != nil {
		t.Fatal(err)
	}
	found, err = repository.ListByIDs(ctx, ids)
	if err != nil || len(found) != 2 {
		t.Fatalf("ready version not visible: %+v %v", found, err)
	}
}
func TestStaleChunkWriterIsRejected(t *testing.T) {
	repository, _, db, _, kb, doc, job := chunkFixture(t)
	db.Exec(`UPDATE document_jobs SET generation=generation+1 WHERE id=?`, job.ID)
	rows := []model.DocumentChunk{{KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 0, Content: "late"}}
	if err := repository.BatchCreate(ingestion.WithJob(context.Background(), job), rows); err == nil {
		t.Fatal("stale generation wrote chunks")
	}
}

func TestPublishedScopeUsesReadyGenerationsAndCurrentOwner(t *testing.T) {
	repository, store, _, owner, kb, doc, job := chunkFixture(t)
	ctx := security.WithUserID(context.Background(), owner)
	scope, err := repository.PublishedScope(ctx, kb)
	if err != nil || len(scope.DocumentVersions) != 0 {
		t.Fatal("partial document in ANN scope", scope, err)
	}
	rows := []model.DocumentChunk{{KBID: kb.ID, DocumentID: doc.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 0, Content: "published scope", TokenCount: 4, Meta: datatypes.JSON(`{"page":1}`)}}
	if err = repository.BatchCreate(ingestion.WithJob(ctx, job), rows); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, job, ingestion.Stats{ChunkCount: 1, TokenCount: 4}); err != nil {
		t.Fatal(err)
	}
	scope, err = repository.PublishedScope(ctx, kb)
	if err != nil || scope.DocumentVersions[doc.ID] != 1 || scope.UserID != owner {
		t.Fatal("ready generation absent from ANN scope", scope, err)
	}
	foreign := security.WithUserID(context.Background(), owner+1)
	if _, err = repository.PublishedScope(foreign, kb); err == nil {
		t.Fatal("foreign scope accepted")
	}
	found, err := repository.ListByIDs(foreign, []int64{rows[0].ID})
	if err != nil || len(found) != 0 {
		t.Fatal("foreign authoritative content exposed", found, err)
	}
	if err = store.Cancel(ctx, owner, doc.ID); err != nil {
		t.Fatal(err)
	}
	scope, err = repository.PublishedScope(ctx, kb)
	if err != nil || len(scope.DocumentVersions) != 0 {
		t.Fatal("archived generation in ANN scope", scope, err)
	}
}
