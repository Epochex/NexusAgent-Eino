//go:build integration && infrastructure

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"github.com/chengpeng-cp/nexus-agent/internal/worker"
	"github.com/minio/minio-go/v7"
)

func TestDocumentPipelineRealInfrastructure(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	addr := os.Getenv("NX_TEST_MILVUS_ADDR")
	endpoint := os.Getenv("NX_TEST_MINIO_ADDR")
	if addr == "" || endpoint == "" {
		t.Fatal("explicit Milvus/MinIO acceptance endpoints required")
	}
	cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	store := vector.NewMilvusStore(cli)
	var schema string
	services.Repos.DB.Raw("SELECT current_schema()").Scan(&schema)
	bucket := strings.ReplaceAll(schema, "_", "-")
	mc, err := infra.NewMinIO(ctx, &config.MinIO{Endpoint: endpoint, AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	// This fixture uses only public test credentials in an isolated acceptance stack.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := mc.RemoveBucket(cleanup, bucket); err != nil {
			t.Error(err)
		}
	})
	var batch atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model string
			Input []string
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		if request.Model != "pinned-v1" {
			t.Error("model settings drifted: ", request.Model)
		}
		if len(request.Input) != 1 {
			t.Error("unexpected batch size")
		}
		if request.Input[0] != "query" && batch.Add(1) > 1 && fail.Load() {
			http.Error(w, "injected embedding failure", 400)
			return
		}
		fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1,0,0]}]}`)
	}))
	defer peer.Close()
	services.Cfg.Milvus.Dim = 7
	services.Cfg.RAG = config.RAG{ChunkSize: 64, ChunkOverlap: 8, BatchEmbed: 1, DenseTopK: 8, SparseTopK: 8, FusedTopN: 8, FinalTopK: 4, RRFK: 60}
	services.Cfg.Worker = config.Worker{Concurrency: 1, MaxRetry: 3}
	services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4, CircuitErrorRate: 1, CircuitOpen: time.Second})
	services.Vector = store
	rag := engine.New(services.Gateway, store, services.Repos.KB, &services.Cfg.RAG, 7)
	rag.SetEmbeddingResolver(services.ResolveKBEmbedding)
	services.SetRAGEngine(rag)
	services.KB = NewKBService(services)
	services.KB.SetMinIO(mc, bucket)
	provider := &model.ModelProvider{Code: "test-local", Name: "test", BaseURL: peer.URL, Status: 1}
	if err = services.Repos.DB.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "embed", ModelName: "pinned-v1", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}
	if err = services.Repos.DB.Create(configured).Error; err != nil {
		t.Fatal(err)
	}
	kb := &model.KnowledgeBase{Name: "pipeline", EmbeddingConfigID: configured.ID, ChunkSize: 64, ChunkOverlap: 8}
	if err = services.KB.CreateKB(ctx, owner, kb); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := store.DropCollection(cleanup, kb.MilvusCollection); err != nil {
			t.Error(err)
		}
	})
	services.Repos.DB.Model(configured).Update("model_name", "edited-v2")
	doc, err := services.KB.UploadDoc(ctx, owner, kb.ID, "pipeline.txt", []byte(strings.Repeat("字", 100)))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := services.KB.UploadDoc(ctx, owner, kb.ID, "pipeline.txt", []byte(strings.Repeat("字", 100)))
	if err != nil || duplicate.ID != doc.ID {
		t.Fatal("upload did not deduplicate", err)
	}
	processCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.New(nil, services.Repos, rag, mc, bucket, &services.Cfg.Worker).Run(processCtx) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("worker did not drain")
		}
	})
	wait := func(status string) *model.Document {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			got, e := services.Repos.KB.GetDoc(ctx, doc.ID)
			if e == nil && got.Status == status {
				return got
			}
			time.Sleep(50 * time.Millisecond)
		}
		var job map[string]any
		services.Repos.DB.Table("document_jobs").Take(&job)
		t.Fatalf("document did not reach %s; job=%v", status, job)
		return nil
	}
	wait("retry_wait")
	refs, err := rag.Retrieve(ctx, kb, "query")
	if err != nil || len(refs) != 0 {
		t.Fatalf("partial vectors leaked: %+v %v", refs, err)
	}
	var before []model.DocumentChunk
	services.Repos.DB.Where("document_id=?", doc.ID).Order("seq").Find(&before)
	if len(before) != 2 {
		t.Fatalf("expected two chunks, got %d", len(before))
	}
	fail.Store(false)
	ready := wait("ready")
	if ready.ChunkCount != 2 || ready.ReadyVersion != 1 || ready.TokenCount <= 0 {
		t.Fatalf("false ready stats %+v", ready)
	}
	var after []model.DocumentChunk
	services.Repos.DB.Where("document_id=?", doc.ID).Order("seq").Find(&after)
	if len(after) != 2 || before[0].ID != after[0].ID || before[1].ID != after[1].ID {
		t.Fatal("retry changed stable chunk IDs")
	}
	count, err := store.Count(ctx, kb.MilvusCollection)
	if err != nil || count != 2 {
		t.Fatalf("vector retry count=%d err=%v", count, err)
	}
	refs, err = rag.Retrieve(ctx, kb, "query")
	if err != nil || len(refs) != 2 || refs[0].Filename != "pipeline.txt" {
		t.Fatalf("published content unavailable: %+v %v", refs, err)
	}
	if err = services.KB.DeleteDoc(ctx, owner, kb.ID, doc.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		services.Repos.DB.Table("document_jobs").Select("status").Where("document_id=? AND kind='delete'", doc.ID).Scan(&status)
		if status == "done" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var deleteStatus string
	services.Repos.DB.Table("document_jobs").Select("status").Where("document_id=? AND kind='delete'", doc.ID).Scan(&deleteStatus)
	if deleteStatus != "done" {
		t.Fatalf("delete not durable: %s", deleteStatus)
	}
	count, err = store.Count(ctx, kb.MilvusCollection)
	if err != nil || count != 0 {
		t.Fatalf("deleted vectors remain: %d %v", count, err)
	}
	if _, err = mc.StatObject(ctx, bucket, doc.ObjectKey, minio.StatObjectOptions{}); err == nil {
		t.Error("deleted object remains")
	}

	// Explicit migration reads the legacy source, creates a verified replacement,
	// and schedules removal of the old source instead of certifying old chunks.
	legacyBytes := []byte("legacy source for explicit migration")
	legacy := &model.Document{KBID: kb.ID, Filename: "legacy.txt", ObjectKey: "legacy/source.txt", Status: "needs_reindex"}
	if err = services.Repos.DB.Create(legacy).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = mc.PutObject(ctx, bucket, legacy.ObjectKey, bytes.NewReader(legacyBytes), int64(len(legacyBytes)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement, err := services.KB.ReindexDoc(ctx, owner, kb.ID, legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID == legacy.ID || len(replacement.ContentHash) != 64 {
		t.Fatal("legacy source silently certified")
	}
	doc = replacement
	wait("ready")
	var archived model.Document
	services.Repos.DB.Unscoped().First(&archived, legacy.ID)
	if !archived.ArchivedAt.Valid {
		t.Fatal("legacy generation not archived")
	}

	// Simulate API death after confirmed S3 PUT but before final enqueue.
	reservedBytes := []byte("confirmed object with interrupted HTTP finalization")
	digest := sha256.Sum256(reservedBytes)
	reserved := &model.Document{KBID: kb.ID, Filename: "reserved.txt", ObjectKey: "reserved/source.txt", FileSize: int64(len(reservedBytes)), ContentHash: hex.EncodeToString(digest[:]), IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: "uploading"}
	if err = services.Repos.DB.Create(reserved).Error; err != nil {
		t.Fatal(err)
	}
	reservation, err := ingestion.NewStore(services.Repos.DB).Reserve(ctx, owner, reserved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mc.PutObject(ctx, bucket, reserved.ObjectKey, bytes.NewReader(reservedBytes), int64(len(reservedBytes)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = services.Repos.DB.Model(reservation).Update("available_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	doc = reserved
	wait("ready")
	var recovered ingestion.Job
	services.Repos.DB.First(&recovered, reservation.ID)
	if recovered.Attempts != 1 || recovered.Status != "done" {
		t.Fatalf("reserved upload was not recovered exactly once %+v", recovered)
	}
	// A missing object can exhaust recovery. An explicit reupload repairs the
	// same reserved source and resets only its retry epoch, not its total count.
	repairBytes := []byte("source repaired by explicit reupload")
	repairHash := sha256.Sum256(repairBytes)
	repair := &model.Document{KBID: kb.ID, Filename: "repair.txt", ObjectKey: "repair/source.txt", FileSize: int64(len(repairBytes)), ContentHash: hex.EncodeToString(repairHash[:]), IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: "uploading"}
	if err = services.Repos.DB.Create(repair).Error; err != nil {
		t.Fatal(err)
	}
	repairJob, err := ingestion.NewStore(services.Repos.DB).ReserveWithBudget(ctx, owner, repair.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	services.Repos.DB.Model(repairJob).Update("available_at", time.Now().Add(-time.Second))
	doc = repair
	wait("failed")
	repaired, err := services.KB.UploadDoc(ctx, owner, kb.ID, repair.Filename, repairBytes)
	if err != nil || repaired.ID != repair.ID {
		t.Fatal("failed reservation not reused", err)
	}
	if _, err = mc.StatObject(ctx, bucket, repair.ObjectKey, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("explicit reupload did not repair missing source: %v", err)
	}
	wait("ready")
	var retried ingestion.Job
	services.Repos.DB.First(&retried, repairJob.ID)
	if retried.Attempts != 2 || retried.RetryEpoch != 1 {
		t.Fatalf("repair lost retry history %+v", retried)
	}
	if err = services.KB.DeleteKB(ctx, owner, kb.ID); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var pending int64
		services.Repos.DB.Model(&ingestion.Job{}).Where("kb_id=? AND kind='delete' AND status<>'done'", kb.ID).Count(&pending)
		if pending == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var pending int64
	services.Repos.DB.Model(&ingestion.Job{}).Where("kb_id=? AND kind='delete' AND status<>'done'", kb.ID).Count(&pending)
	if pending != 0 {
		t.Fatal("KB cleanup jobs did not finish")
	}
	count, err = store.Count(ctx, kb.MilvusCollection)
	if err != nil || count != 0 {
		t.Fatalf("KB deletion left vectors %d %v", count, err)
	}
}
