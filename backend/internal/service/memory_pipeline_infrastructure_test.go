//go:build integration && infrastructure

package service

import (
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"gorm.io/datatypes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryPipelineRealInfrastructure(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	var fail atomic.Bool
	fail.Store(true)
	var facts atomic.Int32
	var updated, opsFail atomic.Bool
	var goID, englishID atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chat/completions" {
			var request struct{ Messages []llm.Message }
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid", 400)
				return
			}
			if len(request.Messages) == 0 {
				http.Error(w, "invalid", 400)
				return
			}
			body := ""
			switch {
			case strings.Contains(request.Messages[0].Content, "MEMORY_FACTS"):
				facts.Add(1)
				body = `["用户喜欢 Go","用户习惯英语"]`
				if updated.Load() {
					body = `["用户喜欢 Rust","用户偏好日语"]`
				}
			case strings.Contains(request.Messages[0].Content, "MEMORY_OPS"):
				if opsFail.Load() {
					http.Error(w, "injected operations failure", 400)
					return
				}
				encoded, _ := json.Marshal([]long_term.Operation{{Op: "ADD", Content: "用户偏好日语"}, {Op: "UPDATE", ID: goID.Load(), Content: "用户喜欢 Rust"}, {Op: "DELETE", ID: englishID.Load()}})
				body = string(encoded)
			default:
				http.Error(w, "unexpected phase", 400)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": body}, "finish_reason": "stop"}}})
			return
		}
		if r.URL.Path == "/embeddings" {
			var request struct {
				Input []string
				Model string
			}
			// Model identity determines the pinned vector dimension in this peer.
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid", 400)
				return
			}
			result := make([]map[string]any, 0, len(request.Input))
			for i, text := range request.Input {
				if text == "用户习惯英语" && fail.Load() {
					http.Error(w, "injected memory sync failure", 400)
					return
				}
				value := []float32{1, 0, 0}
				if strings.Contains(text, "英语") || strings.Contains(text, "日语") {
					value = []float32{0, 1, 0}
				}
				if strings.Contains(text, "Rust") {
					value = []float32{0.6, 0, 0.8}
				}
				if request.Model == "memory-v4" {
					value = append(value, 0)
				}
				result = append(result, map[string]any{"index": i, "embedding": value})
			}
			json.NewEncoder(w).Encode(map[string]any{"data": result})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(peer.Close)
	client, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	vectors := vector.NewMilvusStore(client)
	services.Vector = vectors
	services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4, CircuitErrorRate: 1})
	services.Cfg.Milvus.Dim = 7
	services.Cfg.LLM.DefaultEmbedding = "memory-embed"
	services.Cfg.LLM.DefaultChat = "memory-chat"
	provider := &model.ModelProvider{Code: "memory-peer", Name: "memory-peer", BaseURL: peer.URL, Status: 1}
	if err = services.Repos.DB.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	for _, configured := range []*model.ModelConfig{{ProviderID: provider.ID, Alias: "memory-embed", ModelName: "memory-v1", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}, {ProviderID: provider.ID, Alias: "memory-chat", ModelName: "memory-chat", Type: model.ModelTypeChat, ContextWindow: 32768, MaxOutput: 2048, Status: 1}} {
		if err = services.Repos.DB.Create(configured).Error; err != nil {
			t.Fatal(err)
		}
	}
	session := &model.Session{UserID: owner}
	if err = services.Repos.DB.Create(session).Error; err != nil {
		t.Fatal(err)
	}
	if err = services.Repos.DB.Create(&model.Message{SessionID: session.ID, Role: "user", Content: "我喜欢 Go，平时习惯使用英语，请记住。"}).Error; err != nil {
		t.Fatal(err)
	}
	manager := long_term.NewManager(services.Gateway, services, long_term.NewStore(services.Repos.DB), vectors)
	job, created, err := manager.EnqueueSession(ctx, owner, session.ID, true, 6, 3)
	if err != nil || !created {
		t.Fatal("enqueue memory", err)
	}
	profile, err := services.EnsureMemoryProfile(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	collection := long_term.MemoryCollection(profile.EmbeddingVersion)
	collections := []string{collection}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, name := range collections {
			if err := vectors.DropCollection(cleanup, name); err != nil {
				t.Error(err)
			}
		}
	})
	worker, err := long_term.NewWorker(manager, long_term.WorkerConfig{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	running, stopWorker := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(running) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("memory worker did not drain")
		}
	})
	wait := func(predicate func() bool, description string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if predicate() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		var jobs []long_term.Job
		services.Repos.DB.Order("id").Find(&jobs)
		t.Fatalf("%s: %+v", description, jobs)
	}
	wait(func() bool {
		var count int64
		services.Repos.DB.Model(&model.Memory{}).Where("user_id=? AND content='用户习惯英语' AND index_status='retry_wait'", owner).Count(&count)
		return count == 1
	}, "sync failure not visible")
	recalled, err := manager.Recall(ctx, owner, nil, "英语")
	if err != nil || len(recalled) != 0 {
		t.Fatal("unsynced memory recalled", recalled, err)
	}
	fail.Store(false)
	wait(func() bool {
		var count int64
		services.Repos.DB.Model(&model.Memory{}).Where("user_id=? AND index_status='ready' AND indexed_revision=revision", owner).Count(&count)
		return count == 2
	}, "memory did not recover")
	recalled, err = manager.Recall(ctx, owner, nil, "英语")
	if err != nil || len(recalled) != 1 || recalled[0].Content != "用户习惯英语" {
		t.Fatal("recovered recall not authoritative", recalled, err)
	}
	count, err := vectors.Count(ctx, collection)
	if err != nil || count != 2 {
		t.Fatal("duplicate memory vector", count, err)
	}
	if facts.Load() != 1 {
		t.Fatal("vector retry repeated extraction", facts.Load())
	}
	var source long_term.Job
	services.Repos.DB.First(&source, job.ID)
	if source.Status != "done" {
		t.Fatal("source extraction not committed", source.Status)
	}

	// The second decision deliberately reorders ADD/UPDATE/DELETE relative to
	// extracted facts. Each vector must be computed from the operation's content.
	var before []model.Memory
	services.Repos.DB.Where("user_id=? AND status=1", owner).Order("id").Find(&before)
	for _, memory := range before {
		if memory.Content == "用户喜欢 Go" {
			goID.Store(memory.ID)
		}
		if memory.Content == "用户习惯英语" {
			englishID.Store(memory.ID)
		}
	}
	updated.Store(true)
	opsFail.Store(true)
	if err = services.Repos.DB.Create(&model.Message{SessionID: session.ID, Role: "user", Content: "改为喜欢 Rust，并且现在偏好日语，请更新记忆。"}).Error; err != nil {
		t.Fatal(err)
	}
	second, _, err := manager.EnqueueSession(ctx, owner, session.ID, true, 6, 3)
	if err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		var row long_term.Job
		services.Repos.DB.First(&row, second.ID)
		return row.Status == "retry_wait"
	}, "operations failure not retryable")
	if facts.Load() != 2 {
		t.Fatal("facts were not checkpointed before operations")
	}
	opsFail.Store(false)
	wait(func() bool {
		var rust, japanese, removed int64
		services.Repos.DB.Model(&model.Memory{}).Where("user_id=? AND content='用户喜欢 Rust' AND revision=2 AND index_status='ready'", owner).Count(&rust)
		services.Repos.DB.Model(&model.Memory{}).Where("user_id=? AND content='用户偏好日语' AND index_status='ready'", owner).Count(&japanese)
		services.Repos.DB.Model(&model.Memory{}).Where("id=? AND status=2 AND index_status='removed'", englishID.Load()).Count(&removed)
		return rust == 1 && japanese == 1 && removed == 1
	}, "reordered operations did not synchronize")
	if facts.Load() != 2 {
		t.Fatal("operations retry repeated extraction", facts.Load())
	}
	scope, err := long_term.NewStore(services.Repos.DB).ReadyScope(ctx, owner, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	hits, err := vectors.SearchScoped(ctx, collection, []float32{0.6, 0, 0.8}, 1, scope)
	if err != nil || len(hits) != 1 || hits[0].ID != goID.Load() || hits[0].Score < 0.99 {
		t.Fatal("UPDATE did not re-embed new content", hits, err)
	}
	recalled, err = manager.Recall(ctx, owner, nil, "日语")
	if err != nil || len(recalled) != 1 || recalled[0].Content != "用户偏好日语" {
		t.Fatal("reordered ADD used wrong fact vector", recalled, err)
	}

	// Model edits keep the existing space pinned until an explicit, idempotent
	// rebuild. Rebuild removes old-space vectors and publishes the new dimension.
	var configured model.ModelConfig
	services.Repos.DB.Where("alias='memory-embed'").First(&configured)
	services.Repos.DB.Model(&configured).Updates(map[string]any{"model_name": "memory-v4", "embedding_dimension": 4})
	if _, err = manager.Recall(ctx, owner, nil, "Rust"); err != nil {
		t.Fatal("ordinary model edit changed old space", err)
	}
	raw, version, dimension, err := services.freezeEmbeddingSpace(ctx, configured.ID, long_term.MemoryVectorSchemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	next := &long_term.Profile{UserID: owner, EmbeddingConfigID: configured.ID, EmbeddingVersion: version, EmbeddingDimension: dimension, EmbeddingSnapshot: datatypes.JSON(raw)}
	rebuild, err := long_term.NewStore(services.Repos.DB).RebuildProfile(ctx, next, "memory-migration-1")
	if err != nil || rebuild.Reindexed != 2 {
		t.Fatal("dimension migration failed", rebuild, err)
	}
	collections = append(collections, long_term.MemoryCollection(version))
	repeated, err := long_term.NewStore(services.Repos.DB).RebuildProfile(ctx, next, "memory-migration-1")
	if err != nil || repeated.ID != rebuild.ID {
		t.Fatal("migration replay duplicated jobs", err)
	}
	wait(func() bool {
		var count int64
		services.Repos.DB.Model(&model.Memory{}).Where("user_id=? AND status=1 AND embedding_version=? AND index_status='ready' AND indexed_revision=revision", owner, version).Count(&count)
		return count == 2
	}, "new memory space not published")
	oldCount, err := vectors.Count(ctx, collection)
	if err != nil || oldCount != 0 {
		t.Fatal("old memory space not cleaned", oldCount, err)
	}
	collection = long_term.MemoryCollection(version)
	recalled, err = manager.Recall(ctx, owner, nil, "日语")
	if err != nil || len(recalled) != 1 || recalled[0].Content != "用户偏好日语" {
		t.Fatal("new-dimension recall failed", recalled, err)
	}
	if err = long_term.NewStore(services.Repos.DB).Archive(ctx, owner, recalled[0].ID, true); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		var row model.Memory
		services.Repos.DB.First(&row, recalled[0].ID)
		return row.Status == 3 && row.IndexStatus == "removed"
	}, "remote memory delete not completed")
	count, err = vectors.Count(ctx, collection)
	if err != nil || count != 1 {
		t.Fatal("memory remote delete lost", count, err)
	}
}
