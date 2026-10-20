//go:build integration && infrastructure

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/redis/go-redis/v9"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Real HTTP/JWT/PG/Milvus/worker acceptance. Only the model service is a controlled
// peer; NX_MEMORY_BROWSER=1 additionally leaves this fixture open for UI actions.
func TestMemoryHTTPRealInfrastructure(t *testing.T) {
	if os.Getenv("NX_TEST_MILVUS_ADDR") == "" || os.Getenv("NX_TEST_REDIS_ADDR") == "" {
		t.Skip("explicit Milvus and Redis test addresses required")
	}
	db := testpg.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 14})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	var malformed atomic.Bool
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		if r.URL.Path == "/chat/completions" {
			body := `["用户偏好 Go"]`
			if malformed.Load() {
				body = `not-json`
			}
			messages, _ := request["messages"].([]any)
			if len(messages) > 0 {
				first, _ := messages[0].(map[string]any)
				if strings.Contains(fmt.Sprint(first["content"]), "MEMORY_OPS") {
					body = `[{"op":"NOOP"}]`
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": body}, "finish_reason": "stop"}}})
			return
		}
		if r.URL.Path == "/embeddings" {
			inputs, _ := request["input"].([]any)
			data := make([]any, 0, len(inputs))
			for i := range inputs {
				data = append(data, map[string]any{"index": i, "embedding": []float32{1, 0, 0}})
			}
			json.NewEncoder(w).Encode(map[string]any{"data": data})
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
	encryptor, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("memory-http-public-test-only-jwt-secret", time.Hour, time.Hour)
	cfg := &config.Config{LLM: config.LLM{DefaultChat: "memory-chat", DefaultEmbedding: "memory-embed", MaxConnsPerProvider: 4, CircuitErrorRate: 1}, Milvus: config.Milvus{Dim: 3}}
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repo.New(db), Encryptor: encryptor, JWT: jwt, RDB: rdb, Gateway: llm.NewGateway(&cfg.LLM), Vector: vectors})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := svcs.Auth.Register(ctx, &service.RegisterInput{Username: "memory-browser", Email: "memory-browser@test.invalid", Password: "memory-test-only-2026"})
	if err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "memory-http", Name: "memory-http", BaseURL: peer.URL, Status: 1}
	if err = db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	embedding := &model.ModelConfig{ProviderID: provider.ID, Alias: "memory-embed", ModelName: "embed", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}
	for _, configured := range []*model.ModelConfig{embedding, {ProviderID: provider.ID, Alias: "memory-chat", ModelName: "chat", Type: model.ModelTypeChat, ContextWindow: 32768, MaxOutput: 2048, Status: 1}} {
		if err = db.Create(configured).Error; err != nil {
			t.Fatal(err)
		}
	}
	session := &model.Session{UserID: owner.ID, Title: "记忆功能验收"}
	if err = db.Create(session).Error; err != nil {
		t.Fatal(err)
	}
	addMessage := func() {
		t.Helper()
		if err := db.Create(&model.Message{SessionID: session.ID, Role: "user", Content: "请记住我偏好 Go。"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	addMessage()
	manager := svcs.ConfigureMemory()
	profile, err := svcs.EnsureMemoryProfile(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := vectors.DropCollection(cleanup, long_term.MemoryCollection(profile.EmbeddingVersion)); err != nil {
			t.Error(err)
		}
	})
	address := "127.0.0.1:0"
	if os.Getenv("NX_MEMORY_BROWSER") == "1" {
		address = "127.0.0.1:18089"
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter), server.WithWriteTimeout(0))
	Register(h.Group("/", Recovery()), New(svcs), jwt)
	finish := make(chan struct{}, 1)
	h.POST("/__test/finish", func(_ context.Context, c *app.RequestContext) {
		select {
		case finish <- struct{}{}:
		default:
		}
		c.SetStatusCode(204)
	})
	listenerDone := make(chan error, 1)
	go func() { listenerDone <- h.Run() }()
	t.Cleanup(func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = h.Shutdown(shutdown)
		select {
		case <-listenerDone:
		case <-shutdown.Done():
			t.Error("HTTP listener did not drain")
		}
	})
	for deadline := time.Now().Add(2 * time.Second); !h.IsRunning(); {
		if time.Now().After(deadline) {
			t.Fatal("listener startup")
		}
		time.Sleep(time.Millisecond)
	}
	worker, err := long_term.NewWorker(manager, long_term.WorkerConfig{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	workCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workCtx) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(7 * time.Second):
			t.Error("memory worker did not drain")
		}
	})
	httpClient := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(httpClient.CloseIdleConnections)
	token, err := jwt.Generate(owner.ID, owner.Role, owner.Username)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := jwt.Generate(owner.ID+1, "user", "foreign")
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, asForeign bool) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, "http://"+ln.Addr().String()+"/api/v1"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		auth := token.AccessToken
		if asForeign {
			auth = foreign.AccessToken
		}
		req.Header.Set("Authorization", "Bearer "+auth)
		response, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, raw
	}
	wait := func(predicate func() bool, description string) {
		t.Helper()
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
			if predicate() {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal(description)
	}
	extractBody := fmt.Sprintf(`{"session_id":%q}`, session.ID)
	if code, _ := request("POST", "/memories/extract", extractBody, true); code != 404 {
		t.Fatal("foreign session extract", code)
	}
	code, raw := request("POST", "/memories/extract", extractBody, false)
	if code != 202 {
		t.Fatalf("extract %d %s", code, raw)
	}
	var accepted struct{ Data long_term.Job }
	if err = json.Unmarshal(raw, &accepted); err != nil {
		t.Fatal(err)
	}
	if code, _ := request("GET", fmt.Sprintf("/memories/jobs/%d", accepted.Data.ID), "", true); code != 404 {
		t.Fatal("foreign job exposed", code)
	}
	wait(func() bool {
		var count int64
		return db.Model(&model.Memory{}).Where("user_id=? AND index_status='ready'", owner.ID).Count(&count).Error == nil && count == 1
	}, "memory not ready through HTTP/worker")
	code, raw = request("GET", "/memories", "", false)
	if code != 200 || !bytes.Contains(raw, []byte("用户偏好 Go")) {
		t.Fatalf("list %d %s", code, raw)
	}
	var memory model.Memory
	if err = db.Where("user_id=?", owner.ID).First(&memory).Error; err != nil {
		t.Fatal(err)
	}
	// A malformed response must be a visible failed job; explicit retry preserves identity.
	malformed.Store(true)
	addMessage()
	code, raw = request("POST", "/memories/extract", extractBody, false)
	if code != 202 {
		t.Fatalf("second extract %d %s", code, raw)
	}
	if err = json.Unmarshal(raw, &accepted); err != nil {
		t.Fatal(err)
	}
	failedID := accepted.Data.ID
	wait(func() bool {
		var job long_term.Job
		return db.First(&job, failedID).Error == nil && job.Status == "failed"
	}, "malformed response did not fail visibly")
	malformed.Store(false)
	if code, _ := request("POST", fmt.Sprintf("/memories/jobs/%d/retry", failedID), "", true); code != 404 {
		t.Fatal("foreign retry allowed", code)
	}
	if code, raw := request("POST", fmt.Sprintf("/memories/jobs/%d/retry", failedID), "", false); code != 200 {
		t.Fatalf("retry %d %s", code, raw)
	}
	wait(func() bool {
		var job long_term.Job
		return db.First(&job, failedID).Error == nil && job.Status == "done" && job.RetryEpoch == 1
	}, "explicit retry did not recover")
	code, raw = request("GET", "/memories/jobs", "", false)
	if code != 200 || bytes.Contains(raw, []byte("descriptor")) || bytes.Contains(raw, []byte("payload")) {
		t.Fatal("private job material exposed")
	}
	rebuildBody := fmt.Sprintf(`{"embedding_config_id":%d,"request_key":"http-rebuild"}`, embedding.ID)
	code, raw = request("POST", "/memories/rebuild", rebuildBody, false)
	if code != 202 {
		t.Fatalf("rebuild %d %s", code, raw)
	}
	var result struct{ Data long_term.RebuildResult }
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	rebuildID := result.Data.ID
	code, raw = request("POST", "/memories/rebuild", rebuildBody, false)
	if err = json.Unmarshal(raw, &result); err != nil || code != 202 || result.Data.ID != rebuildID {
		t.Fatal("rebuild not idempotent", code, err)
	}
	if code, _ := request("GET", fmt.Sprintf("/memories/rebuilds/%d", rebuildID), "", true); code != 404 {
		t.Fatal("foreign rebuild exposed", code)
	}
	wait(func() bool {
		var row model.Memory
		return db.First(&row, memory.ID).Error == nil && row.Revision == 2 && row.IndexStatus == "ready"
	}, "rebuild did not publish exact revision")
	if os.Getenv("NX_MEMORY_BROWSER") == "1" {
		t.Logf("Memory browser fixture ready: http://127.0.0.1:18089; public fixture username=memory-browser password=memory-test-only-2026 session=%s. Model responses are controlled test data.", session.ID)
		select {
		case <-finish:
		case <-ctx.Done():
			t.Fatal("browser walkthrough timed out")
		}
	} else {
		if code, _ := request("POST", fmt.Sprintf("/memories/%d/archive", memory.ID), "", false); code != 200 {
			t.Fatal("archive failed", code)
		}
		wait(func() bool {
			var row model.Memory
			return db.First(&row, memory.ID).Error == nil && row.Status == 2 && row.IndexStatus == "removed"
		}, "archive did not clean vector")
		if code, _ := request("DELETE", fmt.Sprintf("/memories/%d", memory.ID), "", false); code != 200 {
			t.Fatal("delete failed", code)
		}
		code, raw = request("GET", "/memories", "", false)
		if code != 200 || bytes.Contains(raw, []byte("用户偏好 Go")) {
			t.Fatal("deleted memory still listed")
		}
	}
}
