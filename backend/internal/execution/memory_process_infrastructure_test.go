//go:build integration && infrastructure

package execution_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryWorkerProcessRecoversAfterRemoteUpsert(t *testing.T) {
	if os.Getenv("NX_TEST_MILVUS_ADDR") == "" {
		t.Skip("NX_TEST_MILVUS_ADDR required")
	}
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "memory-worker")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = backend
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("worker build: %v %s", err, output)
	}
	db, svcs, owner, dir, dsn := processFixture(t, backend)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	var facts, embeddings atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		if r.URL.Path == "/chat/completions" {
			facts.Add(1)
			fmt.Fprint(w, `{"choices":[{"message":{"content":"[\"user prefers Go\"]"},"finish_reason":"stop"}]}`)
			return
		}
		if r.URL.Path == "/embeddings" {
			embeddings.Add(1)
			fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1,0,0]}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	client, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	vectors := vector.NewMilvusStore(client)
	svcs.Vector = vectors
	svcs.Cfg.LLM.DefaultEmbedding = "memory-embed"
	svcs.Cfg.LLM.DefaultChat = "memory-chat"
	svcs.Cfg.Milvus.Dim = 3
	svcs.Chat = service.NewChatService(svcs)
	svcs.Memory = service.NewMemoryService(svcs)
	provider := &model.ModelProvider{Code: "memory-process", Name: "memory-process", BaseURL: peer.URL, Status: 1}
	if err = db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	for _, configured := range []*model.ModelConfig{{ProviderID: provider.ID, Alias: "memory-embed", ModelName: "embed", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}, {ProviderID: provider.ID, Alias: "memory-chat", ModelName: "chat", Type: model.ModelTypeChat, ContextWindow: 32768, MaxOutput: 2048, Status: 1}} {
		if err = db.Create(configured).Error; err != nil {
			t.Fatal(err)
		}
	}
	session := &model.Session{UserID: owner}
	if err = db.Create(session).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&model.Message{SessionID: session.ID, Role: "user", Content: "Please remember I prefer Go."}).Error; err != nil {
		t.Fatal(err)
	}
	queued, _, err := svcs.ConfigureMemory().EnqueueSession(ctx, owner, session.ID, true, 6, 3)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := svcs.EnsureMemoryProfile(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	collection := long_term.MemoryCollection(profile.EmbeddingVersion)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := vectors.DropCollection(cleanup, collection); err != nil {
			t.Error(err)
		}
	})
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	key := time.Now().UnixNano() % 1000000000
	if _, err = blocker.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
	trigger := fmt.Sprintf(`CREATE FUNCTION hold_memory_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='vector' AND NEW.status='done' THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$; CREATE TRIGGER hold_memory_completion BEFORE UPDATE ON memory_jobs FOR EACH ROW EXECUTE FUNCTION hold_memory_completion()`, key)
	if err = db.Exec(trigger).Error; err != nil {
		t.Fatal(err)
	}
	start := func() *ownedWorker {
		child := exec.Command(binary, "-queues", "memory", "-memory-concurrency", "1")
		child.Dir = dir
		child.Env = append(os.Environ(), "NX_PG__DSN="+dsn, "NX_SECRET_KEY="+processTestKey, "NX_ENV=dev", "NX_MILVUS__ADDR="+os.Getenv("NX_TEST_MILVUS_ADDR"), "NX_REDIS__ADDR=127.0.0.1:1", "NX_OBSERVABILITY__OTEL_ENABLED=false")
		logs := &processLog{}
		child.Stdout = logs
		child.Stderr = logs
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		worker := &ownedWorker{cmd: child, done: make(chan error, 1), log: logs}
		go func() { worker.done <- child.Wait() }()
		t.Cleanup(worker.kill)
		return worker
	}
	first := start()
	parsed, _ := url.Parse(dsn)
	appName := parsed.Query().Get("application_name")
	wait := func(check func() bool, description string, worker *ownedWorker) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline) && ctx.Err() == nil; {
			if check() {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		jobs, _, _ := long_term.NewStore(db).ListJobs(context.Background(), owner, 0, 20)
		encoded, _ := json.Marshal(jobs)
		t.Fatalf("%s; facts=%d embeds=%d jobs=%s; worker %s", description, facts.Load(), embeddings.Load(), encoded, worker.log.String())
	}
	wait(func() bool {
		var count int64
		return db.Raw("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=? AND wait_event='advisory'", appName).Scan(&count).Error == nil && count > 0
	}, "worker never reached post-upsert commit", first)
	count, err := vectors.Count(ctx, collection)
	if err != nil || count != 1 {
		t.Fatalf("no actual remote effect: %d %v", count, err)
	}
	first.kill()
	if _, err = blocker.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
		t.Fatal(err)
	}
	replacement := start()
	wait(func() bool {
		var count int64
		return db.Model(&model.Memory{}).Where("user_id=? AND index_status='ready' AND indexed_revision=revision", owner).Count(&count).Error == nil && count == 1
	}, "replacement failed to publish memory", replacement)
	var job long_term.Job
	if err = db.Where("kind='vector'").First(&job).Error; err != nil {
		t.Fatal(err)
	}
	count, err = vectors.Count(ctx, collection)
	if err != nil || count != 1 || job.Attempts != 2 || facts.Load() != 1 || embeddings.Load() != 2 {
		t.Fatalf("recovery duplicated/re-extracted: count=%d job=%+v facts=%d embeds=%d err=%v", count, job, facts.Load(), embeddings.Load(), err)
	}
	summary, err := long_term.NewStore(db).JobStatus(ctx, owner, queued.ID)
	if err != nil || summary.VectorDone != 1 || !strings.EqualFold(summary.Status, "done") {
		t.Fatal("incorrect completion", summary, err)
	}
}
