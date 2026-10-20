//go:build integration

package execution_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const processTestKey = "01234567890123456789012345678901"

type processLog struct {
	mu   sync.Mutex
	text string
}

func (l *processLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.text += string(p)
	if len(l.text) > 8192 {
		l.text = l.text[len(l.text)-8192:]
	}
	return len(p), nil
}
func (l *processLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.text }

type ownedWorker struct {
	cmd  *exec.Cmd
	done chan error
	log  *processLog
	once sync.Once
}

func (w *ownedWorker) kill() { w.once.Do(func() { _ = w.cmd.Process.Kill(); <-w.done }) }

func startProcess(t *testing.T, binary, dir, dsn string) *ownedWorker {
	t.Helper()
	child := exec.Command(binary, "-queues", "execution", "-execution-concurrency", "1")
	child.Dir = dir
	// These values configure an isolated test schema and test-only encryption.
	child.Env = append(os.Environ(), "NX_PG__DSN="+dsn, "NX_SECRET_KEY="+processTestKey, "NX_ENV=dev", "NX_SANDBOX__ENABLED=false", "NX_OBSERVABILITY__OTEL_ENABLED=false")
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
func processFixture(t *testing.T, backend string) (*gorm.DB, *service.Services, int64, string, string) {
	t.Helper()
	db := testpg.Open(t)
	repos := repo.New(db)
	owner := &model.User{Username: "process-owner", Email: "process@test.invalid", PasswordHash: "test-only"}
	if err := db.Create(owner).Error; err != nil {
		t.Fatal(err)
	}
	encryptor, err := security.NewEncryptor(processTestKey)
	if err != nil {
		t.Fatal(err)
	}
	reg := tool.NewRegistry()
	tool.RegisterFiles(reg, tool.FileOpsConfig{BaseDir: "./data/files", MaxSize: 1 << 20})
	executor := tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 30 * time.Second})
	cfg := &config.Config{LLM: config.LLM{MaxConnsPerProvider: 4, DefaultChat: "fault-model"}}
	svcs := &service.Services{Deps: service.Deps{Cfg: cfg, Repos: repos, Encryptor: encryptor, Gateway: llm.NewGateway(&cfg.LLM), Executor: executor, Registry: reg}}
	svcs.Workflow = service.NewWorkflowService(svcs)
	var schema string
	if err = db.Raw(`SELECT current_schema()`).Scan(&schema).Error; err != nil {
		t.Fatal(err)
	}
	dsn, err := url.Parse(os.Getenv("NX_TEST_DATABASE_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	query := dsn.Query()
	query.Set("search_path", schema)
	query.Set("application_name", "fault_"+schema)
	dsn.RawQuery = query.Encode()
	dir := t.TempDir()
	if err = os.Mkdir(filepath.Join(dir, "configs"), 0700); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(backend, "configs", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "configs", "config.yaml"), content, 0600); err != nil {
		t.Fatal(err)
	}
	return db, svcs, owner.ID, dir, dsn.String()
}
func awaitProcessState(t *testing.T, store *execution.Store, owner, id int64, want string, worker *ownedWorker) *execution.Run {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Get(context.Background(), owner, id)
		if err == nil && run.Status == want {
			return run
		}
		time.Sleep(25 * time.Millisecond)
	}
	run, err := store.Get(context.Background(), owner, id)
	t.Fatalf("want %s got %+v %v; worker: %s", want, run, err, worker.log.String())
	return nil
}

// Executes the actual cmd/worker binary, with production service wiring, frozen
// model/tool configuration and normal 15s leases. Redis/Milvus/MinIO are absent.
func TestWorkerProcessFaultRecovery(t *testing.T) {
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "worker")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = backend
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("worker build: %v %s", err, output)
	}
	t.Run("safe_model_call", func(t *testing.T) {
		db, svcs, owner, dir, dsn := processFixture(t, backend)
		var calls atomic.Int32
		started := make(chan struct{})
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				close(started)
				<-r.Context().Done()
				return
			}
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"recovered response"},"finish_reason":"stop"}]}`)
		}))
		defer func() { peer.CloseClientConnections(); peer.Close() }()
		cipher, err := svcs.Encryptor.Encrypt("test-only-model-key")
		if err != nil {
			t.Fatal(err)
		}
		provider := &model.ModelProvider{Code: "fault-peer", Name: "fault peer", BaseURL: peer.URL, APIKey: cipher, Status: 1}
		if err = db.Create(provider).Error; err != nil {
			t.Fatal(err)
		}
		if err = db.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "fault-model", ModelName: "fault-peer", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
			t.Fatal(err)
		}
		flow := &model.Workflow{Name: "safe", CreatorID: owner, DSL: "name: safe\nnodes: [{key: answer,type: llm,prompt: answer}]"}
		if err = svcs.Workflow.Create(context.Background(), flow); err != nil {
			t.Fatal(err)
		}
		run, err := svcs.Workflow.Run(context.Background(), owner, &service.RunInput{WorkflowID: flow.ID})
		if err != nil {
			t.Fatal(err)
		}
		first := startProcess(t, binary, dir, dsn)
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatalf("model never called: %s", first.log.String())
		}
		first.kill()
		replacement := startProcess(t, binary, dir, dsn)
		got := awaitProcessState(t, execution.NewStore(db), owner, run.ID, execution.Succeeded, replacement)
		if calls.Load() != 2 || !strings.Contains(string(got.Output), "recovered response") {
			t.Fatalf("recovery duplicated/lost result: calls=%d output=%s", calls.Load(), got.Output)
		}
		ops, err := execution.NewStore(db).Operations(context.Background(), owner, run.ID)
		if err != nil || len(ops) != 1 || ops[0].Attempt != 2 {
			t.Fatalf("logical operation not reused %+v %v", ops, err)
		}
	})
	t.Run("write_returned_before_commit", func(t *testing.T) {
		db, svcs, owner, dir, dsn := processFixture(t, backend)
		params := datatypes.JSON(`{"type":"object","properties":{"operation":{"type":"string"},"path":{"type":"string"},"content":{"type":"string"}},"required":["operation","path","content"],"additionalProperties":false}`)
		if err = db.Create(&model.Tool{Code: "file_ops", Name: "file_ops", Type: model.ToolTypeBuiltin, Parameters: params, IsDangerous: true, ReplayPolicy: "manual", Status: 1}).Error; err != nil {
			t.Fatal(err)
		}
		flow := &model.Workflow{Name: "write", CreatorID: owner, DSL: "name: write\nnodes: [{key: write,type: tool,tool_code: file_ops,args: {operation: save,path: effect.txt,content: durable-effect}}]"}
		if err = svcs.Workflow.Create(context.Background(), flow); err != nil {
			t.Fatal(err)
		}
		run, err := svcs.Workflow.Run(context.Background(), owner, &service.RunInput{WorkflowID: flow.ID})
		if err != nil {
			t.Fatal(err)
		}
		// A schema-local trigger holds the DB commit after the real filesystem write
		// returns, without adding a fault hook to production code.
		key := time.Now().UnixNano() % 1000000000
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		blocker, err := sqlDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Close()
		if _, err = blocker.ExecContext(context.Background(), `SELECT pg_advisory_lock($1)`, key); err != nil {
			t.Fatal(err)
		}
		defer blocker.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
		trigger := fmt.Sprintf(`CREATE FUNCTION hold_completed_operation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='succeeded' THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$; CREATE TRIGGER hold_completed_operation BEFORE UPDATE ON execution_operations FOR EACH ROW EXECUTE FUNCTION hold_completed_operation()`, key)
		if err = db.Exec(trigger).Error; err != nil {
			t.Fatal(err)
		}
		store := execution.NewStore(db)
		first := startProcess(t, binary, dir, dsn)
		waiting := awaitProcessState(t, store, owner, run.ID, execution.Waiting, first)
		if len(waiting.Approvals) != 1 {
			t.Fatal("missing sensitive tool approval")
		}
		approval := waiting.Approvals[0]
		if err = store.Decide(context.Background(), owner, run.ID, approval.InterruptID, approval.ArgsHash, true); err != nil {
			t.Fatal(err)
		}
		parsed, _ := url.Parse(dsn)
		appName := parsed.Query().Get("application_name")
		deadline := time.Now().Add(10 * time.Second)
		blocked := false
		for time.Now().Before(deadline) {
			var count int64
			db.Raw(`SELECT COUNT(*) FROM pg_stat_activity WHERE application_name=? AND wait_event='advisory'`, appName).Scan(&count)
			if count > 0 {
				blocked = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !blocked {
			t.Fatalf("worker did not reach commit boundary: %s", first.log.String())
		}
		effect := filepath.Join(dir, "data", "files", strconv.FormatInt(owner, 10), "effect.txt")
		before, err := os.Stat(effect)
		if err != nil {
			t.Fatal("external effect missing: ", err)
		}
		data, err := os.ReadFile(effect)
		if err != nil || string(data) != "durable-effect" {
			t.Fatalf("write not real: %q %v", data, err)
		}
		first.kill()
		if _, err = blocker.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key); err != nil {
			t.Fatal(err)
		}
		replacement := startProcess(t, binary, dir, dsn)
		awaitProcessState(t, store, owner, run.ID, execution.NeedsAttention, replacement)
		after, err := os.Stat(effect)
		if err != nil || !after.ModTime().Equal(before.ModTime()) {
			t.Fatal("unknown write was repeated")
		}
		ops, err := store.Operations(context.Background(), owner, run.ID)
		if err != nil || len(ops) != 1 || ops[0].Status != "unknown" || ops[0].Attempt != 1 {
			t.Fatalf("unknown operation incorrectly replayed: %+v %v", ops, err)
		}
	})
}
