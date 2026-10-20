//go:build integration && infrastructure

package execution_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

func TestChatServerProcessShutdownDrainsActiveAndIdleStreams(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("actual SIGTERM and production container lifecycle are verified on Linux")
	}
	for _, key := range []string{"NX_TEST_DATABASE_DSN", "NX_TEST_REDIS_ADDR", "NX_TEST_MILVUS_ADDR", "NX_TEST_MINIO_ADDR"} {
		if os.Getenv(key) == "" {
			t.Skip(key + " required")
		}
	}
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := os.Getenv("NX_TEST_SHUTDOWN_BASELINE_BINARY")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "shutdown-server")
		build := exec.Command("go", "build", "-o", binary, "./cmd/server")
		build.Dir = backend
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("actual server build %v %s", err, out)
		}
	}
	db, svcs, owner, dir, dsn := processFixture(t, backend)
	flow := &model.Workflow{Name: "shutdown-idle", CreatorID: owner, DSL: "name: shutdown-idle\nnodes: [{key: review,type: human,prompt: review}]"}
	if err = svcs.Workflow.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	run, err := svcs.Workflow.Run(context.Background(), owner, &service.RunInput{WorkflowID: flow.ID})
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewStore(db)
	claimed, err := store.Claim(context.Background(), "prepare-idle", time.Minute)
	if err != nil || claimed == nil {
		t.Fatal("prepare waiting workflow", err)
	}
	if err = svcs.Workflow.Driver().Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	entered, modelStopped := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"provisional\"}}]}\n\n")
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
		close(modelStopped)
	}))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	provider := model.ModelProvider{Code: "shutdown-process", Name: "shutdown", BaseURL: peer.URL, Status: 1}
	if err = db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "shutdown-process-chat", ModelName: "controlled", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	_, port, _ := net.SplitHostPort(address)
	jwtSecret := "shutdown-process-public-test-only-jwt-secret"
	cmd := exec.Command(binary)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "NX_PG__DSN="+dsn, "NX_SECRET_KEY="+processTestKey, "NX_ENV=dev", "NX_HTTP__PORT="+port, "NX_HTTP__SHUTDOWN_TIMEOUT=5s", "NX_JWT__SECRET="+jwtSecret,
		"NX_REDIS__ADDR="+os.Getenv("NX_TEST_REDIS_ADDR"), "NX_MILVUS__ADDR="+os.Getenv("NX_TEST_MILVUS_ADDR"), "NX_MINIO__ENDPOINT="+os.Getenv("NX_TEST_MINIO_ADDR"), "NX_MINIO__ACCESS_KEY=minioadmin", "NX_MINIO__SECRET_KEY=minioadmin", "NX_MINIO__BUCKET=nexus-shutdown-process",
		"NX_A2A__ENABLED=false", "NX_SANDBOX__ENABLED=false", "NX_LLM__DEFAULT_CHAT=shutdown-process-chat", "NX_AGENT__RUN_TIMEOUT=1m", "NX_AGENT__STEP_TIMEOUT=1m", "NX_OBSERVABILITY__OTEL_ENABLED=false", "NX_OBSERVABILITY__DIAGNOSTICS_ADDR=")
	logs := &processLog{}
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	process := &ownedWorker{cmd: cmd, log: logs, done: make(chan error, 1)}
	go func() { process.done <- cmd.Wait() }()
	t.Cleanup(process.kill)
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	for until := time.Now().Add(25 * time.Second); ; {
		res, e := client.Get("http://" + address + "/healthz")
		if e == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(until) {
			t.Fatal("server not ready", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	pair, err := security.NewJWTManager(jwtSecret, time.Hour, time.Hour).Generate(owner, "user", "process-owner")
	if err != nil {
		t.Fatal(err)
	}
	open := func(method, path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, "http://"+address+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+pair.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		if res.StatusCode != 200 {
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()
			t.Fatalf("stream status=%d %s", res.StatusCode, raw)
		}
		return res
	}
	idle := open("GET", fmt.Sprintf("/api/v1/workflows/runs/%d/events", run.ID), "")
	idleDone := make(chan struct{})
	go func() { io.Copy(io.Discard, idle.Body); idle.Body.Close(); close(idleDone) }()
	chat := open("POST", "/api/v1/chat/stream", `{"content":"shutdown process active request"}`)
	chatDone := make(chan struct{})
	go func() { io.Copy(io.Discard, chat.Body); chat.Body.Close(); close(chatDone) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual model not reached", logs.String())
	}
	start := time.Now()
	process.once.Do(func() {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Error(err)
			_ = cmd.Process.Kill()
		}
		select {
		case err := <-process.done:
			if err != nil {
				t.Error("graceful server shutdown failed", err, logs.String())
			}
		case <-time.After(4 * time.Second):
			_ = cmd.Process.Kill()
			<-process.done
			t.Error("SIGTERM failed to cancel active Chat and idle workflow SSE before shutdown deadline")
		}
	})
	for _, done := range []<-chan struct{}{idleDone, chatDone, modelStopped} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("server exit left stream/model HTTP open")
		}
	}
	var rows []model.Message
	if err = db.Table("messages AS m").Select("m.*").Joins("JOIN sessions s ON s.id=m.session_id").Where("s.user_id=? AND m.role='assistant'", owner).Find(&rows).Error; err != nil || len(rows) != 1 || rows[0].Status != model.MsgStatusStopped {
		t.Error("shutdown lost truthful terminal commit", rows, err)
	}
	var active int64
	if err = db.Table("execution_active_capacity").Where("owner_id=?", owner).Count(&active).Error; err != nil || active != 0 {
		t.Error("shutdown retained active slots", active, err)
	}
	stored, err := store.Get(context.Background(), owner, run.ID)
	if err != nil || stored.Status != execution.Waiting || len(stored.Approvals) != 1 || stored.Approvals[0].Decision != "pending" {
		t.Error("ending idle SSE canceled or lost durable approval", stored, err)
	}
	if !t.Failed() {
		t.Logf("actual SIGTERM drained Chat/model/idleSSE, persisted stopped answer and retained approval: elapsed=%s", time.Since(start))
	}
}
