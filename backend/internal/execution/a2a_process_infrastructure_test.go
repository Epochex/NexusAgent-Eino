//go:build integration && infrastructure

package execution_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

func TestA2AServerProcessRestartDoesNotFabricateCompletion(t *testing.T) {
	for _, key := range []string{"NX_TEST_DATABASE_DSN", "NX_TEST_REDIS_ADDR", "NX_TEST_MILVUS_ADDR", "NX_TEST_MINIO_ADDR"} {
		if os.Getenv(key) == "" {
			t.Skip(key + " required for actual server process")
		}
	}
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "a2a-server")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/server")
	build.Dir = backend
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("actual server build %v %s", err, output)
	}
	db, _, owner, dir, dsn := processFixture(t, backend)
	var calls atomic.Int32
	var block atomic.Bool
	entered := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if block.Load() {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
			w.(http.Flusher).Flush()
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"persisted before restart\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	provider := model.ModelProvider{Code: "a2a-process-peer", Name: "controlled model peer", BaseURL: peer.URL, Status: 1}
	if err = db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "a2a-process-chat", ModelName: "controlled-chat", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	address := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		value := listener.Addr().String()
		listener.Close()
		return value
	}
	controlAddress, agentAddress := address(), address()
	_, controlPort, _ := net.SplitHostPort(controlAddress)
	agentBase := "http://" + agentAddress
	jwtSecret := "a2a-process-public-test-only-jwt-secret"
	start := func() *ownedWorker {
		cmd := exec.Command(binary)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "NX_PG__DSN="+dsn, "NX_SECRET_KEY="+processTestKey, "NX_ENV=dev",
			"NX_REDIS__ADDR="+os.Getenv("NX_TEST_REDIS_ADDR"), "NX_MILVUS__ADDR="+os.Getenv("NX_TEST_MILVUS_ADDR"),
			"NX_MINIO__ENDPOINT="+os.Getenv("NX_TEST_MINIO_ADDR"), "NX_MINIO__ACCESS_KEY=minioadmin", "NX_MINIO__SECRET_KEY=minioadmin", "NX_MINIO__BUCKET=nexus-a2a-process",
			"NX_HTTP__PORT="+controlPort, "NX_JWT__SECRET="+jwtSecret, "NX_A2A__ENABLED=true", "NX_A2A__BIND_ADDR="+agentAddress, "NX_A2A__PUBLIC_URL="+agentBase,
			"NX_LLM__DEFAULT_CHAT=a2a-process-chat", "NX_AGENT__RUN_TIMEOUT=3s", "NX_AGENT__STEP_TIMEOUT=2s", "NX_SANDBOX__ENABLED=false", "NX_OBSERVABILITY__OTEL_ENABLED=false")
		logs := &processLog{}
		cmd.Stdout = logs
		cmd.Stderr = logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		process := &ownedWorker{cmd: cmd, log: logs, done: make(chan error, 1)}
		go func() { process.done <- cmd.Wait() }()
		t.Cleanup(process.kill)
		client := &http.Client{Timeout: 300 * time.Millisecond, Transport: &http.Transport{}}
		defer client.CloseIdleConnections()
		for deadline := time.Now().Add(25 * time.Second); time.Now().Before(deadline); {
			response, err := client.Get("http://" + controlAddress + "/healthz")
			if err == nil {
				response.Body.Close()
				if response.StatusCode == 200 {
					return process
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal("actual API/A2A server did not start", logs.String())
		return nil
	}
	token, err := security.NewJWTManager(jwtSecret, time.Hour, time.Hour).Generate(owner, "user", "process-owner")
	if err != nil {
		t.Fatal(err)
	}
	client := a2a.NewClient(a2a.WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{agentBase}}), a2a.WithCredentials(agentBase, map[string]string{"Authorization": "Bearer " + token.AccessToken}))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	first := start()
	card, err := client.FetchCard(ctx, agentBase)
	if err != nil {
		t.Fatal("actual process AgentCard", err, first.log.String())
	}
	completed, err := client.SendToCard(ctx, agentBase, card, "quick answer")
	if err != nil || completed.State != "completed" || a2a.ExtractText(completed) != "persisted before restart" {
		t.Fatal("actual process Eino answer", completed, err, first.log.String())
	}
	block.Store(true)
	pending, err := client.SendRequest(ctx, agentBase, card, "hold response", "a2a-process-interrupted-message", true)
	if err != nil || pending.ID == "" {
		t.Fatal("actual process pending task", pending, err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual model request not reached", first.log.String())
	}
	first.kill()
	replacement := start()
	stored, err := client.GetTask(ctx, agentBase, card, completed.ID)
	if err != nil || stored.State != "completed" || a2a.ExtractText(stored) != "persisted before restart" {
		t.Fatal("completed A2A task lost after process replacement", stored, err, replacement.log.String())
	}
	var final *a2a.Task
	for deadline := time.Now().Add(40 * time.Second); time.Now().Before(deadline); {
		final, err = client.GetTask(ctx, agentBase, card, pending.ID)
		if err == nil && final.State == "failed" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil || final == nil || final.State != "failed" || strings.Contains(a2a.ExtractText(final), "partial") || calls.Load() != 2 {
		t.Fatal("interrupted task fabricated completion or replayed model", final, err, calls.Load(), replacement.log.String())
	}
	var task struct {
		OwnerID int64
		State   string
	}
	if err = db.Table("a2a_tasks").Where("id=?", pending.ID).First(&task).Error; err != nil || task.OwnerID != owner || task.State != string(sdk.TaskStateFailed) {
		t.Fatal("orphan owner/state", task, err)
	}
	if runtime.GOOS != "windows" {
		replacement.once.Do(func() {
			if err := replacement.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Error(err)
				_ = replacement.cmd.Process.Kill()
			}
			select {
			case err := <-replacement.done:
				if err != nil {
					t.Error("graceful server shutdown", err, replacement.log.String())
				}
			case <-time.After(15 * time.Second):
				_ = replacement.cmd.Process.Kill()
				<-replacement.done
				t.Error("server did not drain")
			}
		})
	}
	t.Log("actual API+A2A process restart; orphan expired after configured run budget plus 30s grace; owner=" + strconv.FormatInt(owner, 10))
}
