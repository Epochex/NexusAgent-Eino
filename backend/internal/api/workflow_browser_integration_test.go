//go:build integration

package api

import (
	"context"
	"net"
	"os"
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
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/redis/go-redis/v9"
)

// Opt-in manual browser fixture, never a substitute for automated assertions.
// Uses production handlers/JWT/services/worker and real PG/Redis, no mocked API.
func TestWorkflowBrowserFixture(t *testing.T) {
	if os.Getenv("NX_BROWSER_WALKTHROUGH") != "1" {
		t.Skip("opt-in interactive browser fixture")
	}
	db := testpg.Open(t)
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR")})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal("explicit NX_TEST_REDIS_ADDR required: ", err)
	}
	encryptor, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("browser-fixture-test-only-jwt-secret", time.Hour, time.Hour)
	svcs, err := service.New(service.Deps{Cfg: &config.Config{}, Repos: repo.New(db), Encryptor: encryptor, JWT: jwt, RDB: rdb, Gateway: llm.NewGateway(&config.LLM{})})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := svcs.Auth.Register(ctx, &service.RegisterInput{Username: "browser-review", Email: "browser@test.invalid", Password: "browser-test-only-2026"})
	if err != nil {
		t.Fatal(err)
	}
	flow := &model.Workflow{Name: "浏览器双重审批验收", CreatorID: owner.ID, DSL: "name: 浏览器双重审批验收\ninput: [ticket]\nnodes:\n  - {key: first, type: human, prompt: '初审 {{ticket}}'}\n  - {key: second, type: human, prompt: '复审 {{ticket}}'}"}
	if err = svcs.Workflow.Create(ctx, flow); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:18089")
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
	worker, err := execution.NewWorker(execution.NewStore(db), svcs.Workflow.Driver(), execution.WorkerConfig{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(ctx) }()
	listenerDone := make(chan error, 1)
	go func() { listenerDone <- h.Run() }()
	t.Log("Browser fixture ready at 127.0.0.1:18089; frontend should proxy there. Only workflow/auth features are provisioned.")
	select {
	case <-finish:
	case <-ctx.Done():
		t.Error("browser walkthrough not finished within 10 minutes")
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	_ = h.Shutdown(shutdown)
	select {
	case err = <-workerDone:
		if err != nil {
			t.Error(err)
		}
	case <-shutdown.Done():
		t.Error("worker did not drain")
	}
	select {
	case <-listenerDone:
	case <-shutdown.Done():
		t.Error("listener did not stop")
	}
}
