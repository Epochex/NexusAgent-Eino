//go:build integration

package execution_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"gopkg.in/yaml.v3"
)

func TestMCPWorkerProcessDynamicToolsAndUnknownWrite(t *testing.T) {
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "mcp-worker")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = backend
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("worker build %v %s", err, out)
	}
	db, base, owner, dir, dsn := processFixture(t, backend)
	if err = db.Model(&model.User{}).Where("id=?", owner).Update("role", "admin").Error; err != nil {
		t.Fatal(err)
	}
	var reads, writes atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	remote := sdk.NewServer(&sdk.Implementation{Name: "worker-peer", Version: "1"}, nil)
	remote.AddTool(&sdk.Tool{Name: "inspect", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		reads.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "worker-read"}}}, nil
	})
	remote.AddTool(&sdk.Tool{Name: "write", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, _ *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		writes.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
		case <-release:
		}
		return nil, fmt.Errorf("response lost after remote effect")
	})
	peer := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return remote }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	t.Cleanup(func() { close(release); peer.CloseClientConnections(); peer.Close() })
	base.Cfg.MCP.Enabled = true
	base.Cfg.Outbound.PrivateOrigins = []string{peer.URL}
	svcs, err := service.New(base.Deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := security.WithUserID(context.Background(), owner)
	// Start with no MCP rows: the live worker must accept tools discovered later.
	configPath := filepath.Join(dir, "configs", "config.yaml")
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var settings map[string]any
	if err = yaml.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	settings["outbound"] = map[string]any{"private_origins": []string{peer.URL}}
	raw, err = yaml.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	worker := startProcess(t, binary, dir, dsn)
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(worker.log.String(), "worker started"); {
		if time.Now().After(deadline) {
			t.Fatal("worker startup", worker.log.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	srv := &model.MCPServer{Name: "worker-peer", Transport: "streamable_http", URL: peer.URL}
	if err = svcs.MCP.Create(ctx, srv); err != nil {
		t.Fatal(err)
	}
	rows, err := svcs.MCP.SyncTools(ctx, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	var read, write model.Tool
	for _, item := range rows {
		if item.MCPName == "inspect" {
			read = item
		} else {
			write = item
		}
	}
	if _, err = svcs.MCP.UpdateToolPolicy(ctx, srv.ID, read.ID, &service.MCPToolPolicyInput{BindingVersion: read.BindingVersion, ReplayPolicy: "readonly", Evidence: "reviewed read-only diagnostic handler"}); err != nil {
		t.Fatal(err)
	}
	submit := func(item model.Tool) *model.WorkflowRun {
		t.Helper()
		flow := &model.Workflow{Name: item.MCPName, CreatorID: owner, DSL: fmt.Sprintf("name: mcp\nnodes: [{key: remote,type: tool,tool_code: %s,args: {}}]", item.Code)}
		if err := svcs.Workflow.Create(ctx, flow); err != nil {
			t.Fatal(err)
		}
		run, err := svcs.Workflow.Run(ctx, owner, &service.RunInput{WorkflowID: flow.ID})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	store := execution.NewStore(db)
	first := submit(read)
	awaitProcessState(t, store, owner, first.ID, execution.Succeeded, worker)
	if reads.Load() != 1 {
		t.Fatal("live new tool did not execute once", reads.Load())
	}
	worker.kill()
	replacement := startProcess(t, binary, dir, dsn)
	second := submit(read)
	got := awaitProcessState(t, store, owner, second.ID, execution.Succeeded, replacement)
	if reads.Load() != 2 || !strings.Contains(string(got.Output), "worker-read") {
		t.Fatal("registry restart lost actual remote output", reads.Load(), string(got.Output))
	}
	pending := submit(write)
	waiting := awaitProcessState(t, store, owner, pending.ID, execution.Waiting, replacement)
	if writes.Load() != 0 || len(waiting.Approvals) != 1 {
		t.Fatal("write executed without approval", writes.Load())
	}
	approval := waiting.Approvals[0]
	if err = store.Decide(ctx, owner, pending.ID, approval.InterruptID, approval.ArgsHash, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("approved write did not reach remote", replacement.log.String())
	}
	replacement.kill()
	recovery := startProcess(t, binary, dir, dsn)
	awaitProcessState(t, store, owner, pending.ID, execution.NeedsAttention, recovery)
	operations, err := store.Operations(ctx, owner, pending.ID)
	if err != nil || len(operations) != 1 || operations[0].Attempt != 1 || operations[0].Status != "unknown" || writes.Load() != 1 {
		t.Fatal("unknown MCP write was replayed", operations, err, writes.Load())
	}
}
