//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

func TestMCPHTTPManagementAndLiveRoleRevocation(t *testing.T) {
	db := testpg.Open(t)
	admin := model.User{Username: "mcp-http-admin", Email: "mcp-admin@test.invalid", PasswordHash: "test", Role: "admin"}
	browser := os.Getenv("NX_MCP_BROWSER") == "1"
	if browser {
		hash, err := bcrypt.GenerateFromPassword([]byte("mcp-test-only-2026"), bcrypt.DefaultCost)
		if err != nil {
			t.Fatal(err)
		}
		admin.PasswordHash = string(hash)
	}
	if err := db.Create(&admin).Error; err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int32
	remote := sdk.NewServer(&sdk.Implementation{Name: "http-acceptance", Version: "1"}, nil)
	remote.AddTool(&sdk.Tool{Name: "inspect", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		effects.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "healthy"}}}, nil
	})
	peer := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return remote }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	t.Cleanup(peer.Close)
	cfg := &config.Config{}
	cfg.MCP.Enabled = true
	cfg.Outbound.PrivateOrigins = []string{peer.URL}
	enc, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("mcp-http-test-only-jwt-secret", time.Hour, time.Hour)
	reg := tool.NewRegistry()
	repos := repo.New(db)
	var rdb *redis.Client
	if browser {
		if os.Getenv("NX_TEST_REDIS_ADDR") == "" {
			t.Fatal("NX_TEST_REDIS_ADDR required for browser login")
		}
		rdb = redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 13})
		t.Cleanup(func() {
			_ = rdb.Del(context.Background(), fmt.Sprintf("nx:auth:refresh:%d", admin.ID)).Err()
			_ = rdb.Close()
		})
	}
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repos, Encryptor: enc, JWT: jwt, RDB: rdb, Registry: reg, Executor: tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{})})
	if err != nil {
		t.Fatal(err)
	}
	address := "127.0.0.1:0"
	if browser {
		address = "127.0.0.1:18089"
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter))
	Register(h.Group("/", Recovery()), New(svcs), jwt)
	finish := make(chan struct{}, 1)
	if browser {
		h.POST("/__test/finish", func(_ context.Context, c *app.RequestContext) {
			select {
			case finish <- struct{}{}:
			default:
			}
			c.SetStatusCode(204)
		})
	}
	done := make(chan error, 1)
	go func() { done <- h.Run() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("listener did not drain")
		}
	})
	for deadline := time.Now().Add(2 * time.Second); !h.IsRunning(); {
		if time.Now().After(deadline) {
			t.Fatal("listener startup")
		}
		time.Sleep(time.Millisecond)
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)
	token, err := jwt.Generate(admin.ID, "admin", admin.Username)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path string, body any) (int, []byte) {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, err := http.NewRequest(method, "http://"+ln.Addr().String()+"/api/v1/admin/mcp"+path, strings.NewReader(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, data
	}
	code, raw := request("POST", "/servers", map[string]any{"name": "http-peer", "transport": "streamable_http", "url": peer.URL, "headers": map[string]string{"Authorization": "Bearer public-http-fixture-secret"}})
	var created struct {
		Data model.MCPServer `json:"data"`
	}
	if code != 200 || json.Unmarshal(raw, &created) != nil {
		t.Fatalf("create %d %s", code, raw)
	}
	if strings.Contains(string(raw), "public-http-fixture-secret") {
		t.Fatal("credential leaked")
	}
	path := fmt.Sprintf("/servers/%d", created.Data.ID)
	if code, raw = request("POST", path+"/credentials/migrate", map[string]any{}); code != 200 {
		t.Fatalf("credential migration endpoint %d %s", code, raw)
	}
	code, raw = request("POST", path+"/sync", map[string]any{})
	var synced struct {
		Data []model.Tool `json:"data"`
	}
	if code != 200 || json.Unmarshal(raw, &synced) != nil || len(synced.Data) != 1 {
		t.Fatalf("sync %d %s", code, raw)
	}
	if code, _ = request("POST", path+"/call", map[string]any{"name": "inspect", "args": map[string]any{}}); code != 403 || effects.Load() != 0 {
		t.Fatal("debug bypass", code, effects.Load())
	}
	code, raw = request("GET", path+"/tools?page=1&page_size=1", nil)
	var listed struct {
		Data struct {
			List  []model.Tool `json:"list"`
			Total int64        `json:"total"`
		} `json:"data"`
	}
	if code != 200 || json.Unmarshal(raw, &listed) != nil || listed.Data.Total != 1 || len(listed.Data.List) != 1 {
		t.Fatalf("tool policy list %d %s", code, raw)
	}
	item := listed.Data.List[0]
	policyPath := fmt.Sprintf("%s/tools/%d/policy", path, item.ID)
	policy := map[string]any{"binding_version": item.BindingVersion, "replay_policy": "readonly", "is_dangerous": false, "evidence": "read-only peer source reviewed"}
	if code, raw = request("PUT", policyPath, policy); code != 200 {
		t.Fatalf("review %d %s", code, raw)
	}
	if code, _ = request("PUT", policyPath, policy); code != 409 {
		t.Fatal("stale policy CAS", code)
	}
	if code, raw = request("POST", path+"/call", map[string]any{"name": "inspect", "args": map[string]any{}}); code != 200 || !strings.Contains(string(raw), "healthy") || effects.Load() != 1 {
		t.Fatalf("reviewed debug %d %s effects=%d", code, raw, effects.Load())
	}
	if browser {
		remote.AddTool(&sdk.Tool{Name: "inspect_new", Description: "Read deployment diagnostics; review required", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
			effects.Add(1)
			return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "new tool verified healthy"}}}, nil
		})
		t.Log("MCP browser fixture ready: http://127.0.0.1:18089; public fixture login mcp-http-admin / mcp-test-only-2026")
		select {
		case <-finish:
		case <-time.After(10 * time.Minute):
			t.Fatal("browser walkthrough timed out")
		}
		if effects.Load() < 2 {
			t.Fatal("browser did not invoke reviewed tool")
		}
	}
	effectsBeforeRevocation := effects.Load()
	if err = db.Model(&admin).Update("role", "user").Error; err != nil {
		t.Fatal(err)
	}
	if code, _ = request("GET", "/servers", nil); code != 403 {
		t.Fatal("stale admin JWT retained authority", code)
	}
	if code, _ = request("POST", path+"/call", map[string]any{"name": "inspect", "args": map[string]any{}}); code != 403 || effects.Load() != effectsBeforeRevocation {
		t.Fatal("revoked admin invoked remote tool", code, effects.Load())
	}
}
