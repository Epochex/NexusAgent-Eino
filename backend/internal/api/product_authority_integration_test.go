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
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/redis/go-redis/v9"
	"gorm.io/datatypes"
)

func TestProductToolsAndFreshJWTAuthority(t *testing.T) {
	if os.Getenv("NX_TEST_REDIS_ADDR") == "" {
		t.Skip("NX_TEST_REDIS_ADDR required for actual login/refresh")
	}
	db := testpg.Open(t)
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 12})
	t.Cleanup(func() { _ = rdb.Close() })
	jwt := security.NewJWTManager("product-public-test-only-jwt-secret", time.Hour, time.Hour)
	enc, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	browser := os.Getenv("NX_PRODUCT_BROWSER") == "1"
	var modelCalls, toolCalls atomic.Int32
	var leakedForeign atomic.Bool
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		modelCalls.Add(1)
		if strings.Contains(string(raw), "FOREIGN-EXPERT-PRIVATE") {
			leakedForeign.Store(true)
		}
		var request struct {
			Stream   bool          `json:"stream"`
			Messages []llm.Message `json:"messages"`
		}
		if json.Unmarshal(raw, &request) != nil {
			http.Error(w, "bad test request", 400)
			return
		}
		if !request.Stream {
			fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"1"},"finish_reason":"stop"}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if len(request.Messages) > 0 && request.Messages[len(request.Messages)-1].Role == llm.RoleTool {
			if request.Messages[len(request.Messages)-1].Content != "verified" {
				t.Error("model did not receive actual tool output")
			}
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Host 工具结果已核验\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"product-tool-call\",\"type\":\"function\",\"function\":{\"name\":\"product_visible\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(peer.Close)
	cfg := &config.Config{LLM: config.LLM{DefaultChat: "product-chat", MaxConnsPerProvider: 4}, Agent: config.Agent{RunTimeout: 15 * time.Second, StepTimeout: 5 * time.Second, MaxIterations: 4}}
	cfg.MCP.Enabled = true
	reg := tool.NewRegistry()
	reg.Register("product_visible", func(context.Context, map[string]any, string) (string, error) {
		toolCalls.Add(1)
		return "verified", nil
	})
	reg.Register("product_disabled", func(context.Context, map[string]any, string) (string, error) { return "must not run", nil })
	repos := repo.New(db)
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repos, RDB: rdb, JWT: jwt, Encryptor: enc, Registry: reg, Gateway: llm.NewGateway(&cfg.LLM), Executor: tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{})})
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
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("product listener did not drain")
		}
	})
	for deadline := time.Now().Add(2 * time.Second); !h.IsRunning(); {
		if time.Now().After(deadline) {
			t.Fatal("product API startup")
		}
		time.Sleep(time.Millisecond)
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)
	request := func(method, path string, body any, token string) (int, []byte) {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, "http://"+ln.Addr().String()+"/api/v1"+path, strings.NewReader(string(raw)))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
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
	users := make([]model.User, 2)
	pairs := make([]security.TokenPair, 2)
	for i := range users {
		username := fmt.Sprintf("product-user-%d", i)
		code, raw := request("POST", "/auth/register", map[string]string{"username": username, "email": username + "@test.invalid", "password": "product-public-test-2026"}, "")
		var registered struct{ Data model.User }
		if code != 200 || json.Unmarshal(raw, &registered) != nil {
			t.Fatalf("actual registration %d %s", code, raw)
		}
		users[i] = registered.Data
		code, raw = request("POST", "/auth/login", map[string]string{"username": username, "password": "product-public-test-2026"}, "")
		var loggedIn struct{ Data security.TokenPair }
		if code != 200 || json.Unmarshal(raw, &loggedIn) != nil || loggedIn.Data.AccessToken == "" {
			t.Fatalf("actual login %d", code)
		}
		pairs[i] = loggedIn.Data
		t.Cleanup(func() { _ = rdb.Del(context.Background(), fmt.Sprintf("nx:auth:refresh:%d", registered.Data.ID)).Err() })
	}
	if users[0].Role != "admin" || users[1].Role != "user" {
		t.Fatal("bootstrap roles", users[0].Role, users[1].Role)
	}
	on := true
	cipher, err := enc.Encrypt("{}")
	if err != nil {
		t.Fatal(err)
	}
	mcp := model.MCPServer{Name: "product-private", Transport: "streamable_http", URL: "https://private.example.invalid/mcp", Enabled: &on, EncryptedCredentials: cipher, AllowedUserIDs: datatypes.JSON(`[]`)}
	if err = db.Create(&mcp).Error; err != nil {
		t.Fatal(err)
	}
	private := model.Tool{Code: fmt.Sprintf("mcp_%d_private", mcp.ID), Name: "private", Type: model.ToolTypeMCP, MCPName: "private", MCPServerID: &mcp.ID, MCPConfigVersion: mcp.ConfigVersion, IsDangerous: true, ReplayPolicy: "manual", Status: 1, Parameters: datatypes.JSON(`{"type":"object"}`)}
	ghost := model.Tool{Code: "product_ghost", Name: "unavailable", Type: model.ToolTypeBuiltin, ReplayPolicy: "readonly", Status: 1, Parameters: datatypes.JSON(`{"type":"object"}`)}
	disabled := model.Tool{Code: "product_disabled", Name: "disabled", Type: model.ToolTypeBuiltin, ReplayPolicy: "readonly", Status: 0, Parameters: datatypes.JSON(`{"type":"object"}`)}
	visible := model.Tool{Code: "product_visible", Name: "visible", Type: model.ToolTypeBuiltin, ReplayPolicy: "readonly", Status: 1, Parameters: datatypes.JSON(`{"type":"object"}`), AuthConfig: datatypes.JSON(`{"key":"public-fixture-must-not-leak"}`)}
	for _, item := range []*model.Tool{&private, &ghost, &disabled, &visible} {
		if err = db.Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}
	code, raw := request("GET", "/tools?page=1&page_size=1", nil, pairs[1].AccessToken)
	var page struct {
		Data struct {
			List  []model.Tool
			Total int64
		}
	}
	if code != 200 || json.Unmarshal(raw, &page) != nil || page.Data.Total != 1 || len(page.Data.List) != 1 || page.Data.List[0].ID != visible.ID {
		t.Errorf("tool visibility must precede pagination/count: status=%d body=%s", code, raw)
	}
	grants, _ := json.Marshal([]int64{users[1].ID})
	if err = db.Model(&mcp).Update("allowed_user_ids", datatypes.JSON(grants)).Error; err != nil {
		t.Fatal(err)
	}
	code, raw = request("GET", "/tools?type=mcp&page=1&page_size=1", nil, pairs[1].AccessToken)
	if code != 200 || json.Unmarshal(raw, &page) != nil || page.Data.Total != 1 || len(page.Data.List) != 1 || page.Data.List[0].ID != private.ID {
		t.Fatal("new live grant absent from filtered page", code, string(raw))
	}
	if err = db.Model(&mcp).Update("allowed_user_ids", datatypes.JSON(`[]`)).Error; err != nil {
		t.Fatal(err)
	}
	code, raw = request("GET", "/tools?type=mcp&page=1&page_size=1", nil, pairs[1].AccessToken)
	if code != 200 || json.Unmarshal(raw, &page) != nil || page.Data.Total != 0 || len(page.Data.List) != 0 {
		t.Fatal("revoked tool retained in picker", code, string(raw))
	}
	code, raw = request("GET", "/tools?page=1&page_size=100", nil, pairs[0].AccessToken)
	if code != 200 || strings.Contains(string(raw), "public-fixture-must-not-leak") {
		t.Error("tool picker exposed private execution config")
	}
	ctx := security.WithUserID(context.Background(), users[1].ID)
	if _, err = svcs.Executor.ListContext(ctx, fmt.Sprintf(`[{"tool_id":%d}]`, ghost.ID)); err == nil {
		t.Error("model received a definition without a registered handler")
	}
	provider := model.ModelProvider{Code: "product-fixture", Name: "product", BaseURL: peer.URL, Status: 1}
	if err = db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	chat := model.ModelConfig{ProviderID: provider.ID, Alias: "product-chat", ModelName: "product", Type: model.ModelTypeChat, Status: 1, ContextWindow: 8192, MaxOutput: 1024}
	if err = db.Create(&chat).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{private.ID, ghost.ID, disabled.ID, 999999} {
		code, _ = request("POST", "/agents", map[string]any{"name": "invalid binding", "model_config_id": chat.ID, "tools": []map[string]int64{{"tool_id": id}}}, pairs[1].AccessToken)
		if code < 400 {
			t.Errorf("Agent accepted unauthorized/unavailable tool ID %d status=%d", id, code)
		}
	}
	createAgent := func(name, kind string, owner int, tools []model.AgentToolBinding) model.Agent {
		t.Helper()
		code, raw := request("POST", "/agents", map[string]any{"name": name, "type": kind, "model_config_id": chat.ID, "description": name, "system_prompt": name, "tools": tools, "memory_enabled": false}, pairs[owner].AccessToken)
		var result struct{ Data model.Agent }
		if code != 200 || json.Unmarshal(raw, &result) != nil {
			t.Fatalf("create %s %d %s", name, code, raw)
		}
		return result.Data
	}
	foreignAgent := createAgent("FOREIGN-EXPERT-PRIVATE", "expert", 0, []model.AgentToolBinding{})
	_ = createAgent("product-local-expert", "expert", 1, []model.AgentToolBinding{{ToolID: visible.ID}})
	host := createAgent("product-host", "host", 1, []model.AgentToolBinding{})
	code, raw = request("POST", "/chat/stream", map[string]any{"agent_id": host.ID, "content": "执行一次工具并核对结果"}, pairs[1].AccessToken)
	if code != 200 || !strings.Contains(string(raw), "Host 工具结果已核验") || !strings.Contains(string(raw), `"delegate"`) || !strings.Contains(string(raw), `"merge"`) || !strings.Contains(string(raw), "event: done") || modelCalls.Load() != 3 || toolCalls.Load() != 1 || leakedForeign.Load() {
		t.Fatalf("actual Host/Eino/tool HTTP chain %d model=%d tool=%d foreign=%v body=%s", code, modelCalls.Load(), toolCalls.Load(), leakedForeign.Load(), raw)
	}
	if code, _ = request("POST", "/chat/stream", map[string]any{"agent_id": foreignAgent.ID, "content": "read foreign Agent"}, pairs[1].AccessToken); code != 404 {
		t.Error("foreign Agent execution", code)
	}
	if code, _ = request("GET", fmt.Sprintf("/agents/%d", host.ID), nil, pairs[0].AccessToken); code != 404 {
		t.Error("other actor read Host config", code)
	}
	var completed int64
	if err = db.Model(&model.Message{}).Where("role='assistant' AND status=? AND content=?", model.MsgStatusDone, "Host 工具结果已核验").Count(&completed).Error; err != nil || completed != 1 {
		t.Fatal("Host result persistence", completed, err)
	}
	if err = db.Model(&model.User{}).Where("id=?", users[0].ID).Update("role", "user").Error; err != nil {
		t.Fatal(err)
	}
	if code, _ = request("GET", "/admin/models/providers", nil, pairs[0].AccessToken); code != 403 {
		t.Errorf("old admin JWT retained generic model management: %d", code)
	}
	code, raw = request("POST", "/auth/refresh", map[string]string{"refresh_token": pairs[0].RefreshToken}, "")
	var refreshed struct{ Data security.TokenPair }
	if code != 200 || json.Unmarshal(raw, &refreshed) != nil {
		t.Fatalf("active user's refresh %d", code)
	}
	claims, err := jwt.Parse(refreshed.Data.AccessToken, false)
	if err != nil || claims.Role != "user" {
		t.Error("refresh copied obsolete admin role", err)
	}
	if err = db.Model(&model.User{}).Where("id=?", users[0].ID).Update("status", 2).Error; err != nil {
		t.Fatal(err)
	}
	if code, _ = request("GET", "/agents", nil, pairs[0].AccessToken); code != 401 {
		t.Errorf("disabled user retained generic access: %d", code)
	}
	if code, _ = request("POST", "/auth/refresh", map[string]string{"refresh_token": refreshed.Data.RefreshToken}, ""); code != 401 {
		t.Errorf("disabled user refreshed identity: %d", code)
	}
	if browser {
		for i := range 45 {
			code := fmt.Sprintf("product_page_%02d", i)
			reg.Register(code, func(context.Context, map[string]any, string) (string, error) { return "page tool", nil })
			item := model.Tool{Code: code, Name: fmt.Sprintf("分页工具 %02d", i), Type: model.ToolTypeBuiltin, Status: 1, ReplayPolicy: "readonly", Parameters: datatypes.JSON(`{"type":"object"}`)}
			if err = db.Create(&item).Error; err != nil {
				t.Fatal(err)
			}
		}
		t.Log("Product browser ready at 127.0.0.1:18089; public login product-user-1 / product-public-test-2026; 46 visible tools")
		select {
		case <-finish:
		case <-time.After(10 * time.Minute):
			t.Fatal("product browser timed out")
		}
	}
}
