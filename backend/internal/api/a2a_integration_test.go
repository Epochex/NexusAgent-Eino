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
	"golang.org/x/crypto/bcrypt"
)

func TestA2AHTTPManagementThroughActualLocalEino(t *testing.T) {
	db := testpg.Open(t)
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	users := []model.User{{Username: "a2a-api-owner", Email: "a2a-api-owner@test.invalid", PasswordHash: "test", Role: "admin"}, {Username: "a2a-api-foreign", Email: "a2a-api-foreign@test.invalid", PasswordHash: "test", Role: "admin"}}
	browser := os.Getenv("NX_A2A_BROWSER") == "1"
	if browser {
		hash, err := bcrypt.GenerateFromPassword([]byte("a2a-test-only-2026"), bcrypt.DefaultCost)
		if err != nil {
			t.Fatal(err)
		}
		users[0].PasswordHash = string(hash)
	}
	for i := range users {
		if err := db.Create(&users[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	var modelCalls atomic.Int32
	var brokenModel, dropResponse atomic.Bool
	modelPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelCalls.Add(1)
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if browser && strings.Contains(string(body), "等待取消") {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if brokenModel.Load() || browser && strings.Contains(string(body), "注入失败") {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: {broken}\n\n")
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"实际本地 Eino 回答\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer modelPeer.Close()
	cfg := &config.Config{Agent: config.Agent{RunTimeout: 15 * time.Second, StepTimeout: 5 * time.Second, MaxIterations: 4}, LLM: config.LLM{DefaultChat: "a2a-chat", MaxConnsPerProvider: 4, CircuitErrorRate: 1}}
	cfg.A2A.Enabled = true
	cfg.A2A.Name = "本地 Agent 验收"
	enc, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("a2a-api-test-only-jwt-secret", time.Hour, time.Hour)
	repos := repo.New(db)
	registry := tool.NewRegistry()
	var rdb *redis.Client
	if browser {
		if os.Getenv("NX_TEST_REDIS_ADDR") == "" {
			t.Fatal("NX_TEST_REDIS_ADDR required for browser login")
		}
		rdb = redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 13})
		t.Cleanup(func() {
			_ = rdb.Del(context.Background(), fmt.Sprintf("nx:auth:refresh:%d", users[0].ID)).Err()
			_ = rdb.Close()
		})
	}
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repos, JWT: jwt, Encryptor: enc, RDB: rdb, Gateway: llm.NewGateway(&cfg.LLM), Registry: registry, Executor: tool.NewExecutor(repos.Tool, registry, tool.ExecutorConfig{})})
	if err != nil {
		t.Fatal(err)
	}
	provider := model.ModelProvider{Code: "a2a-model-peer", Name: "A2A controlled model", BaseURL: modelPeer.URL, Status: 1}
	if err = db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	configuredModel := model.ModelConfig{ProviderID: provider.ID, Alias: "a2a-chat", ModelName: "controlled-chat", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}
	if err = db.Create(&configuredModel).Error; err != nil {
		t.Fatal(err)
	}
	remote := httptest.NewUnstartedServer(nil)
	remoteBase := "http://" + remote.Listener.Addr().String()
	adapter, err := svcs.A2A.Inbound(root, remoteBase)
	if err != nil {
		t.Fatal(err)
	}
	remoteMux := http.NewServeMux()
	adapter.RegisterRoutes(remoteMux)
	remote.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if dropResponse.Load() && r.Method == http.MethodPost {
			captured := httptest.NewRecorder()
			remoteMux.ServeHTTP(captured, r)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		remoteMux.ServeHTTP(w, r)
	})
	remote.Start()
	defer remote.Close()
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = adapter.Close(ctx)
	}()
	cfg.Outbound.PrivateOrigins = []string{remoteBase}
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
	stopped := make(chan error, 1)
	go func() { stopped <- h.Run() }()
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = h.Shutdown(ctx)
		<-stopped
	}()
	for deadline := time.Now().Add(2 * time.Second); !h.IsRunning(); {
		if time.Now().After(deadline) {
			t.Fatal("API listener startup")
		}
		time.Sleep(time.Millisecond)
	}
	tokens := make([]string, 2)
	for i, user := range users {
		pair, err := jwt.Generate(user.ID, user.Role, user.Username)
		if err != nil {
			t.Fatal(err)
		}
		tokens[i] = pair.AccessToken
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{}}
	defer client.CloseIdleConnections()
	request := func(method, path string, body any, foreign bool) (int, []byte) {
		t.Helper()
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, "http://"+ln.Addr().String()+"/api/v1/admin/a2a"+path, strings.NewReader(string(raw)))
		token := tokens[0]
		if foreign {
			token = tokens[1]
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}
	code, raw := request("POST", "/agents", map[string]any{"base_url": remoteBase, "headers": map[string]string{"Authorization": "Bearer " + tokens[0]}}, false)
	var registered struct {
		Data model.A2AAgent `json:"data"`
	}
	if code != 200 || json.Unmarshal(raw, &registered) != nil {
		t.Fatalf("register %d %s", code, raw)
	}
	if strings.Contains(string(raw), tokens[0]) {
		t.Fatal("A2A register exposed credentials")
	}
	if code, _ := request("POST", "/agents/999999/health", nil, false); code != 404 {
		t.Fatal("missing registry record reported upstream outage", code)
	}
	agentPath := fmt.Sprintf("/agents/%d", registered.Data.ID)
	code, raw = request("PUT", agentPath, map[string]any{"config_version": registered.Data.ConfigVersion, "name": "已编辑的本地 Agent"}, false)
	if code != 200 {
		t.Fatalf("A2A edit %d %s", code, raw)
	}
	body := map[string]any{"text": "请说明状态", "request_key": "http-eino-stable-request"}
	code, raw = request("POST", agentPath+"/tasks", body, false)
	var delegated struct {
		Data service.SendTaskResult `json:"data"`
	}
	if code != 200 || json.Unmarshal(raw, &delegated) != nil || delegated.Data.State != "completed" || delegated.Data.Text != "实际本地 Eino 回答" {
		t.Fatalf("actual local Eino delegation %d %s", code, raw)
	}
	task := delegated.Data
	if code, raw = request("POST", agentPath+"/tasks", body, false); code != 200 || modelCalls.Load() != 1 {
		t.Fatalf("HTTP repeat dispatched model again %d %s calls=%d", code, raw, modelCalls.Load())
	}
	if code, _ = request("GET", "/tasks/"+task.ID, nil, true); code != 404 {
		t.Fatal("other admin read private delegation", code)
	}
	if code, _ = request("POST", "/tasks/"+task.ID+"/cancel", nil, true); code != 404 {
		t.Fatal("other admin canceled private delegation", code)
	}
	if code, raw = request("GET", "/tasks/"+task.ID+"?refresh=true", nil, false); code != 200 || !strings.Contains(string(raw), "completed") {
		t.Fatalf("owned query %d %s", code, raw)
	}
	if code, raw = request("GET", agentPath+"/tasks?page=1&page_size=20", nil, true); code != 200 || strings.Contains(string(raw), task.ID) {
		t.Fatal("foreign history exposed tasks", code)
	}
	var wrongOwner int64
	if err = db.Model(&model.Session{}).Where("user_id<>?", users[0].ID).Count(&wrongOwner).Error; err != nil || wrongOwner != 0 {
		t.Fatal("A2A persisted chat under wrong/system owner", wrongOwner, err)
	}
	var count int64
	if err = db.Model(&model.Session{}).Where("user_id=?", users[0].ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("actual local chat was not persisted exactly once", count, err)
	}
	brokenModel.Store(true)
	code, raw = request("POST", agentPath+"/tasks", map[string]any{"text": "trigger model failure", "request_key": "http-model-failure"}, false)
	if code != 200 || json.Unmarshal(raw, &delegated) != nil || delegated.Data.State != "failed" || delegated.Data.Text == "partial" {
		t.Fatalf("partial Eino output reported completion %d %s", code, raw)
	}
	brokenModel.Store(false)
	dropResponse.Store(true)
	lostBody := map[string]any{"text": "lose completed response", "request_key": "http-lost-response"}
	code, raw = request("POST", agentPath+"/tasks", lostBody, false)
	if code != 202 || json.Unmarshal(raw, &delegated) != nil || delegated.Data.State != "unknown" || delegated.Data.Error == "" {
		t.Fatalf("lost response did not become explicit unknown %d %s", code, raw)
	}
	callsBeforeRetry := modelCalls.Load()
	code, raw = request("POST", agentPath+"/tasks", lostBody, false)
	if code != 202 || json.Unmarshal(raw, &delegated) != nil || delegated.Data.State != "unknown" || modelCalls.Load() != callsBeforeRetry {
		t.Fatal("same-key unknown delegation resent actual Eino execution", code, modelCalls.Load(), callsBeforeRetry)
	}
	if browser {
		dropResponse.Store(false)
		t.Log("A2A browser fixture ready: http://127.0.0.1:18089; public fixture login a2a-api-owner / a2a-test-only-2026; wait=等待取消; fail=注入失败")
		select {
		case <-finish:
		case <-time.After(10 * time.Minute):
			t.Fatal("browser walkthrough timed out")
		}
	}
}
