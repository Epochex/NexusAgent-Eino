//go:build integration && infrastructure && realmodel

package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	dataworker "github.com/chengpeng-cp/nexus-agent/internal/worker"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redis/go-redis/v9"
	"gorm.io/datatypes"
)

// Explicit opt-in only: ordinary CI cannot spend model credits. The real key is
// read from a private file and exists only in the loopback budget relay memory.
func TestRealModelCriticalEndToEnd(t *testing.T) {
	if os.Getenv("NX_REAL_SPEND") != "ALLOW" {
		t.Skip("explicit NX_REAL_SPEND=ALLOW required")
	}
	key, err := os.ReadFile(os.Getenv("NX_REAL_KEY_FILE"))
	if err != nil {
		t.Fatal("private key file unavailable")
	}
	out := os.Getenv("NX_REAL_OUTPUT")
	if out == "" {
		t.Fatal("absolute NX_REAL_OUTPUT required")
	}
	budget := newRealBudgetRelay("https://api.deepseek.com", strings.TrimSpace(string(key)), filepath.Join(out, "private", "ledger.json"))
	checks := map[string]any{}
	t.Cleanup(func() {
		raw, _ := json.MarshalIndent(map[string]any{"checks": checks, "requests": budget.snapshot(), "failed": t.Failed(), "embedding": "controlled 32D fixture; no real embedding quality claim", "max_requests": 24, "max_output_tokens": 1024, "peak_price_budget_cny": .8}, "", "  ")
		if err := os.WriteFile(filepath.Join(out, "report.json"), raw, 0600); err != nil {
			t.Error("report persistence failed")
		}
	})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/embeddings" {
			var req struct {
				Input []string `json:"input"`
			}
			if json.NewDecoder(r.Body).Decode(&req) != nil {
				http.Error(w, "input", 400)
				return
			}
			rows := make([]any, len(req.Input))
			for i := range rows {
				v := make([]float32, 32)
				v[0] = 1
				rows[i] = map[string]any{"index": i, "embedding": v}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
			return
		}
		budget.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	db := testpg.Open(t)
	repos := repo.New(db)
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("NX_TEST_REDIS_ADDR"), DB: 11})
	t.Cleanup(func() { _ = rdb.Close() })
	if err = rdb.Ping(ctx).Err(); err != nil {
		t.Fatal("Redis unavailable")
	}
	enc, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("live-acceptance-public-test-jwt-secret", time.Hour, time.Hour)
	files := t.TempDir()
	reg := tool.NewRegistry()
	tool.RegisterFiles(reg, tool.FileOpsConfig{BaseDir: files, MaxSize: 4096})
	marker := "LIVE_" + rand.Text()[:10]
	var effects atomic.Int32
	remote := sdk.NewServer(&sdk.Implementation{Name: "live-readonly", Version: "1"}, nil)
	remote.AddTool(&sdk.Tool{Name: "inspect", Description: "Read the current deployment acceptance marker. No arguments.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		effects.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: marker}}}, nil
	})
	mcp := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return remote }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	t.Cleanup(mcp.Close)
	cfg := &config.Config{LLM: config.LLM{DefaultChat: "live-chat", DefaultEmbedding: "live-embed", MaxConnsPerProvider: 1, CircuitErrorRate: 1}, Agent: config.Agent{RunTimeout: 90 * time.Second, StepTimeout: 60 * time.Second, MaxIterations: 4}, Milvus: config.Milvus{Dim: 32}, RAG: config.RAG{ChunkSize: 256, ChunkOverlap: 16, BatchEmbed: 1, DenseTopK: 4, SparseTopK: 4, FusedTopN: 4, FinalTopK: 2, RRFK: 60}, Worker: config.Worker{Concurrency: 1, MaxRetry: 1}}
	cfg.MCP.Enabled = true
	cfg.Outbound.PrivateOrigins = []string{mcp.URL, proxy.URL}
	cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	vectors := vector.NewMilvusStore(cli)
	svcs, err := service.New(service.Deps{Root: ctx, Cfg: cfg, Repos: repos, RDB: rdb, JWT: jwt, Encryptor: enc, Registry: reg, Vector: vectors, Gateway: llm.NewGateway(&cfg.LLM), Executor: tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		svcs.Chat.Close(closeCtx)
	}()
	providerKey, err := enc.Encrypt("loopback-budget-relay-only")
	if err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "live-validation", Name: "Budget guarded real DeepSeek", BaseURL: proxy.URL, APIKey: providerKey, Status: 1}
	if err = db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	chatModel := &model.ModelConfig{ProviderID: provider.ID, Alias: "live-chat", ModelName: "deepseek-flash", Type: model.ModelTypeChat, ContextWindow: 32768, MaxOutput: 1024, Status: 1}
	embedModel := &model.ModelConfig{ProviderID: provider.ID, Alias: "live-embed", ModelName: "controlled-32d", Type: model.ModelTypeEmbed, EmbeddingDimension: 32, Status: 1}
	for _, m := range []*model.ModelConfig{chatModel, embedModel} {
		if err = db.Create(m).Error; err != nil {
			t.Fatal(err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter), server.WithWriteTimeout(0))
	Register(h.Group("/", Recovery()), New(svcs), jwt)
	apiDone := make(chan error, 1)
	go func() { apiDone <- h.Run() }()
	client := &http.Client{Timeout: 100 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(func() {
		cancel()
		client.CloseIdleConnections()
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = h.Shutdown(c)
		select {
		case <-apiDone:
		case <-c.Done():
			t.Error("API shutdown deadline")
		}
	})
	for !h.IsRunning() {
		select {
		case <-ctx.Done():
			t.Fatal("API startup")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	base := "http://" + ln.Addr().String() + "/api/v1"
	token := ""
	call := func(method, path string, body any, target any, want int) []byte {
		t.Helper()
		raw, _ := json.Marshal(body)
		request, _ := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		res, e := client.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(res.Body, 2<<20))
		if res.StatusCode != want {
			t.Fatalf("HTTP %s %s status%d: %.1500s", method, path, res.StatusCode, data)
		}
		if target != nil {
			var env struct {
				Data json.RawMessage `json:"data"`
			}
			if json.Unmarshal(data, &env) != nil || json.Unmarshal(env.Data, target) != nil {
				t.Fatal("invalid response envelope")
			}
		}
		return data
	}
	var owner model.User
	call("POST", "/auth/register", map[string]any{"username": "real-acceptance", "email": "real@test.invalid", "password": "real-public-fixture-2026"}, &owner, 200)
	var auth struct {
		AccessToken string `json:"access_token"`
	}
	call("POST", "/auth/login", map[string]any{"username": owner.Username, "password": "real-public-fixture-2026"}, &auth, 200)
	token = auth.AccessToken
	if token == "" {
		t.Fatal("missing JWT")
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), fmt.Sprintf("nx:auth:refresh:%d", owner.ID)).Err() })
	var serverConfig model.MCPServer
	call("POST", "/admin/mcp/servers", map[string]any{"name": "real-marker", "transport": "streamable_http", "url": mcp.URL}, &serverConfig, 200)
	var tools []model.Tool
	mcpPath := fmt.Sprintf("/admin/mcp/servers/%d", serverConfig.ID)
	call("POST", mcpPath+"/sync", map[string]any{}, &tools, 200)
	if len(tools) != 1 {
		t.Fatal("MCP discovery")
	}
	call("PUT", fmt.Sprintf("%s/tools/%d/policy", mcpPath, tools[0].ID), map[string]any{"binding_version": tools[0].BindingVersion, "replay_policy": "readonly", "is_dangerous": false, "evidence": "fixture SDK tool reviewed; reads ephemeral marker only"}, nil, 200)
	fileTool := &model.Tool{Code: "file_ops", Name: "Acceptance file save", Type: "builtin", Handler: "file_ops", Description: "Save a release acceptance marker within the owner's directory", Parameters: datatypes.JSON(`{"type":"object","properties":{"operation":{"type":"string","enum":["read","save"]},"path":{"type":"string"},"content":{"type":"string"}},"required":["operation","path"]}`), IsDangerous: true, ReplayPolicy: "manual", Status: 1}
	if err = db.Create(fileTool).Error; err != nil {
		t.Fatal(err)
	}
	newAgent := func(name, prompt string, kind string, binding, kbs datatypes.JSON) *model.Agent {
		a := &model.Agent{Name: name, Description: name, Type: kind, SystemPrompt: prompt, ModelConfigID: chatModel.ID, MaxTokens: 1024, MaxIterations: 4, Tools: binding, KBIDs: kbs, Status: 1}
		var got model.Agent
		call("POST", "/agents", a, &got, 200)
		return &got
	}
	bindings, _ := json.Marshal([]map[string]any{{"tool_id": tools[0].ID}})
	_ = newAgent("Deployment marker expert", "Use the inspect tool exactly once. Return the marker verbatim in one short sentence. Never invent the marker.", "expert", bindings, datatypes.JSON(`[]`))
	host := newAgent("Deployment coordinator", "Delegate inspection to the deployment marker expert.", "host", datatypes.JSON(`[]`), datatypes.JSON(`[]`))
	chat := func(name string, ag int64, question string) string {
		t.Helper()
		started := time.Now()
		raw, _ := json.Marshal(map[string]any{"agent_id": ag, "content": question})
		request, _ := http.NewRequestWithContext(ctx, "POST", base+"/chat/stream", bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		res, e := client.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("chat HTTP%d", res.StatusCode)
		}
		scanner := bufio.NewScanner(res.Body)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		event := ""
		answer := ""
		done := false
		first := 0.0
		events := map[string]int{}
		var refs any
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var data map[string]any
			if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &data) != nil {
				continue
			}
			events[event]++
			if event == "error" {
				checks[name] = data
				t.Fatalf("real chat failed: %v", data)
			}
			if event == "delta" {
				text, _ := data["content"].(string)
				answer += text
				if text != "" && first == 0 {
					first = float64(time.Since(started)) / float64(time.Millisecond)
				}
			}
			if event == "refs" {
				refs = data
			}
			if event == "done" {
				done = true
			}
		}
		checks[name] = map[string]any{"answer": answer, "first_content_ms": first, "total_ms": float64(time.Since(started)) / float64(time.Millisecond), "events": events, "references": refs, "done": done}
		if scanner.Err() != nil || !done {
			t.Fatal("missing client terminal")
		}
		var rows int64
		if err := db.Model(&model.Message{}).Where("role='assistant' AND content=? AND status=1", answer).Count(&rows).Error; err != nil || rows != 1 {
			t.Fatal("answer not committed exactly once")
		}
		return answer
	}
	answer := chat("host_expert_mcp", host.ID, "Inspect the current deployment acceptance marker using the expert and its tool. Return the exact marker.")
	if !strings.Contains(answer, marker) || effects.Load() != 1 {
		t.Fatalf("real tool evidence absent; invocations=%d answer=%q", effects.Load(), answer)
	}
	checks["mcp_effects"] = effects.Load()
	// Durable model -> automatic manual-tool approval -> real owner file write
	// -> model. Recreate the worker between waiting and decision to load PG state.
	store := execution.NewStore(db)
	startWorker := func() context.CancelFunc {
		wc, stop := context.WithCancel(ctx)
		w, e := execution.NewWorker(store, svcs.Workflow.Driver(), execution.WorkerConfig{Concurrency: 1, PollInterval: 20 * time.Millisecond})
		if e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { done <- w.Run(wc) }()
		return func() {
			stop()
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(6 * time.Second):
				t.Error("worker drain")
			}
		}
	}
	stopWorker := startWorker()
	defer func() {
		if stopWorker != nil {
			stopWorker()
		}
	}()
	var flow model.Workflow
	call("POST", "/workflows", map[string]any{"name": "Real release approval", "dsl": "name: real-release\ninput: [marker]\nnodes:\n  - {key: plan, type: llm, prompt: 'Reply READY only. Release {{marker}}.'}\n  - key: save\n    type: tool\n    tool_code: file_ops\n    args: {operation: save, path: 'releases/{{marker}}.txt', content: '{{marker}}'}\n  - {key: answer, type: llm, prompt: 'The file was saved: {{save}}. Reply ACTIVATE_OK {{marker}} only.'}\nedges: [plan, save, answer]"}, &flow, 200)
	waitRun := func(id int64, accept func(string) bool) map[string]json.RawMessage {
		t.Helper()
		deadline := time.Now().Add(100 * time.Second)
		for time.Now().Before(deadline) {
			var value map[string]json.RawMessage
			call("GET", fmt.Sprintf("/workflows/runs/%d", id), nil, &value, 200)
			var status string
			_ = json.Unmarshal(value["status"], &status)
			if accept(status) {
				return value
			}
			if status == "failed" || status == "needs_attention" {
				t.Fatalf("workflow failed: %.1600s", mustRealJSON(value))
			}
			time.Sleep(40 * time.Millisecond)
		}
		t.Fatal("workflow wait exceeded")
		return nil
	}
	for _, approved := range []bool{true, false} {
		value := "release_" + rand.Text()[:10]
		input := map[string]any{"workflow_id": flow.ID, "request_key": value, "input": map[string]any{"marker": value}}
		var run, duplicate model.WorkflowRun
		call("POST", "/workflows/run", input, &run, 202)
		call("POST", "/workflows/run", input, &duplicate, 202)
		if duplicate.ID != run.ID {
			t.Fatal("idempotent submit duplicated")
		}
		waiting := waitRun(run.ID, func(s string) bool { return s == "waiting_approval" })
		path := filepath.Join(files, strconv.FormatInt(owner.ID, 10), "releases", value+".txt")
		if _, e := os.Stat(path); !os.IsNotExist(e) {
			t.Fatal("effect before approval")
		}
		if approved {
			stopWorker()
			stopWorker = nil
		}
		var approvals []struct {
			InterruptID string `json:"interrupt_id"`
			ArgsHash    string `json:"args_hash"`
		}
		_ = json.Unmarshal(waiting["approvals"], &approvals)
		if len(approvals) != 1 {
			t.Fatalf("missing approval: %.1200s", mustRealJSON(waiting))
		}
		before := len(budget.snapshot())
		call("POST", fmt.Sprintf("/workflows/runs/%d/approve", run.ID), map[string]any{"interrupt_id": approvals[0].InterruptID, "args_hash": approvals[0].ArgsHash, "approved": approved, "comment": "real model sandbox acceptance"}, nil, 200)
		if approved {
			stopWorker = startWorker()
		}
		terminal := waitRun(run.ID, func(s string) bool { return s == "succeeded" || s == "canceled" || s == "failed" })
		var status string
		_ = json.Unmarshal(terminal["status"], &status)
		if approved {
			data, e := os.ReadFile(path)
			if e != nil || string(data) != value || status != "succeeded" {
				t.Fatal("approved effect/result missing")
			}
			if len(budget.snapshot()) != before+1 {
				t.Fatal("checkpoint reran earlier paid model node")
			}
		} else {
			if _, e := os.Stat(path); !os.IsNotExist(e) || status == "succeeded" {
				t.Fatal("rejected effect ran")
			}
			if len(budget.snapshot()) != before {
				t.Fatal("rejected branch used model budget")
			}
		}
		var operations []execution.Operation
		call("GET", fmt.Sprintf("/workflows/runs/%d/operations", run.ID), nil, &operations, 200)
		checks[fmt.Sprintf("approval_%t", approved)] = map[string]any{"run_id": run.ID, "status": status, "operations": operations, "effect_verified": true}
	}
	// Real source upload/ETL/PG/MinIO/Milvus, controlled embeddings, real chat.
	rag := engine.New(svcs.Gateway, vectors, repos.KB, &cfg.RAG, 32)
	rag.SetEmbeddingResolver(svcs.ResolveKBEmbedding)
	svcs.SetRAGEngine(rag)
	var schema string
	db.Raw("SELECT current_schema()").Scan(&schema)
	bucket := strings.ReplaceAll(schema, "_", "-")
	mc, err := infra.NewMinIO(ctx, &config.MinIO{Endpoint: os.Getenv("NX_TEST_MINIO_ADDR"), AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	svcs.KB.SetMinIO(mc, bucket)
	dc, stopDocs := context.WithCancel(ctx)
	docDone := make(chan error, 1)
	dw := dataworker.New(nil, repos, rag, mc, bucket, &cfg.Worker)
	go func() { docDone <- dw.Run(dc) }()
	t.Cleanup(func() {
		stopDocs()
		select {
		case <-docDone:
		case <-time.After(8 * time.Second):
			t.Error("document worker drain")
		}
	})
	var kb model.KnowledgeBase
	call("POST", "/kbs", map[string]any{"name": "LANTERN handbook", "embedding_config_id": embedModel.ID, "chunk_size": 256, "chunk_overlap": 16}, &kb, 200)
	var upload bytes.Buffer
	mw := multipart.NewWriter(&upload)
	part, _ := mw.CreateFormFile("file", "lantern-handbook.txt")
	_, _ = io.WriteString(part, "LANTERN deployment handbook. The rollback window is 17 minutes. The required change approver is Iris. The annual GPU budget is not recorded in this handbook.")
	_ = mw.Close()
	req, _ := http.NewRequestWithContext(ctx, "POST", base+fmt.Sprintf("/kbs/%d/documents", kb.ID), &upload)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 202 {
		t.Fatalf("upload %d %.1000s", res.StatusCode, data)
	}
	var uploaded struct {
		Data model.Document `json:"data"`
	}
	if json.Unmarshal(data, &uploaded) != nil {
		t.Fatal("document envelope")
	}
	for deadline := time.Now().Add(30 * time.Second); ; {
		var doc model.Document
		if e := db.First(&doc, uploaded.Data.ID).Error; e != nil {
			t.Fatal(e)
		}
		if doc.Status == model.DocStatusReady {
			checks["document"] = map[string]any{"id": doc.ID, "chunks": doc.ChunkCount, "ready_version": doc.ReadyVersion}
			break
		}
		if doc.Status == model.DocStatusFailed || time.Now().After(deadline) {
			t.Fatalf("index state=%s error=%s", doc.Status, doc.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
	kbs, _ := json.Marshal([]int64{kb.ID})
	qa := newAgent("LANTERN evidence QA", "Answer only from the supplied knowledge context. Cite the source using [1]. If the requested fact is absent, answer NOT_PROVIDED and invent no numbers. Keep answers under 40 words.", "expert", datatypes.JSON(`[]`), kbs)
	if answer = chat("grounded_answer", qa.ID, "According to the LANTERN handbook, what is the rollback window? Cite the source."); !strings.Contains(answer, "17") {
		t.Fatal("grounded answer incorrect")
	}
	if answer = chat("unanswerable", qa.ID, "What is the annual GPU budget in dollars for LANTERN? Use only the supplied handbook."); !strings.Contains(answer, "NOT_PROVIDED") {
		t.Fatal("unanswerable fact was invented")
	}
	before := len(budget.snapshot())
	var foreign model.User
	call("POST", "/auth/register", map[string]any{"username": "real-foreign", "email": "foreign@test.invalid", "password": "real-public-fixture-2026"}, &foreign, 200)
	call("POST", "/auth/login", map[string]any{"username": foreign.Username, "password": "real-public-fixture-2026"}, &auth, 200)
	token = auth.AccessToken
	call("GET", fmt.Sprintf("/kbs/%d", kb.ID), nil, nil, 404)
	if len(budget.snapshot()) != before {
		t.Fatal("access denial consumed model budget")
	}
	checks["cross_owner_denied_before_model"] = true
	t.Logf("Real model critical cases complete; billable attempts=%d (cap24), report saved without credentials", len(budget.snapshot()))
}

func mustRealJSON(value any) []byte { raw, _ := json.Marshal(value); return raw }
