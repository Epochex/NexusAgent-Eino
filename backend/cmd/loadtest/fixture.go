package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const fixturePassword = "capacity-public-test-password-2026"

var regexpSchema = regexp.MustCompile(`^nexus_load_[a-f0-9]{12}$`)

type loadActor struct {
	ID         int64  `json:"id"`
	Username   string `json:"username"`
	Token      string `json:"token"`
	AgentID    int64  `json:"agent_id"`
	FlowID     int64  `json:"flow_id"`
	WaitFlowID int64  `json:"wait_flow_id"`
}
type loadFixture struct {
	Schema     string      `json:"schema"`
	SuiteID    string      `json:"suite_id"`
	API        string      `json:"api"`
	PeerAPI    string      `json:"peer_api"`
	Actors     []loadActor `json:"actors"`
	PreparedAt time.Time   `json:"prepared_at"`
}
type apiClient struct {
	base string
	http *http.Client
}
type apiFailure struct {
	Status, Code int
	Path         string
}

func (e *apiFailure) Error() string {
	return fmt.Sprintf("API %s status=%d code=%d", e.Path, e.Status, e.Code)
}
func newAPIClient(base string) *apiClient {
	return &apiClient{base: strings.TrimSuffix(base, "/"), http: &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxIdleConns: 256, MaxIdleConnsPerHost: 128, MaxConnsPerHost: 256, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 15 * time.Second}}}
}
func (a *apiClient) call(ctx context.Context, method, path, token string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+"/api/v1"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 2<<20 {
		return fmt.Errorf("API response exceeded fixture bound")
	}
	var envelope struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return &apiFailure{Status: res.StatusCode, Path: path}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 || envelope.Code != 0 {
		return &apiFailure{Status: res.StatusCode, Code: envelope.Code, Path: path}
	}
	if out != nil {
		if err = json.Unmarshal(envelope.Data, out); err != nil {
			return fmt.Errorf("API %s returned invalid data", path)
		}
	}
	return nil
}
func saveFixture(path string, fixture *loadFixture) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0600) // test JWTs stay in this private ignored file, never in reports/logs
}
func readFixture(path string) (*loadFixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fixture loadFixture
	if json.Unmarshal(raw, &fixture) != nil || !strings.HasPrefix(fixture.Schema, "nexus_load_") || fixture.SuiteID == "" {
		return nil, fmt.Errorf("invalid isolated load fixture")
	}
	return &fixture, nil
}
func fixtureDB(ctx context.Context, schema string) (*gorm.DB, error) {
	parsed, err := url.Parse(os.Getenv("NX_TEST_DATABASE_DSN"))
	if err != nil || parsed.Host == "" || !strings.HasSuffix(parsed.Path, "_test") {
		return nil, fmt.Errorf("explicit NX_TEST_DATABASE_DSN URL ending in _test required")
	}
	if schema != "" {
		if !regexpSchema.MatchString(schema) {
			return nil, fmt.Errorf("invalid isolated schema")
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
	}
	db, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, fmt.Errorf("load fixture PostgreSQL unavailable")
	}
	pool, err := db.DB()
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(8)
	pool.SetMaxIdleConns(4)
	pool.SetConnMaxLifetime(5 * time.Minute)
	if err = pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("load fixture PostgreSQL unavailable")
	}
	return db, nil
}
func prepareFixture(ctx context.Context, path, migrations, api, peer string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("fixture path already exists; use a new test fixture path")
	}
	seed := make([]byte, 6)
	if _, err := rand.Read(seed); err != nil {
		return err
	}
	id := hex.EncodeToString(seed)
	fixture := &loadFixture{Schema: "nexus_load_" + id, SuiteID: id, API: api, PeerAPI: peer, PreparedAt: time.Now().UTC()}
	base, err := fixtureDB(ctx, "")
	if err != nil {
		return err
	}
	basePool, _ := base.DB()
	defer basePool.Close()
	if err = base.WithContext(ctx).Exec(`CREATE SCHEMA "` + fixture.Schema + `"`).Error; err != nil {
		return fmt.Errorf("could not create a fresh isolated schema")
	}
	db, err := fixtureDB(ctx, fixture.Schema)
	if err != nil {
		return err
	}
	pool, _ := db.DB()
	defer pool.Close()
	files, err := filepath.Glob(filepath.Join(migrations, "*.up.sql"))
	if err != nil || len(files) < 13 {
		return fmt.Errorf("current migration files unavailable")
	}
	sort.Strings(files)
	for _, file := range files {
		// Match controlled integration fixtures: product schema, no teaching
		// providers/tools pointing at unrelated services. Setup uses actual APIs.
		if strings.HasPrefix(filepath.Base(file), "002_") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if err = db.WithContext(ctx).Exec(string(raw)).Error; err != nil {
			return fmt.Errorf("fixture migration %s failed", filepath.Base(file))
		}
	}
	return saveFixture(path, fixture)
}

func bootstrapFixture(ctx context.Context, path string) error {
	f, err := readFixture(path)
	if err != nil {
		return err
	}
	api := newAPIClient(f.API)
	defer api.http.CloseIdleConnections()
	for i := len(f.Actors); i < 16; i++ {
		username := fmt.Sprintf("load-%s-%02d", f.SuiteID, i)
		var user struct {
			ID             int64
			Username, Role string
		}
		err = api.call(ctx, "POST", "/auth/register", "", map[string]any{"username": username, "email": username + "@test.invalid", "password": fixturePassword}, &user)
		if err != nil {
			if failure, ok := err.(*apiFailure); !ok || failure.Status != 409 {
				return err
			}
		}
		var pair struct {
			AccessToken string `json:"access_token"`
		}
		if err = api.call(ctx, "POST", "/auth/login", "", map[string]string{"username": username, "password": fixturePassword}, &pair); err != nil {
			return err
		}
		if err = api.call(ctx, "GET", "/me", pair.AccessToken, nil, &user); err != nil {
			return err
		}
		if user.ID < 1 || user.Username != username || i == 0 && user.Role != "admin" {
			return fmt.Errorf("fixture actor/schema identity mismatch")
		}
		f.Actors = append(f.Actors, loadActor{ID: user.ID, Username: username, Token: pair.AccessToken})
		if err = saveFixture(path, f); err != nil {
			return err
		}
	}
	admin := f.Actors[0].Token
	var providers []struct {
		ID   int64
		Code string
	}
	if err = api.call(ctx, "GET", "/admin/models/providers", admin, nil, &providers); err != nil {
		return err
	}
	providerID := int64(0)
	for _, p := range providers {
		if p.Code == "load-peer" {
			providerID = p.ID
		}
	}
	if providerID == 0 {
		if err = api.call(ctx, "POST", "/admin/models/providers", admin, map[string]any{"code": "load-peer", "name": "Controlled capacity peer", "base_url": "http://peer:18080/v1", "status": 1}, nil); err != nil {
			return err
		}
		if err = api.call(ctx, "GET", "/admin/models/providers", admin, nil, &providers); err != nil {
			return err
		}
		for _, p := range providers {
			if p.Code == "load-peer" {
				providerID = p.ID
			}
		}
	}
	var models []struct {
		ID    int64
		Alias string
	}
	if err = api.call(ctx, "GET", "/admin/models", admin, nil, &models); err != nil {
		return err
	}
	modelID := int64(0)
	for _, m := range models {
		if m.Alias == "load-controlled" {
			modelID = m.ID
		}
	}
	if modelID == 0 {
		if err = api.call(ctx, "POST", "/admin/models", admin, map[string]any{"provider_id": providerID, "model_name": "controlled", "alias": "load-controlled", "type": "chat", "context_window": 32768, "max_output": 1024, "capabilities": []string{"function_call"}, "status": 1}, nil); err != nil {
			return err
		}
		if err = api.call(ctx, "GET", "/admin/models", admin, nil, &models); err != nil {
			return err
		}
		for _, m := range models {
			if m.Alias == "load-controlled" {
				modelID = m.ID
			}
		}
	}
	if providerID == 0 || modelID == 0 {
		return fmt.Errorf("controlled provider/model was not persisted")
	}
	var servers []struct {
		ID   int64
		Name string
	}
	if err = api.call(ctx, "GET", "/admin/mcp/servers", admin, nil, &servers); err != nil {
		return err
	}
	serverID := int64(0)
	for _, server := range servers {
		if server.Name == "capacity-inspect" {
			serverID = server.ID
		}
	}
	ids := make([]int64, len(f.Actors))
	for i, actor := range f.Actors {
		ids[i] = actor.ID
	}
	if serverID == 0 {
		var server struct{ ID int64 }
		if err = api.call(ctx, "POST", "/admin/mcp/servers", admin, map[string]any{"name": "capacity-inspect", "transport": "streamable_http", "url": "http://peer:18080/mcp", "enabled": true, "allowed_user_ids": ids}, &server); err != nil {
			return err
		}
		serverID = server.ID
	}
	var tools []struct {
		ID             int64
		Code           string
		BindingVersion int64 `json:"binding_version"`
	}
	if err = api.call(ctx, "POST", fmt.Sprintf("/admin/mcp/servers/%d/sync", serverID), admin, map[string]any{}, &tools); err != nil {
		return err
	}
	if len(tools) != 1 || tools[0].ID < 1 {
		return fmt.Errorf("controlled MCP catalog was not discovered")
	}
	tool := tools[0]
	if err = api.call(ctx, "PUT", fmt.Sprintf("/admin/mcp/servers/%d/tools/%d/policy", serverID, tool.ID), admin, map[string]any{"binding_version": tool.BindingVersion, "is_dangerous": false, "replay_policy": "readonly", "evidence": "Controlled inspect returns its nonce after 100ms; no external business side effects, only fixture audit counters."}, nil); err != nil {
		return err
	}
	for i := range f.Actors {
		actor := &f.Actors[i]
		if actor.AgentID == 0 {
			var created struct{ ID int64 }
			if err = api.call(ctx, "POST", "/agents", actor.Token, map[string]any{"name": "capacity-expert", "type": "expert", "model_config_id": modelID, "system_prompt": "Call inspect once, then answer from its actual evidence.", "tools": []any{map[string]any{"tool_id": tool.ID}}, "max_tokens": 128, "max_iterations": 4, "memory_long_enabled": false, "kb_ids": []int64{}, "config": map[string]any{}}, &created); err != nil {
				return err
			}
			actor.AgentID = created.ID
		}
		for _, waiting := range []bool{false, true} {
			if waiting && actor.WaitFlowID != 0 || !waiting && actor.FlowID != 0 {
				continue
			}
			dsl := "name: capacity-wait\nnodes: [{key: review,type: human,prompt: capacity approval}]"
			if !waiting {
				dsl = fmt.Sprintf("name: capacity-short\ninput: [nonce]\nnodes:\n - {key: plan,type: llm,prompt: 'nonce={{nonce}}'}\n - {key: inspect,type: tool,tool_code: %s,args: {nonce: '{{plan.result}}'}}\n - {key: answer,type: llm,prompt: 'final {{inspect.result}}'}", tool.Code)
			}
			var created struct{ ID int64 }
			if err = api.call(ctx, "POST", "/workflows", actor.Token, map[string]any{"name": "capacity", "dsl": dsl}, &created); err != nil {
				return err
			}
			if waiting {
				actor.WaitFlowID = created.ID
			} else {
				actor.FlowID = created.ID
			}
		}
		if err = saveFixture(path, f); err != nil {
			return err
		}
	}
	return nil
}
