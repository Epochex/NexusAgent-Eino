//go:build integration

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"gorm.io/datatypes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func mcpServiceFixture(t *testing.T) (*Services, context.Context, *sdk.Server, *model.MCPServer, *atomic.Int32) {
	t.Helper()
	_, base, owner := workflowServiceFixture(t)
	if err := base.Repos.DB.Model(&model.User{}).Where("id=?", owner).Update("role", "admin").Error; err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	remote := sdk.NewServer(&sdk.Implementation{Name: "service-peer", Version: "1"}, nil)
	remote.AddTool(&sdk.Tool{Name: "inspect", Description: "read diagnostic status", InputSchema: map[string]any{"type": "object", "additionalProperties": false}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		calls.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "confirmed-status"}}}, nil
	})
	peer := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return remote }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	t.Cleanup(peer.Close)
	base.Cfg.MCP.Enabled = true
	base.Cfg.Outbound.PrivateOrigins = []string{peer.URL}
	reg := tool.NewRegistry()
	base.Registry = reg
	base.Executor = tool.NewExecutor(base.Repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 5 * time.Second})
	svcs, err := New(base.Deps)
	if err != nil {
		t.Fatal(err)
	}
	svcs.MCP.SetRegistry(reg)
	ctx := security.WithUserID(context.Background(), owner)
	srv := &model.MCPServer{Name: "service-peer", Transport: "streamable_http", URL: peer.URL, Headers: datatypes.JSON(`{"Authorization":"Bearer public-service-fixture-key"}`)}
	if err = svcs.MCP.Create(ctx, srv); err != nil {
		t.Fatal(err)
	}
	return svcs, ctx, remote, srv, calls
}

func TestMCPCredentialsNeverPersistOrReturnAsPlaintext(t *testing.T) {
	svcs, ctx, _, srv, _ := mcpServiceFixture(t)
	stored, err := svcs.Repos.MCP.Get(ctx, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored.Headers), "public-service-fixture-key") {
		t.Fatal("raw MCP credentials persisted in JSONB")
	}
	rows, err := svcs.MCP.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "public-service-fixture-key") || strings.Contains(string(raw), "encrypted_credentials") {
		t.Fatal("MCP management exposed credentials")
	}
}

func TestMCPSyncPreservesIdentityAndRollsBackPartialChanges(t *testing.T) {
	svcs, ctx, remote, srv, _ := mcpServiceFixture(t)
	first, err := svcs.MCP.SyncTools(ctx, srv.ID)
	if err != nil || len(first) != 1 {
		t.Fatal("initial sync", err)
	}
	second, err := svcs.MCP.SyncTools(ctx, srv.ID)
	if err != nil || len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatal("sync changed bound tool ID", first, second, err)
	}
	if _, err = svcs.MCP.UpdateToolPolicy(ctx, srv.ID, first[0].ID, &MCPToolPolicyInput{BindingVersion: first[0].BindingVersion, ReplayPolicy: "readonly", Evidence: "reviewed read-only diagnostic handler"}); err != nil {
		t.Fatal(err)
	}
	if err = svcs.Repos.DB.Exec(`CREATE FUNCTION reject_broken_tool() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='broken' THEN RAISE EXCEPTION 'injected sync failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_broken_tool BEFORE INSERT OR UPDATE ON tools FOR EACH ROW EXECUTE FUNCTION reject_broken_tool()`).Error; err != nil {
		t.Fatal(err)
	}
	remote.AddTool(&sdk.Tool{Name: "broken", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{}, nil
	})
	if _, err = svcs.MCP.SyncTools(ctx, srv.ID); err == nil {
		t.Fatal("failed sync was accepted")
	}
	current, err := svcs.Repos.Tool.GetByCode(ctx, first[0].Code)
	if err != nil || current.Status != 1 || current.ID != first[0].ID {
		t.Fatal("partial sync destroyed usable previous binding", current, err)
	}
	out, err := svcs.MCP.CallTool(ctx, srv.ID, "inspect", map[string]any{})
	if err != nil || out != "confirmed-status" {
		t.Fatal("previous tool unusable after failed sync", out, err)
	}
}

func TestMCPRuntimePermissionsBeforeModelToolExposure(t *testing.T) {
	svcs, admin, _, srv, _ := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	user := &model.User{Username: "ordinary-mcp-user", Email: "ordinary-mcp@test.invalid", PasswordHash: "test"}
	if err = svcs.Repos.DB.Create(user).Error; err != nil {
		t.Fatal(err)
	}
	ctx := security.WithUserID(context.Background(), user.ID)
	binding := fmt.Sprintf(`[{"tool_id":%d}]`, rows[0].ID)
	if _, err = svcs.Executor.ListContext(ctx, binding); err == nil {
		t.Fatal("admin-only MCP tool exposed to ordinary user")
	}
	current, err := svcs.MCP.Get(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.AllowedUserIDs = datatypes.JSON(fmt.Sprintf(`[%d]`, user.ID))
	if err = svcs.MCP.Update(admin, current); err != nil {
		t.Fatal(err)
	}
	if defs, err := svcs.Executor.ListContext(ctx, binding); err != nil || len(defs) != 1 {
		t.Fatal("explicit MCP grant not effective", defs, err)
	}
	if err = svcs.Repos.DB.Model(user).Update("status", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = svcs.Executor.ListContext(ctx, binding); err == nil {
		t.Fatal("disabled user retained MCP access")
	}
}

func TestMCPBindingVersionGuardsRemoteTargetSwitch(t *testing.T) {
	svcs, admin, _, srv, calls := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := svcs.MCP.Get(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.URL += "/changed-target"
	if err = svcs.MCP.Update(admin, current); err != nil {
		t.Fatal(err)
	}
	handler, ok := svcs.Registry.Get(rows[0].Code)
	if !ok {
		t.Fatal("synced handler missing")
	}
	if out, err := handler(tool.WithBindingVersion(admin, rows[0].Code, rows[0].BindingVersion), map[string]any{}, ""); err == nil || calls.Load() != 0 {
		t.Fatal("old approval reached new endpoint", out, err, calls.Load())
	}
}

func TestMCPReviewedPolicyAndRegistryRestart(t *testing.T) {
	svcs, admin, remote, srv, calls := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rows[0].IsDangerous || rows[0].ReplayPolicy != "manual" {
		t.Fatal("discovery implicitly trusted remote tools")
	}
	reviewed, err := svcs.MCP.UpdateToolPolicy(admin, srv.ID, rows[0].ID, &MCPToolPolicyInput{BindingVersion: rows[0].BindingVersion, ReplayPolicy: "readonly", IsDangerous: false, Evidence: "fixture handler reviewed as read-only"})
	if err != nil {
		t.Fatal(err)
	}
	if reviewed.PolicyReviewedBy == nil || reviewed.PolicyReviewedAt == nil || reviewed.IsDangerous || reviewed.ReplayPolicy != "readonly" {
		t.Fatal("policy review not persisted")
	}
	registry := tool.NewRegistry()
	dependencies := svcs.Deps
	dependencies.Registry = registry
	dependencies.Executor = tool.NewExecutor(svcs.Repos.Tool, registry, tool.ExecutorConfig{DefaultTimeout: 5 * time.Second})
	restarted, err := New(dependencies)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.MCP.RestoreRegistry(admin); err != nil {
		t.Fatal(err)
	}
	binding := fmt.Sprintf(`[{"tool_id":%d}]`, reviewed.ID)
	if result := restarted.Executor.Invoke(admin, binding, reviewed.Code, `{}`); result.Status != "ok" || result.Output != "confirmed-status" {
		t.Fatal("restored tool failed", result)
	}
	remote.AddTool(&sdk.Tool{Name: "new_after_startup", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		calls.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "new-tool"}}}, nil
	})
	synced, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldHandler, _ := restarted.Registry.Get(reviewed.Code)
	if out, err := oldHandler(tool.WithBindingVersion(admin, reviewed.Code, reviewed.BindingVersion), map[string]any{}, ""); err != nil || out != "confirmed-status" {
		t.Fatal("unrelated catalogue addition invalidated reviewed binding", out, err)
	}
	var added model.Tool
	for _, item := range synced {
		if item.MCPName == "new_after_startup" {
			added = item
		}
	}
	addedPtr, err := svcs.MCP.UpdateToolPolicy(admin, srv.ID, added.ID, &MCPToolPolicyInput{BindingVersion: added.BindingVersion, ReplayPolicy: "readonly", Evidence: "reviewed diagnostic fixture"})
	if err != nil {
		t.Fatal(err)
	}
	result := restarted.Executor.Invoke(admin, fmt.Sprintf(`[{"tool_id":%d}]`, addedPtr.ID), addedPtr.Code, `{}`)
	if result.Status != "ok" || result.Output != "new-tool" || calls.Load() != 3 {
		t.Fatal("already-running registry did not dispatch newly synced tool", result, calls.Load())
	}
	remote.RemoveTools("inspect")
	if _, err = svcs.MCP.SyncTools(admin, srv.ID); err != nil {
		t.Fatal(err)
	}
	if result = restarted.Executor.Invoke(admin, binding, reviewed.Code, `{}`); result.Status == "ok" || calls.Load() != 3 {
		t.Fatal("removed tool executed through stale registry", result, calls.Load())
	}
}

func TestMCPDebugCannotBypassDurableWriteApproval(t *testing.T) {
	svcs, admin, _, srv, calls := mcpServiceFixture(t)
	if _, err := svcs.MCP.SyncTools(admin, srv.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svcs.MCP.CallTool(admin, srv.ID, "inspect", map[string]any{}); err == nil || calls.Load() != 0 {
		t.Fatal("unreviewed tool executed through debug bypass", err, calls.Load())
	}
}

func TestMCPUnsupportedRuntimeSchemaDoesNotPublishPartialCatalogue(t *testing.T) {
	svcs, admin, remote, srv, _ := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	remote.AddTool(&sdk.Tool{Name: "unsupported", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"payload": map[string]any{"type": "string", "contentEncoding": "base64"}}}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{}, nil
	})
	if _, err = svcs.MCP.SyncTools(admin, srv.ID); err == nil {
		t.Fatal("catalogue published tools that Eino cannot load")
	}
	current, err := svcs.Repos.Tool.GetByCode(admin, rows[0].Code)
	if err != nil || current.BindingVersion != rows[0].BindingVersion || current.Status != 1 {
		t.Fatal("unsupported catalogue changed previous tool", current, err)
	}
}

func TestMCPMetadataUpdatesUseCASWithoutRevokingReviewedBinding(t *testing.T) {
	svcs, admin, _, srv, _ := mcpServiceFixture(t)
	rows, err := svcs.MCP.SyncTools(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	reviewed, err := svcs.MCP.UpdateToolPolicy(admin, srv.ID, rows[0].ID, &MCPToolPolicyInput{BindingVersion: rows[0].BindingVersion, ReplayPolicy: "readonly", Evidence: "read-only peer implementation reviewed"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svcs.MCP.Get(admin, srv.ID)
	if err != nil {
		t.Fatal(err)
	}
	stale := *first
	first.Name = "renamed-peer"
	if err = svcs.MCP.Update(admin, first); err != nil {
		t.Fatal(err)
	}
	stale.Name = "stale-edit"
	if err = svcs.MCP.Update(admin, &stale); err == nil {
		t.Fatal("stale metadata edit accepted")
	}
	current, err := svcs.Repos.Tool.GetByCode(admin, reviewed.Code)
	if err != nil || current.BindingVersion != reviewed.BindingVersion || !validMCPReview(current) {
		t.Fatal("metadata edit revoked unchanged tool review", current, err)
	}
	first.URL += "/different-target"
	if err = svcs.MCP.Update(admin, first); err != nil {
		t.Fatal(err)
	}
	current, err = svcs.Repos.Tool.GetByCode(admin, reviewed.Code)
	if err != nil || current.BindingVersion <= reviewed.BindingVersion || !current.IsDangerous || current.ReplayPolicy != "manual" || current.PolicyReviewedBy != nil {
		t.Fatal("target change retained old target review", current, err)
	}
}
