//go:build integration

package service

import (
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPLegacyUpgradeRequiresReviewAndEncryptsOnlyExplicitly(t *testing.T) {
	db := testpg.OpenAt(t, 10)
	owner := &model.User{Username: "legacy-admin", Email: "legacy@test.invalid", PasswordHash: "test", Role: "admin"}
	if err := db.Create(owner).Error; err != nil {
		t.Fatal(err)
	}
	var serverID int64
	if err := db.Raw(`INSERT INTO mcp_servers(name,transport,url,headers,env,command,args) VALUES('legacy','streamable_http','https://mcp.example.invalid','{"Authorization":"Bearer public-legacy-key"}','{"TOKEN":"public-legacy-env"}','unused-command','["--legacy"]') RETURNING id`).Scan(&serverID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO tools(code,name,type,description,parameters,mcp_server_id,is_dangerous,replay_policy,status) VALUES('mcp_legacy_write','write','mcp','legacy write','{"type":"object"}',?,FALSE,'readonly',1)`, serverID).Error; err != nil {
		t.Fatal(err)
	}
	// Seed at the old boundary, then upgrade to the schema required by the
	// current production constructor. Never bypass admission's startup check.
	for _, filename := range []string{"011_mcp_authority.up.sql", "012_a2a_authority.up.sql", "013_shared_capacity.up.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", filename))
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Exec(string(migration)).Error; err != nil {
			t.Fatal(err)
		}
	}
	repos := repo.New(db)
	stored, err := repos.Tool.GetByCode(context.Background(), "mcp_legacy_write")
	if err != nil {
		t.Fatal(err)
	}
	if !stored.IsDangerous || stored.ReplayPolicy != "manual" || stored.BindingVersion < 1 {
		t.Fatal("legacy unreviewed MCP kept unsafe execution policy", stored.IsDangerous, stored.ReplayPolicy)
	}
	encryptor, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	svcs, err := New(Deps{Cfg: &config.Config{MCP: config.MCP{Enabled: true}}, Repos: repos, Encryptor: encryptor})
	if err != nil {
		t.Fatal(err)
	}
	ctx := security.WithUserID(context.Background(), owner.ID)
	before, err := repos.MCP.Get(ctx, serverID)
	if err != nil {
		t.Fatal(err)
	}
	if before.EncryptedCredentials != "" || !strings.Contains(string(before.Headers), "public-legacy-key") {
		t.Fatal("startup silently rewrote legacy credentials")
	}
	rows, err := svcs.MCP.List(ctx)
	if err != nil || len(rows) != 1 || !rows[0].CredentialsPendingMigration {
		t.Fatal("pending migration invisible", rows, err)
	}
	raw, _ := json.Marshal(rows)
	if strings.Contains(string(raw), "public-legacy-key") || strings.Contains(string(raw), "public-legacy-env") {
		t.Fatal("legacy credentials leaked through management")
	}
	if err = svcs.MCP.MigrateCredentials(ctx, serverID); err != nil {
		t.Fatal(err)
	}
	after, err := repos.MCP.Get(ctx, serverID)
	if err != nil {
		t.Fatal(err)
	}
	if after.EncryptedCredentials == "" || string(after.Headers) != "{}" || string(after.Env) != "{}" || string(after.Args) != "[]" || after.Command != "" {
		t.Fatal("explicit migration left plaintext credential fields")
	}
	plaintext, err := encryptor.Decrypt(after.EncryptedCredentials)
	if err != nil || !strings.Contains(plaintext, "public-legacy-key") || !strings.Contains(plaintext, "public-legacy-env") {
		t.Fatal("migration lost recoverable configuration", err)
	}
}

func TestBuiltinReportMigrationAcceptsExistingTool(t *testing.T) {
	db := testpg.OpenAt(t, 9)
	if err := db.Exec(`INSERT INTO tools(code,name,type,description,parameters,is_dangerous,replay_policy,status) VALUES('db_query','query','builtin','legacy SQL','{"type":"object","properties":{"sql":{"type":"string"}}}',TRUE,'manual',1)`).Error; err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "010_builtin_tool_boundaries.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Exec(string(migration)).Error; err != nil {
		t.Fatal("existing-tool upgrade failed", err)
	}
	stored, err := repo.New(db).Tool.GetByCode(context.Background(), "db_query")
	if err != nil {
		t.Fatal(err)
	}
	if stored.IsDangerous || stored.ReplayPolicy != "readonly" || !strings.Contains(string(stored.Parameters), "report") {
		t.Fatal("report policy/schema not migrated")
	}
}
