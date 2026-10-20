//go:build integration

package tool

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
)

func TestRegisteredCodeExecCommitsOwnedSuccessfulAudit(t *testing.T) {
	sandbox := realSandbox(t, "128m", "0.5", 5e9)
	db := testpg.Open(t)

	var owner int64
	if err := db.Raw("INSERT INTO users(username,email,password_hash) VALUES('code-audit-owner','code-audit@test.invalid','public-test-only') RETURNING id").Scan(&owner).Error; err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	RegisterCodeExec(reg, sandbox, db)
	handler, _ := reg.Get("code_exec")
	code := "printf registered-tool"
	output, err := handler(security.WithUserID(context.Background(), owner), map[string]any{"language": "shell", "code": code, "user_id": owner + 999}, "")
	if err != nil || !strings.Contains(output, "registered-tool") {
		t.Fatal("registered tool did not complete", output, err)
	}
	var row struct {
		UserID               int64
		ExitCode             int
		CodeHash, StdoutHead string
	}
	if err = db.Table("sandbox_executions").Order("id DESC").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.UserID != owner || row.ExitCode != 0 || row.CodeHash != fmt.Sprintf("%x", sha256.Sum256([]byte(code))) || row.StdoutHead != "registered-tool" {
		t.Fatalf("incorrect persisted audit: %+v", row)
	}
}
