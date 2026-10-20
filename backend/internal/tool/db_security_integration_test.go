//go:build integration

package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"strings"
	"testing"
)

func TestDBReportRejectsArbitrarySQLAndIsolatesOwner(t *testing.T) {
	db := testpg.Open(t)
	owners := []model.User{{Username: "one", Email: "one@test.invalid", PasswordHash: "test"}, {Username: "two", Email: "two@test.invalid", PasswordHash: "test"}}
	for i := range owners {
		if err := db.Create(&owners[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := range owners {
		if err := db.Create(&model.UsageLog{UserID: owners[i].ID, Model: "private-model", Scene: "chat", PromptTokens: (i + 1) * 100}).Error; err != nil {
			t.Fatal(err)
		}
	}
	actor := security.WithUserID(context.Background(), owners[0].ID)
	if _, err := runDBQuery(actor, db, map[string]any{"sql": "SELECT current_user"}); err == nil {
		t.Fatal("arbitrary SQL accepted")
	}
	if _, err := runDBQuery(context.Background(), db, map[string]any{"report": "usage_summary"}); err == nil {
		t.Fatal("anonymous reporting accepted")
	}
	raw, err := runDBQuery(actor, db, map[string]any{"report": "usage_summary"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"prompt_tokens":100`) || strings.Contains(raw, `"prompt_tokens":300`) {
		t.Fatal("owner isolation or aggregate failed", raw)
	}
	if _, err := runDBQuery(actor, db, map[string]any{"report": "pg_read_file('/etc/passwd')"}); err == nil {
		t.Fatal("unregistered report accepted")
	}
	var firstRun int64
	for i, user := range owners {
		flow := &model.Workflow{Name: fmt.Sprint("owner-", i), CreatorID: user.ID, DSL: "name: report"}
		if err = db.Create(flow).Error; err != nil {
			t.Fatal(err)
		}
		var runID int64
		if err = db.Raw("INSERT INTO workflow_runs(workflow_id,owner_id,status) VALUES(?,?,'waiting_approval') RETURNING id", flow.ID, user.ID).Scan(&runID).Error; err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstRun = runID
		}
		if err = db.Exec("INSERT INTO execution_approvals(run_id,interrupt_id,path,args_hash,prompt,expires_at) VALUES(?,?,'review','hash','review',NOW()+INTERVAL '1 hour')", runID, fmt.Sprint("review-", i)).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, report := range []string{"recent_runs", "pending_approvals"} {
		raw, err = runDBQuery(actor, db, map[string]any{"report": report})
		if err != nil {
			t.Fatal(report, err)
		}
		var rows []map[string]any
		if err = json.Unmarshal([]byte(raw), &rows); err != nil || len(rows) != 1 {
			t.Fatal(report, raw, err)
		}
		key := "id"
		if report == "pending_approvals" {
			key = "run_id"
		}
		if rows[0][key] != float64(firstRun) {
			t.Fatal("foreign report row", report, raw)
		}
	}
}
