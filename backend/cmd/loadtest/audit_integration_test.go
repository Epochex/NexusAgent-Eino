//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
)

func TestAuditFindsCommittedWorkWithoutAnHTTPAcknowledgement(t *testing.T) {
	db := testpg.Open(t)
	var owner, flow int64
	if err := db.Raw("INSERT INTO users(username,email,password_hash) VALUES('audit','audit@test.invalid','public-fixture') RETURNING id").Scan(&owner).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("INSERT INTO workflows(name,dsl,creator_id) VALUES('audit','name: audit',?) RETURNING id", owner).Scan(&flow).Error; err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"load-audit-lost", "load-other-not-in-scope"} {
		if err := db.Exec("INSERT INTO workflow_runs(workflow_id,owner_id,status,request_key,input,output,definition) VALUES(?,?,'succeeded',?,'{}',?::jsonb,'{}')", flow, owner, key, `{"result":"completed nonce=`+key+`"}`).Error; err != nil {
			t.Fatal(err)
		}
	}
	repos := repo.New(db)
	session := model.Session{UserID: owner}
	if err := repos.Session.CreateSession(context.Background(), &session); err != nil {
		t.Fatal(err)
	}
	if err := repos.Session.CreateMessage(context.Background(), &model.Message{SessionID: session.ID, Role: "user", Content: "nonce=load-audit-chat"}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Session.CreateMessage(context.Background(), &model.Message{SessionID: session.ID, Role: "assistant", Content: "completed nonce=load-audit-chat", Status: model.MsgStatusDone}); err != nil {
		t.Fatal(err)
	}
	got, err := auditDatabase(context.Background(), db, "load-audit-", map[int64]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowTotal != 1 || got.WorkflowStates["succeeded"] != 1 || got.UnacknowledgedWorkflows != 1 || got.ChatTotal != 1 || got.ChatStates["done"] != 1 {
		t.Fatalf("audit trusted client acknowledgements or included foreign stage: %+v", got)
	}
}
