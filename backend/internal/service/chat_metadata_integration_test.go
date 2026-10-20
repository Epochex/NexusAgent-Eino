//go:build integration

package service

import (
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"gorm.io/datatypes"
	"os"
	"testing"
)

func TestChatMetadataKeepsTraceInNamedObject(t *testing.T) {
	f := pumpFixture(t)
	trace := observability.NewAgentTrace()
	trace.Add(observability.EventAnswer, "test", "answer")
	events := make(chan agent.Event, 1)
	events <- agent.Evt(agent.TypeDone, map[string]any{})
	close(events)
	out := make(chan agent.Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	f.chat.pump(ctx, cancel, chatRun{ID: "metadata", Session: f.session, Agent: f.ag, UserMessage: f.user, Trace: trace, Warnings: []string{"知识库暂不可用"}}, events, out)
	row := assistant(t, f)
	var metadata struct {
		Trace    []observability.TraceStep `json:"agent_trace"`
		Warnings []string                  `json:"preparation_warnings"`
	}
	if err := json.Unmarshal(row.Metadata, &metadata); err != nil || len(metadata.Trace) != 1 {
		t.Fatalf("history cannot render trace metadata: %s %v", row.Metadata, err)
	}
	if len(metadata.Warnings) != 1 {
		t.Fatal("degradation warning lost from history")
	}
	first := <-out
	if first.Type != agent.TypeMeta || len(first.Data["preparation_warnings"].([]string)) != 1 {
		t.Fatal("degradation warning lost from SSE metadata")
	}
}

func TestAuditMigrationPreservesHistoricalTraceAndWarnings(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	session := &model.Session{UserID: owner}
	if err := services.Repos.Session.CreateSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	legacy := &model.Message{SessionID: session.ID, Role: "assistant", Content: "legacy", Metadata: datatypes.JSON(`[{"seq":1,"type":"answer","content":"legacy"}]`)}
	current := &model.Message{SessionID: session.ID, Role: "assistant", Content: "current", Metadata: datatypes.JSON(`{"agent_trace":[],"preparation_warnings":["visible warning"]}`)}
	if err := services.Repos.DB.Create(legacy).Error; err != nil {
		t.Fatal(err)
	}
	if err := services.Repos.DB.Create(current).Error; err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile("../../migrations/008_retrieval_audit.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = services.Repos.DB.Exec(string(sql)).Error; err != nil {
		t.Fatal(err)
	}
	services.Repos.DB.First(legacy, legacy.ID)
	services.Repos.DB.First(current, current.ID)
	var migrated struct {
		Trace []observability.TraceStep `json:"agent_trace"`
	}
	if err = json.Unmarshal(legacy.Metadata, &migrated); err != nil || len(migrated.Trace) != 1 {
		t.Fatal("legacy audit was not migrated", err)
	}
	var preserved struct {
		Warnings []string `json:"preparation_warnings"`
	}
	if err = json.Unmarshal(current.Metadata, &preserved); err != nil || len(preserved.Warnings) != 1 {
		t.Fatal("existing structured metadata was changed", err)
	}
}
