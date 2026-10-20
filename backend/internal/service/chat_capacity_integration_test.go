//go:build integration

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

func capacityChatFixture(t *testing.T) pumpTestFixture {
	t.Helper()
	_, svcs, owner := workflowServiceFixture(t)
	session := &model.Session{UserID: owner}
	if err := svcs.Repos.Session.CreateSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	user := &model.Message{SessionID: session.ID, Role: "user", Content: "capacity question"}
	if err := svcs.Repos.Session.CreateMessage(context.Background(), user); err != nil {
		t.Fatal(err)
	}
	svcs.Capacity = capacity.New(svcs.Repos.DB)
	svcs.Cfg.Agent.SendTimeout = time.Second
	return pumpTestFixture{chat: NewChatService(svcs), db: svcs.Repos.DB, session: session, user: user, ag: &model.Agent{Name: "capacity"}}
}

func TestChatShutdownCancelsRealModelAndWaitsForTerminalCommit(t *testing.T) {
	f := capacityChatFixture(t)
	entered, stopped := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"provisional\"}}]}\n\n")
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	provider := model.ModelProvider{Code: "shutdown", Name: "shutdown", BaseURL: peer.URL, Status: 1}
	if err := f.db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.db.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "shutdown", ModelName: "controlled", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	f.chat.svcs.Cfg.LLM.DefaultChat = "shutdown"
	f.chat.svcs.Cfg.Agent.RunTimeout = time.Minute
	f.chat.svcs.Cfg.Agent.StepTimeout = time.Minute
	f.chat.svcs.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4})
	f.chat.svcs.Executor = noTools{}
	stream, err := f.chat.Stream(context.Background(), f.session.UserID, &ChatInput{SessionID: f.session.ID, Content: "hold until shutdown"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual Eino model request not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.chat.Close(ctx); err != nil {
		t.Fatal("shutdown did not drain Chat finalization", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Error("shutdown left model HTTP running")
	}
	for ev := range stream {
		if ev.Type == agent.TypeDone && ev.Data["finish_reason"] != "canceled" {
			t.Error("shutdown fabricated successful answer")
		}
	}
	if row := assistant(t, f); row.Status != model.MsgStatusStopped {
		t.Errorf("shutdown returned before canceled answer was committed: status=%d", row.Status)
	}
	var active int64
	if err = f.db.Table("execution_active_capacity").Where("owner_id=?", f.session.UserID).Count(&active).Error; err != nil || active != 0 {
		t.Error("shutdown leaked shared execution quota", active, err)
	}
	if _, err = f.chat.Stream(context.Background(), f.session.UserID, &ChatInput{Content: "late request"}); err == nil {
		t.Error("shutdown admitted new Chat")
	}
}

func TestChatTotalDeadlineIncludesPreparation(t *testing.T) {
	f := capacityChatFixture(t)
	f.chat.svcs.Cfg.Agent.RunTimeout = 100 * time.Millisecond
	tx := f.db.Begin()
	defer tx.Rollback()
	// Permit the user's message FK, but block the preparation statistics update.
	if err := tx.Exec("SELECT id FROM sessions WHERE id=? FOR NO KEY UPDATE", f.session.ID).Error; err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.chat.Stream(parent, f.session.UserID, &ChatInput{SessionID: f.session.ID, Content: "bounded preparation"})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("blocked preparation reported successful start")
		}
	case <-time.After(800 * time.Millisecond):
		t.Error("preparation ignored the configured total run deadline")
		cancel()
		tx.Rollback()
		<-done
	}
}

func TestChatLostLeaseCannotPublishSuccess(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "expired_before_finalization"
		if blocked {
			name = "expires_while_finalization_waits"
		}
		t.Run(name, func(t *testing.T) {
			f := capacityChatFixture(t)
			ctx, release, err := f.chat.svcs.Capacity.HoldRun(context.Background(), f.session.UserID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			expire := func() {
				t.Helper()
				if err := f.db.Exec("UPDATE execution_capacity_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='run' AND owner_id=?", f.session.UserID).Error; err != nil {
					t.Fatal(err)
				}
			}
			if !blocked {
				expire()
			}
			tx := f.db.Begin()
			defer tx.Rollback()
			if blocked {
				if err := tx.Exec("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE").Error; err != nil {
					t.Fatal(err)
				}
			}
			out, done := startPump(t, f, ctx, []agent.Event{agent.EvtDelta("provisional answer"), agent.EvtDone("stop")}, 16)
			if blocked {
				// Wait until the actual assistant INSERT is blocked, not a guessed sleep.
				waiting := false
				for until := time.Now().Add(time.Second); time.Now().Before(until); {
					var count int64
					if err := f.db.Raw("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%INSERT INTO%messages%' AND query NOT LIKE '%pg_stat_activity%'").Scan(&count).Error; err != nil {
						t.Fatal(err)
					}
					if count > 0 {
						waiting = true
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if !waiting {
					tx.Rollback()
					for range out {
					}
					t.Fatal("assistant INSERT did not enter the intended lock wait")
				}
				expire()
			}
			tx.Rollback()
			failed := false
			for ev := range out {
				if ev.Type == agent.TypeDone && ev.Data["finish_reason"] != "canceled" {
					t.Error("expired executor published successful completion")
				}
				failed = failed || ev.Type == agent.TypeError
			}
			<-done
			var success int64
			if err := f.db.Model(&model.Message{}).Where("session_id=? AND role='assistant' AND status=?", f.session.ID, model.MsgStatusDone).Count(&success).Error; err != nil {
				t.Fatal(err)
			}
			if success != 0 || !failed {
				t.Errorf("lost lease persisted or reported success: successful_rows=%d failed_event=%v", success, failed)
			}
		})
	}
}
