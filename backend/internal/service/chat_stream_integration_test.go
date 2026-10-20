//go:build integration

package service

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type integrationNoTools struct{}

func (integrationNoTools) ListContext(context.Context, string) ([]llm.ToolDef, error) {
	return nil, nil
}
func (integrationNoTools) Invoke(context.Context, string, string, string) tool.ToolResult {
	return tool.ToolResult{}
}

func TestChatEmptyModelAnswerPersistsFailed(t *testing.T) {
	f := pumpFixture(t)
	modelServer := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	runner, err := runtime.NewEinoRunner(llm.NewGateway(&config.LLM{}), integrationNoTools{}, runtime.EinoConfig{StepTimeout: time.Second, RunTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := runner.Run(ctx, &runtime.RunInput{AgentName: "test", Messages: []llm.Message{{Role: llm.RoleUser, Content: "q"}}, Primary: &llm.ModelDescriptor{BaseURL: modelServer.URL, ModelName: "stub"}})
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan agent.Event, 16)
	go f.chat.pump(ctx, cancel, chatRun{ID: "empty-run", Session: f.session, Agent: f.ag, UserMessage: f.user}, events, out)
	var failed bool
	for ev := range out {
		if ev.Type == agent.TypeDone {
			t.Fatal("empty model answer reported done")
		}
		if ev.Type == agent.TypeError {
			failed = true
		}
	}
	if row := assistant(t, f); !failed || row.Status != model.MsgStatusFailed {
		t.Fatalf("empty answer persisted success: %+v failed=%v", row, failed)
	}
}

type pumpTestFixture struct {
	chat    *ChatService
	db      *gorm.DB
	session *model.Session
	user    *model.Message
	ag      *model.Agent
}

func pumpDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("NX_TEST_DATABASE_DSN")
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" || !strings.HasSuffix(parsed.Path, "_test") {
		t.Fatal("NX_TEST_DATABASE_DSN must be an explicit PostgreSQL URL for a database ending in _test")
	}
	base, err := gorm.Open(postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("test PostgreSQL unavailable")
	}
	schemaName := "nexus_it_" + strings.ToLower(rand.Text())
	if err = base.Exec(fmt.Sprintf(`CREATE SCHEMA "%s"`, schemaName)).Error; err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: parsed.String(), PreferSimpleProtocol: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("cannot open isolated test schema")
	}
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		if !strings.HasPrefix(schemaName, "nexus_it_") {
			t.Fatal("invalid cleanup scope")
		}
		if err := base.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName)).Error; err != nil {
			t.Error(err)
		}
		sqlBase, _ := base.DB()
		_ = sqlBase.Close()
	})
	migration, err := os.ReadFile("../../migrations/001_init.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Exec(string(migration)).Error; err != nil {
		t.Fatal(err)
	}
	return db
}
func pumpFixture(t *testing.T) pumpTestFixture {
	t.Helper()
	db := pumpDB(t)
	repos := repo.New(db)
	user := &model.User{Username: "test", Email: "test@example.com", PasswordHash: "not-a-login-credential"}
	if err := db.Create(user).Error; err != nil {
		t.Fatal(err)
	}
	session := &model.Session{UserID: user.ID}
	if err := repos.Session.CreateSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	message := &model.Message{SessionID: session.ID, Role: "user", Content: "question"}
	if err := repos.Session.CreateMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	svcs := &Services{Deps: Deps{Repos: repos, Cfg: &config.Config{Agent: config.Agent{SendTimeout: 20 * time.Millisecond, EventBuffer: 16}}}}
	return pumpTestFixture{chat: NewChatService(svcs), db: db, session: session, user: message, ag: &model.Agent{Name: "test"}}
}
func startPump(t *testing.T, f pumpTestFixture, ctx context.Context, events []agent.Event, buffer int) (<-chan agent.Event, <-chan struct{}) {
	t.Helper()
	input := make(chan agent.Event, len(events))
	for _, ev := range events {
		input <- ev
	}
	close(input)
	out := make(chan agent.Event, buffer)
	done := make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(done)
		f.chat.pump(runCtx, cancel, chatRun{ID: "integration-run", Session: f.session, Agent: f.ag, UserMessage: f.user}, input, out)
	}()
	return out, done
}
func assistant(t *testing.T, f pumpTestFixture) model.Message {
	t.Helper()
	var row model.Message
	if err := f.db.Where("session_id = ? AND role = 'assistant'", f.session.ID).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func TestChatPersistsTruthfulOutcomes(t *testing.T) {
	for _, c := range []struct {
		name string
		last agent.Event
		want int16
	}{{"success", agent.EvtDone("stop"), model.MsgStatusDone}, {"error", agent.EvtError(50001, "model failed"), model.MsgStatusFailed}, {"cancel", agent.EvtDone("canceled"), model.MsgStatusStopped}} {
		t.Run(c.name, func(t *testing.T) {
			f := pumpFixture(t)
			out, _ := startPump(t, f, context.Background(), []agent.Event{agent.EvtDelta("answer"), c.last}, 16)
			for range out {
			}
			row := assistant(t, f)
			if row.Status != c.want || row.Content != "answer" {
				t.Fatalf("untruthful persisted outcome: %+v", row)
			}
		})
	}
}
func TestChatNoDoneBeforeMessageCommit(t *testing.T) {
	f := pumpFixture(t)
	tx := f.db.Begin()
	if err := tx.Exec("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE").Error; err != nil {
		t.Fatal(err)
	}
	out, done := startPump(t, f, context.Background(), []agent.Event{agent.EvtDelta("answer"), agent.EvtDone("stop")}, 16)
	premature := false
	timer := time.NewTimer(80 * time.Millisecond)
watch:
	for {
		select {
		case ev := <-out:
			if ev.Type == agent.TypeDone {
				premature = true
				break watch
			}
		case <-timer.C:
			break watch
		}
	}
	timer.Stop()
	tx.Rollback()
	for range out {
	}
	<-done
	if premature {
		t.Fatal("success was visible before the message transaction committed")
	}
}
func TestChatPersistenceFailureIsNotSuccess(t *testing.T) {
	f := pumpFixture(t)
	if err := f.db.Exec("ALTER TABLE messages ADD CONSTRAINT reject_assistant CHECK (role <> 'assistant')").Error; err != nil {
		t.Fatal(err)
	}
	out, _ := startPump(t, f, context.Background(), []agent.Event{agent.EvtDelta("answer"), agent.EvtDone("stop")}, 16)
	failed := false
	for ev := range out {
		if ev.Type == agent.TypeDone {
			t.Error("persistence failure reported successful completion")
		}
		if ev.Type == agent.TypeError {
			failed = true
		}
	}
	if !failed {
		t.Fatal("persistence failure was invisible")
	}
}
func TestChatCanceledRequestStillPersists(t *testing.T) {
	f := pumpFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, done := startPump(t, f, ctx, []agent.Event{agent.EvtDelta("partial")}, 16)
	for range out {
	}
	select {
	case <-done:
	case <-time.After(3500 * time.Millisecond):
		t.Fatal("canceled request persistence is unbounded")
	}
	if row := assistant(t, f); row.Status != model.MsgStatusStopped {
		t.Fatalf("canceled request persisted success: %d", row.Status)
	}
}
func TestChatSlowConsumerPersistsActualTermination(t *testing.T) {
	f := pumpFixture(t)
	var events []agent.Event
	for range 300 {
		events = append(events, agent.EvtDelta("x"))
	}
	events = append(events, agent.EvtDone("stop"))
	out, done := startPump(t, f, context.Background(), events, 1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow consumer leaked pump")
	}
	for range out {
	}
	if row := assistant(t, f); row.Status != model.MsgStatusFailed {
		t.Fatal("silently dropped output but persisted successful run")
	}
}
func TestChatPersistenceDeadlineBound(t *testing.T) {
	f := pumpFixture(t)
	tx := f.db.Begin()
	if err := tx.Exec("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE").Error; err != nil {
		t.Fatal(err)
	}
	out, done := startPump(t, f, context.Background(), []agent.Event{agent.EvtDone("stop")}, 16)
	timedOut := false
	select {
	case <-done:
	case <-time.After(3500 * time.Millisecond):
		timedOut = true
	}
	tx.Rollback()
	for range out {
	}
	<-done
	if timedOut {
		t.Fatal("database finalization exceeded its three-second deadline")
	}
}
func TestChatRejectsForeignSession(t *testing.T) {
	f := pumpFixture(t)
	_, err := f.chat.Stream(context.Background(), f.session.UserID+1, &ChatInput{SessionID: f.session.ID, Content: "not mine"})
	if err == nil {
		t.Fatal("foreign session accepted")
	}
	var n int64
	f.db.Model(&model.Message{}).Count(&n)
	if n != 1 {
		t.Fatal("foreign request changed message history")
	}
}

func TestChatOverridesDoNotMutateCachedDefault(t *testing.T) {
	f := pumpFixture(t)
	cached := &llm.ModelDescriptor{ModelName: "cached", Temperature: 0.7, TopP: 0.9, MaxTokens: 128}
	f.chat.svcs.defaultChat = cached
	f.chat.svcs.Cfg.LLM.DefaultChat = "missing"
	ag := &model.Agent{Name: "test", Temperature: 0.2, TopP: 0.3, MaxTokens: 64}
	input, _, err := f.chat.buildRunInput(context.Background(), f.session, ag, "question")
	if err != nil {
		t.Fatal(err)
	}
	if cached.Temperature != 0.7 || cached.TopP != 0.9 || cached.MaxTokens != 128 || input.Primary.Temperature != 0.2 {
		t.Fatal("per-agent sampling changed the shared fallback model configuration")
	}
}
