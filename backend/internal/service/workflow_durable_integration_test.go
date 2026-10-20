//go:build integration

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
)

func TestWorkflowConcurrentSubmissionAndArchive(t *testing.T) {
	s, svcs, owner := workflowServiceFixture(t)
	flow := &model.Workflow{Name: "archive", CreatorID: owner, DSL: "name: archive\nnodes: [{key: review, type: human, prompt: review}]"}
	if err := s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan int64, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, err := s.Run(context.Background(), owner, &RunInput{WorkflowID: flow.ID, RequestKey: "parallel-key"})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- run.ID
		}()
	}
	wg.Wait()
	close(ids)
	var id int64
	for next := range ids {
		if id != 0 && next != id {
			t.Fatal("concurrent duplicate")
		}
		id = next
	}
	if id == 0 {
		t.Fatal("no submission")
	}
	if err := s.Delete(context.Background(), owner, flow.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), owner, flow.ID); err == nil {
		t.Fatal("archived definition still editable")
	}
	if _, err := s.GetWorkflowRun(context.Background(), owner, id); err != nil {
		t.Fatalf("archive destroyed history %v", err)
	}
	store := execution.NewStore(svcs.Repos.DB)
	run, err := store.Claim(context.Background(), "after-archive", time.Minute)
	if err != nil || run == nil {
		t.Fatal(err)
	}
	if err = s.Driver().Execute(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWorkflowRun(context.Background(), owner, id)
	if err != nil || got.Status != execution.Waiting {
		t.Fatalf("archive broke frozen job %+v %v", got, err)
	}
}
func TestWorkflowEditRejectsStaleVersion(t *testing.T) {
	s, _, owner := workflowServiceFixture(t)
	flow := &model.Workflow{Name: "edit", CreatorID: owner, DSL: "name: edit\nnodes: [{key: review, type: human, prompt: review}]"}
	if err := s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	first, second := "first editor", "stale editor"
	version := flow.Version
	if err := s.Update(context.Background(), owner, flow.ID, &WorkflowUpdateInput{Name: &first, Version: &version}); err != nil {
		t.Fatal(err)
	}
	err := s.Update(context.Background(), owner, flow.ID, &WorkflowUpdateInput{Name: &second, Version: &version})
	var be *errcode.Error
	if !errors.As(err, &be) || be.HTTPStatus != 409 {
		t.Fatalf("stale overwrite allowed %v", err)
	}
}

func workflowServiceFixture(t *testing.T) (*WorkflowService, *Services, int64) {
	t.Helper()
	db := testpg.Open(t)
	repos := repo.New(db)
	user := &model.User{Username: "workflow-owner", Email: "workflow@test.invalid", PasswordHash: "test-only"}
	if err := db.Create(user).Error; err != nil {
		t.Fatal(err)
	}
	enc, err := security.NewEncryptor("01234567890123456789012345678901")
	if err != nil {
		t.Fatal(err)
	}
	services := &Services{Deps: Deps{Cfg: &config.Config{Agent: config.Agent{RunTimeout: time.Minute}}, Repos: repos, Encryptor: enc}}
	service := NewWorkflowService(services)
	services.Workflow = service
	return service, services, user.ID
}

func TestWorkflowSupportsKeylessLocalModel(t *testing.T) {
	s, services, owner := workflowServiceFixture(t)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("invented model credential")
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"keyless answer"},"finish_reason":"stop"}]}`)
	}))
	defer peer.Close()
	provider := &model.ModelProvider{Code: "keyless", Name: "local", BaseURL: peer.URL, Status: 1}
	if err := services.Repos.DB.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	if err := services.Repos.DB.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "local", ModelName: "local", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	services.Cfg.LLM.DefaultChat = "local"
	services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 2})
	flow := &model.Workflow{Name: "keyless", CreatorID: owner, DSL: "name: keyless\nnodes: [{key: answer,type: llm,prompt: question}]"}
	if err := s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	run, err := s.Run(context.Background(), owner, &RunInput{WorkflowID: flow.ID})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := execution.NewStore(services.Repos.DB).Claim(context.Background(), "keyless-test", time.Minute)
	if err != nil || claimed == nil {
		t.Fatal(err)
	}
	if err = s.Driver().Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil || got.Status != execution.Succeeded {
		t.Fatalf("local keyless provider rejected: %+v %v", got, err)
	}
}

func TestWorkflowFreezesModelSettingsAndEncryptsSnapshot(t *testing.T) {
	s, services, owner := workflowServiceFixture(t)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model string `json:"model"`
		}
		json.NewDecoder(r.Body).Decode(&request)
		if request.Model != "frozen-v1" {
			t.Error("running instance used edited model")
		}
		if r.Header.Get("Authorization") != "Bearer test-private-key" {
			t.Error("frozen credential not restored")
		}
		fmt.Fprint(w, `{"model":"frozen-v1","choices":[{"message":{"role":"assistant","content":"actual frozen answer"},"finish_reason":"stop"}]}`)
	}))
	defer peer.Close()
	cipher, err := services.Encryptor.Encrypt("test-private-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "test", Name: "test", BaseURL: peer.URL, APIKey: cipher, Status: 1}
	if err = services.Repos.LLM.CreateProvider(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "chat", ModelName: "frozen-v1", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}
	if err = services.Repos.LLM.CreateModel(context.Background(), configured); err != nil {
		t.Fatal(err)
	}
	services.Cfg.LLM.DefaultChat = "chat"
	services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 2})
	flow := &model.Workflow{Name: "model-version", CreatorID: owner, DSL: "name: model-version\ninput: [question]\nnodes: [{key: review, type: human, prompt: review}, {key: answer, type: llm, prompt: '{{question}}'}]"}
	if err = s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	run, err := s.Run(context.Background(), owner, &RunInput{WorkflowID: flow.ID, Input: map[string]any{"question": "question"}})
	if err != nil {
		t.Fatal(err)
	}
	store := execution.NewStore(services.Repos.DB)
	snapshot, err := store.Get(context.Background(), owner, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(snapshot.Definition), "test-private-key") {
		t.Fatal("plaintext model credential persisted")
	}
	claimed, err := store.Claim(context.Background(), "before-approval", time.Minute)
	if err != nil || claimed == nil {
		t.Fatal("missing claim")
	}
	if err = s.Driver().Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	configured.ModelName = "changed-after-submit"
	if err = services.Repos.LLM.UpdateModel(context.Background(), configured); err != nil {
		t.Fatal(err)
	}
	provider.BaseURL = "http://127.0.0.1:1"
	if err = services.Repos.LLM.UpdateProvider(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	a := got.Approvals[0]
	if _, err = s.ApproveDecision(context.Background(), owner, run.ID, a.InterruptID, a.ArgsHash, true, "checked"); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.Claim(context.Background(), "after-approval", time.Minute)
	if err != nil || claimed == nil {
		t.Fatal("missing resume")
	}
	if err = s.Driver().Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil || got.Status != execution.Succeeded || !strings.Contains(string(got.Output), "actual frozen answer") {
		t.Fatalf("frozen model failed: %+v %v", got, err)
	}
}

func TestWorkflowServiceQueuesAndResumesExactApproval(t *testing.T) {
	s, services, owner := workflowServiceFixture(t)
	flow := &model.Workflow{Name: "reviews", CreatorID: owner, DSL: "name: reviews\nnodes: [{key: a, type: human, prompt: first}, {key: b, type: human, prompt: second}]"}
	if err := s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	run, err := s.Run(context.Background(), owner, &RunInput{WorkflowID: flow.ID, Input: map[string]any{"request": "original"}})
	if err != nil || run.Status != execution.Queued {
		t.Fatalf("HTTP service did not accept a durable run: %+v %v", run, err)
	}
	store := execution.NewStore(services.Repos.DB)
	drive := func() {
		t.Helper()
		claimed, err := store.Claim(context.Background(), "integration-worker", time.Minute)
		if err != nil || claimed == nil {
			t.Fatalf("not claimable: %v", err)
		}
		if err = s.Driver().Execute(context.Background(), claimed); err != nil {
			t.Fatal(err)
		}
	}
	drive()
	got, err := s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil || got.Status != execution.Waiting || len(got.Approvals) != 1 {
		t.Fatalf("pending approval not exposed: %+v %v", got, err)
	}
	a := got.Approvals[0]
	if _, err = s.ApproveDecision(context.Background(), owner+1, run.ID, a.InterruptID, a.ArgsHash, true, "foreign"); err == nil {
		t.Fatal("foreign user approved run")
	}
	replacement := "name: edited\nnodes: [{key: injected, type: human, prompt: must-not-run}]"
	if err = s.Update(context.Background(), owner, flow.ID, &WorkflowUpdateInput{DSL: &replacement}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ApproveDecision(context.Background(), owner, run.ID, a.InterruptID, a.ArgsHash, true, "first checked"); err != nil {
		t.Fatal(err)
	}
	drive()
	got, err = s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil || got.Status != execution.Waiting || len(got.Approvals) != 1 || got.Approvals[0].Path != "root/b" {
		t.Fatalf("live edit affected frozen run: %+v %v", got, err)
	}
	a = got.Approvals[0]
	if _, err = s.ApproveDecision(context.Background(), owner, run.ID, a.InterruptID, a.ArgsHash, true, "second checked"); err != nil {
		t.Fatal(err)
	}
	drive()
	got, err = s.GetWorkflowRun(context.Background(), owner, run.ID)
	if err != nil || got.Status != execution.Succeeded {
		t.Fatalf("run did not complete: %+v %v", got, err)
	}
	if len(got.ApprovalHistory) != 2 || got.ApprovalHistory[0].Comment != "second checked" || got.ApprovalHistory[1].Comment != "first checked" {
		t.Fatalf("approval audit lost: %+v", got.ApprovalHistory)
	}
	var out map[string]any
	json.Unmarshal(got.Output, &out)
	if out["request"] != "original" || out["b"] != true {
		t.Fatalf("output incomplete: %s", got.Output)
	}
}

func TestWorkflowSubmissionRequestIsIdempotent(t *testing.T) {
	s, _, owner := workflowServiceFixture(t)
	flow := &model.Workflow{Name: "once", CreatorID: owner, DSL: "name: once\nnodes: [{key: review, type: human, prompt: review}]"}
	if err := s.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	in := &RunInput{WorkflowID: flow.ID, Input: map[string]any{"value": "same"}, RequestKey: "client-retry-key"}
	first, err := s.Run(context.Background(), owner, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Run(context.Background(), owner, in)
	if err != nil || second.ID != first.ID {
		t.Fatalf("HTTP retry duplicated run: %+v %+v %v", first, second, err)
	}
	in.Input["value"] = "different"
	_, err = s.Run(context.Background(), owner, in)
	var be *errcode.Error
	if !errors.As(err, &be) || be.HTTPStatus != 409 {
		t.Fatalf("request key accepted different input: %v", err)
	}
}
