//go:build integration

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

type serviceA2APeer struct{ calls *atomic.Int32 }

func (p serviceA2APeer) Execute(ctx context.Context, e *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		p.calls.Add(1)
		if !yield(sdk.NewSubmittedTask(e, e.Message), nil) {
			return
		}
		if e.Message.Parts[0].Text() == "wait" {
			<-ctx.Done()
			return
		}
		if !yield(sdk.NewArtifactEvent(e, sdk.NewTextPart("verified remote answer")), nil) {
			return
		}
		yield(sdk.NewStatusUpdateEvent(e, sdk.TaskStateCompleted, nil), nil)
	}
}
func (p serviceA2APeer) Cancel(_ context.Context, e *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		yield(sdk.NewStatusUpdateEvent(e, sdk.TaskStateCanceled, nil), nil)
	}
}
func TestA2ADelegationReservationCredentialsAndOwnerIsolation(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	if err := base.Repos.DB.Model(&model.User{}).Where("id=?", owner).Update("role", "admin").Error; err != nil {
		t.Fatal(err)
	}
	other := model.User{Username: "other-delegator", Email: "other-delegator@test.invalid", PasswordHash: "test", Role: "admin"}
	if err := base.Repos.DB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	mux := http.NewServeMux()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer public-delegation-fixture" {
			http.Error(w, "unauthorized", 401)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	defer peer.Close()
	card := &sdk.AgentCard{Name: "service-remote", Version: "1", SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(peer.URL+"/a2a", sdk.TransportProtocolJSONRPC)}}
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	mux.Handle("/a2a", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(serviceA2APeer{calls})))
	base.Cfg.A2A.Enabled = true
	base.Cfg.Outbound.PrivateOrigins = []string{peer.URL}
	svcs, err := New(base.Deps)
	if err != nil {
		t.Fatal(err)
	}
	admin := security.WithUserID(context.Background(), owner)
	foreign := security.WithUserID(context.Background(), other.ID)
	configured, err := svcs.A2A.Register(admin, &A2ARegisterInput{BaseURL: peer.URL, Headers: map[string]string{"Authorization": "Bearer public-delegation-fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := svcs.A2A.List(admin)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rows)
	if strings.Contains(string(raw), "public-delegation-fixture") || strings.Contains(string(raw), "encrypted_credentials") {
		t.Fatal("A2A management exposed credentials")
	}
	input := &A2ADelegateInput{Text: "inspect", RequestKey: "stable-request-1"}
	first, err := svcs.A2A.Delegate(admin, configured.ID, input)
	if err != nil || first.State != "completed" || first.ID == "" || first.Text != "verified remote answer" {
		t.Fatal("first delegation", first, err)
	}
	duplicate, err := svcs.A2A.Delegate(admin, configured.ID, input)
	if err != nil || duplicate.ID != first.ID || calls.Load() != 1 {
		t.Fatal("duplicate reservation resent remote task", duplicate, err, calls.Load())
	}
	input.Text = "changed"
	if _, err = svcs.A2A.Delegate(admin, configured.ID, input); err == nil {
		t.Fatal("request key reused for changed input")
	}
	if _, err = svcs.A2A.Delegation(foreign, first.ID, false); err == nil {
		t.Fatal("other admin read private task record")
	}
	if _, err = svcs.A2A.CancelDelegation(foreign, first.ID); err == nil {
		t.Fatal("other admin canceled private task")
	}
	waiting, err := svcs.A2A.Delegate(admin, configured.ID, &A2ADelegateInput{Text: "wait", RequestKey: "wait-request", ReturnImmediately: true})
	if err != nil || waiting.TaskID == "" {
		t.Fatal("pollable delegation", waiting, err)
	}
	canceled, err := svcs.A2A.CancelDelegation(admin, waiting.ID)
	if err != nil || canceled.State != "canceled" {
		t.Fatal("owned remote cancel", canceled, err)
	}
	resolved, err := svcs.A2A.Delegation(admin, waiting.ID, true)
	if err != nil || resolved.State != "canceled" {
		t.Fatal("task state after cancellation", resolved, err)
	}
	var count int64
	if err = svcs.Repos.DB.Table("a2a_delegations").Where("owner_id=?", owner).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("missing durable delegation records", fmt.Sprint(count), err)
	}
	// Independent DB transactions race the same key; only the reservation winner
	// may perform the external effect, regardless of the response each loser sees.
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	before := calls.Load()
	for range 16 {
		wg.Go(func() {
			result, err := svcs.A2A.Delegate(admin, configured.ID, &A2ADelegateInput{Text: "concurrent", RequestKey: "concurrent-key"})
			if err != nil {
				errs <- err
				return
			}
			ids <- result.ID
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error("concurrent delegation", err)
	}
	var reservation string
	for id := range ids {
		if reservation == "" {
			reservation = id
		}
		if id != reservation {
			t.Fatal("same request created multiple reservations")
		}
	}
	if calls.Load() != before+1 {
		t.Fatal("same-key race duplicated remote effect", calls.Load(), before)
	}
	// Simulate reservations held by other service processes using real shared PG.
	for i := range 8 {
		if err = svcs.Repos.DB.Exec("INSERT INTO a2a_delegations(id,owner_id,agent_id,request_key,input_hash,config_version,snapshot,status) SELECT ?,owner_id,agent_id,?,input_hash,config_version,snapshot,'working' FROM a2a_delegations WHERE id=?", fmt.Sprintf("capacity-%d", i), fmt.Sprintf("capacity-key-%d", i), reservation).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err = svcs.A2A.Delegate(admin, configured.ID, &A2ADelegateInput{Text: "overload", RequestKey: "capacity-denied"}); err == nil {
		t.Fatal("shared owner capacity exceeded")
	}
	if calls.Load() != before+1 {
		t.Fatal("overload reached remote peer")
	}
	if err = svcs.Repos.DB.Exec("UPDATE a2a_delegations SET deadline_at=now()-interval '1 second' WHERE id='capacity-0'").Error; err != nil {
		t.Fatal(err)
	}
	if _, err = svcs.A2A.Delegate(admin, configured.ID, &A2ADelegateInput{Text: "reclaimed", RequestKey: "capacity-reclaimed"}); err != nil {
		t.Fatal("expired capacity was not reclaimed", err)
	}
	off := false
	if _, err = svcs.A2A.Update(admin, configured.ID, &A2AUpdateInput{ConfigVersion: configured.ConfigVersion, Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	if cached, err := svcs.A2A.Delegate(admin, configured.ID, &A2ADelegateInput{Text: "inspect", RequestKey: "stable-request-1"}); err != nil || cached.ID != first.ID {
		t.Fatal("configuration disable hid idempotent committed result", cached, err)
	}
}
