//go:build integration

package a2a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
)

func TestA2AServerRealJWTTaskOwnershipCancellationAndFailure(t *testing.T) {
	db := testpg.Open(t)
	users := []model.User{{Username: "a2a-http-owner", Email: "owner@test.invalid", PasswordHash: "test"}, {Username: "a2a-http-other", Email: "other@test.invalid", PasswordHash: "test"}}
	for i := range users {
		if err := db.Create(&users[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	jwt := security.NewJWTManager("a2a-http-test-only-jwt-secret", time.Hour, time.Hour)
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := NewTaskStore(db, TaskOwner, 150*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	exited := make(chan struct{}, 1)
	peer := httptest.NewUnstartedServer(nil)
	base := "http://" + peer.Listener.Addr().String()
	card := &sdk.AgentCard{Name: "owner-aware-agent", Version: "1", SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(base+"/a2a", sdk.TransportProtocolJSONRPC)}}
	adapter := NewServer(card, func(ctx context.Context, text string) (string, error) {
		if id, ok := security.UserIDFromContext(ctx); !ok || id != users[0].ID {
			return "", fmt.Errorf("wrong execution owner")
		}
		switch text {
		case "wait":
			entered <- struct{}{}
			<-ctx.Done()
			exited <- struct{}{}
			return "late result must not complete canceled task", nil
		case "fail":
			return "partial output", fmt.Errorf("injected failure")
		}
		return "confirmed owner response", nil
	}, WithServerConfig(ServerConfig{Root: root, Store: store, RunTimeout: 5 * time.Second, Authenticate: func(ctx context.Context, token string) (int64, error) {
		claims, err := jwt.Parse(token, false)
		if err != nil {
			return 0, err
		}
		var user model.User
		if err = db.WithContext(ctx).Where("id=? AND status=1", claims.UserID).First(&user).Error; err != nil {
			return 0, err
		}
		return user.ID, nil
	}}))
	if err = adapter.ValidationError(); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	adapter.RegisterRoutes(mux)
	peer.Config.Handler = mux
	peer.Start()
	defer peer.Close()
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := adapter.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	clients := make([]*Client, 2)
	for i := range users {
		pair, err := jwt.Generate(users[i].ID, "user", users[i].Username)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = NewClient(WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{base}}), WithCredentials(base, map[string]string{"Authorization": "Bearer " + pair.AccessToken}))
		defer clients[i].Close()
	}
	owner, err := clients[0].protocol(root, base, card)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Destroy()
	foreign, err := clients[1].protocol(root, base, card)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Destroy()
	response, err := owner.SendMessage(root, &sdk.SendMessageRequest{Config: &sdk.SendMessageConfig{ReturnImmediately: true}, Message: sdk.NewMessage(sdk.MessageRoleUser, sdk.NewTextPart("wait"))})
	if err != nil {
		t.Fatal("async acceptance", err)
	}
	pending, ok := response.(*sdk.Task)
	if !ok || pending.ID == "" {
		t.Fatal("missing persisted task ID", response)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("authenticated Agent did not start")
	}
	if _, err = foreign.GetTask(root, &sdk.GetTaskRequest{ID: pending.ID}); !errors.Is(err, sdk.ErrTaskNotFound) {
		t.Fatal("foreign task read", err)
	}
	if _, err = foreign.CancelTask(root, &sdk.CancelTaskRequest{ID: pending.ID}); !errors.Is(err, sdk.ErrTaskNotFound) {
		t.Fatal("foreign cancel changed owner task", err)
	}
	stopped, err := owner.CancelTask(root, &sdk.CancelTaskRequest{ID: pending.ID})
	if err != nil || stopped.Status.State != sdk.TaskStateCanceled {
		t.Fatal("owner cancel", stopped, err)
	}
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not stop Agent")
	}
	current, err := owner.GetTask(root, &sdk.GetTaskRequest{ID: pending.ID})
	if err != nil || current.Status.State != sdk.TaskStateCanceled || len(current.Artifacts) != 0 {
		t.Fatal("late response revived canceled task", current, err)
	}
	failed, err := clients[0].SendToCard(root, base, card, "fail")
	if err != nil || failed.State != "failed" || ExtractText(failed) == "partial output" {
		t.Fatal("partial failure became completed result", failed, err)
	}
	completed, err := clients[0].SendToCard(root, base, card, "inspect")
	if err != nil || completed.State != "completed" || ExtractText(completed) != "confirmed owner response" {
		t.Fatal("authenticated result", completed, err)
	}
	if err = db.Model(&model.User{}).Where("id=?", users[0].ID).Update("status", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = owner.GetTask(root, &sdk.GetTaskRequest{ID: pending.ID}); err == nil {
		t.Fatal("disabled user retained JWT task access")
	}
	if err = db.Model(&model.User{}).Where("id=?", users[0].ID).Update("status", 1).Error; err != nil {
		t.Fatal(err)
	}
	response, err = owner.SendMessage(root, &sdk.SendMessageRequest{Config: &sdk.SendMessageConfig{ReturnImmediately: true}, Message: sdk.NewMessage(sdk.MessageRoleUser, sdk.NewTextPart("wait"))})
	if err != nil {
		t.Fatal(err)
	}
	closing := response.(*sdk.Task)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second execution not entered")
	}
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	if err = adapter.Close(shutdown); err != nil {
		stop()
		t.Fatal(err)
	}
	stop()
	var row struct{ State string }
	if err = db.Table("a2a_tasks").Where("id=?", string(closing.ID)).First(&row).Error; err != nil || row.State != string(sdk.TaskStateFailed) {
		t.Fatal("graceful close returned before terminal task commit", row, err)
	}
	t.Log("actual owner=" + strconv.FormatInt(users[0].ID, 10) + "; owner-scoped task, cancel fence and fresh JWT revocation verified")
}
