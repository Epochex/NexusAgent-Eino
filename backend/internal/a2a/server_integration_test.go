//go:build integration

package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
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
	var executions atomic.Int32
	adapter := NewServer(card, func(ctx context.Context, text string) (string, error) {
		executions.Add(1)
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
	t.Run("unsupported version does not invoke Agent", func(t *testing.T) {
		pair, err := jwt.Generate(users[0].ID, "user", users[0].Username)
		if err != nil {
			t.Fatal(err)
		}
		body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"role":"ROLE_USER","parts":[{"text":"reject unsupported version"}],"messageId":"version-no-execution"}}}`
		req, err := http.NewRequest(http.MethodPost, base+"/a2a", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+pair.AccessToken)
		req.Header.Set("A2A-Version", "99.0")
		before := executions.Load()
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result struct {
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result.Error == nil || result.Error.Code != -32009 || executions.Load() != before {
			t.Fatalf("unsupported version returned %+v; Agent calls before=%d after=%d", result.Error, before, executions.Load())
		}
	})
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
	for _, tc := range []struct {
		name    string
		id      sdk.TaskID
		foreign bool
		want    error
	}{
		{"unknown reference", "missing-message-reference", false, sdk.ErrTaskNotFound},
		{"foreign reference", sdk.TaskID(completed.ID), true, sdk.ErrTaskNotFound},
		{"owned reference remains unsupported", sdk.TaskID(completed.ID), false, sdk.ErrUnsupportedOperation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := sdk.NewMessage(sdk.MessageRoleUser, sdk.NewTextPart("continue task"))
			message.TaskID = tc.id
			client := owner
			if tc.foreign {
				client = foreign
			}
			if _, err := client.SendMessage(root, &sdk.SendMessageRequest{Message: message}); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
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
