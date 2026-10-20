//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

func TestWorkflowHTTPDurableLifecycle(t *testing.T) {
	db := testpg.Open(t)
	owner := model.User{Username: "http-owner", Email: "http@test.invalid", PasswordHash: "test"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	svcs := &service.Services{Deps: service.Deps{Repos: repo.New(db), Cfg: &config.Config{}}}
	svcs.Workflow = service.NewWorkflowService(svcs)
	flow := &model.Workflow{Name: "HTTP", CreatorID: owner.ID, DSL: "name: HTTP\ninput: [ticket]\nnodes: [{key: review, type: human, prompt: '{{ticket}}'}]"}
	if err := svcs.Workflow.Create(context.Background(), flow); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter), server.WithWriteTimeout(0), server.WithSenseClientDisconnection(runtime.GOOS != "windows"))
	handler := New(svcs)
	h.Use(func(ctx context.Context, c *app.RequestContext) {
		id := owner.ID
		if string(c.GetHeader("X-Test-Foreign")) != "" {
			id++
		}
		c.Set(claimsKey, &userClaims{UserID: id})
		c.Next(ctx)
	})
	h.POST("/runs", handler.RunWorkflow)
	h.GET("/runs/:id", handler.GetRun)
	h.POST("/runs/:id/approve", handler.ApproveRun)
	h.POST("/runs/:id/cancel", handler.CancelRun)
	h.GET("/runs/:id/events", handler.RunEvents)
	stopped := make(chan error, 1)
	go func() { stopped <- h.Run() }()
	for deadline := time.Now().Add(time.Second); !h.IsRunning(); {
		if time.Now().After(deadline) {
			t.Fatal("listener startup")
		}
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Error("listener shutdown")
		}
	})
	base := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{}}
	// This fixture owns its keep-alive pool. Close idle test connections before
	// shutting down Hertz; otherwise transport.Shutdown waits for idle timeout.
	t.Cleanup(client.CloseIdleConnections)
	request := func(method, path, body string, foreign bool) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if foreign {
			req.Header.Set("X-Test-Foreign", "1")
		}
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		data, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
		}
		return resp.StatusCode, data
	}
	body := fmt.Sprintf(`{"workflow_id":%d,"request_key":"http-retry","input":{"ticket":9007199254740993}}`, flow.ID)
	status, raw := request("POST", "/runs", body, false)
	var result struct {
		Data model.WorkflowRun `json:"data"`
	}
	if status != 202 {
		t.Fatalf("submit status=%d %s", status, raw)
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	runID := result.Data.ID
	if result.Data.Status != execution.Queued || !bytes.Contains(raw, []byte("9007199254740993")) {
		t.Fatalf("not durable/numeric corruption: %s", raw)
	}
	_, again := request("POST", "/runs", body, false)
	json.Unmarshal(again, &result)
	if result.Data.ID != runID {
		t.Fatal("duplicate request created another job")
	}
	path := fmt.Sprintf("/runs/%d", runID)
	if code, _ := request("GET", path, "", true); code != 404 {
		t.Fatalf("foreign read %d", code)
	}
	store := execution.NewStore(db)
	claimed, err := store.Claim(context.Background(), "http-worker", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim %v", err)
	}
	if err = svcs.Workflow.Driver().Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	_, raw = request("GET", path, "", false)
	json.Unmarshal(raw, &result)
	if result.Data.Status != execution.Waiting || len(result.Data.Approvals) != 1 {
		t.Fatalf("missing approval %s", raw)
	}
	approval := result.Data.Approvals[0]
	decision, _ := json.Marshal(map[string]any{"interrupt_id": approval.InterruptID, "args_hash": "stale", "approved": true})
	if code, _ := request("POST", path+"/approve", string(decision), false); code != 409 {
		t.Fatalf("stale approval %d", code)
	}
	decision, _ = json.Marshal(map[string]any{"interrupt_id": approval.InterruptID, "args_hash": approval.ArgsHash, "approved": true, "comment": "verified ticket"})
	if code, data := request("POST", path+"/approve", string(decision), false); code != 200 {
		t.Fatalf("approve %d %s", code, data)
	}
	status, raw = request("GET", path+"/events?after=0", "", false)
	if status != 200 || !bytes.Contains(raw, []byte("verified ticket")) {
		t.Fatalf("audit %d %s", status, raw)
	}
	// Closing a subscriber never owns or cancels the durable job.
	req, _ := http.NewRequest("GET", base+path+"/events?after=0", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_, err = resp.Body.Read(buf)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, raw = request("GET", path, "", false)
	json.Unmarshal(raw, &result)
	if result.Data.Status != execution.Queued {
		t.Fatalf("subscriber canceled job %s", raw)
	}
	status, raw = request("POST", path+"/cancel", "{}", false)
	if status != 200 {
		t.Fatalf("cancel %s", raw)
	}
	json.Unmarshal(raw, &result)
	if result.Data.Status != execution.Canceled {
		t.Fatalf("cancel state %s", raw)
	}
}
