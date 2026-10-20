//go:build integration

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

func TestProviderCapacitySharedAcrossServiceInstances(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	actors := []int64{owner}
	for i := 1; i < 6; i++ {
		user := model.User{Username: fmt.Sprintf("provider-owner-%d", i), Email: fmt.Sprintf("provider-%d@test.invalid", i), PasswordHash: "public-test-only"}
		if err := base.Repos.DB.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
		actors = append(actors, user.ID)
	}
	var active, maximum, finished, failed atomic.Int32
	release := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		now := active.Add(1)
		defer active.Add(-1)
		for {
			before := maximum.Load()
			if before >= now || maximum.CompareAndSwap(before, now) {
				break
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"provider capacity verified"},"finish_reason":"stop"}]}`)
	}))
	defer peer.Close()
	defer peer.CloseClientConnections()
	cfg := &config.Config{LLM: config.LLM{MaxConnsPerProvider: 32, CircuitErrorRate: 1}}
	nodes := make([]*Services, 2)
	for i := range nodes {
		deps := base.Deps
		deps.Cfg = cfg
		deps.Gateway = llm.NewGateway(&cfg.LLM)
		var err error
		nodes[i], err = New(deps)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Go(func() {
			defer finished.Add(1)
			actor := actors[i/4]
			_, _, err := nodes[i%2].Gateway.Chat(security.WithUserID(ctx, actor), &llm.ModelDescriptor{ProviderCode: "shared-provider", BaseURL: peer.URL, ModelName: "controlled", ContextWindow: 8192, MaxTokens: 1024}, nil, &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "hold actual call"}}})
			if err != nil {
				failed.Add(1)
			}
		})
	}
	for deadline := time.Now().Add(3 * time.Second); active.Load()+finished.Load() < 24 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if maximum.Load() > 16 {
		t.Errorf("provider capacity was multiplied by service instances: active=%d", maximum.Load())
	}
	if active.Load() != 16 || failed.Load() != 8 {
		t.Errorf("want16 in-flight and8 explicit overloads, active=%d failed=%d finished=%d", active.Load(), failed.Load(), finished.Load())
	}
	close(release)
	wg.Wait()
	if finished.Load() != 24 {
		t.Fatal("provider callers leaked", finished.Load())
	}
}

func TestProviderLeaseLossCancelsActualHTTP(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	entered, stopped := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(security.WithUserID(context.Background(), owner), 20*time.Second)
	defer cancel()
	deps := base.Deps
	deps.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4, CircuitErrorRate: 1})
	svcs, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := svcs.Gateway.Chat(ctx, &llm.ModelDescriptor{ProviderCode: "lease-loss-peer", BaseURL: peer.URL, ModelName: "hold"}, nil, &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "hold until authority loss"}}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider call not started")
	}
	sqlDB, err := base.Repos.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err = sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err == nil {
			t.Error("lost authority reported model success")
		}
	case <-time.After(8 * time.Second):
		t.Error("provider kept running after shared lease renewal became impossible")
		cancel()
		<-done
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Error("lost lease did not close actual provider HTTP")
	}
}

func TestProviderStreamLeaseLossIsTerminalError(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer peer.Close()
	if err := base.Repos.DB.Exec("UPDATE execution_capacity_policies SET lease_ms=1000,version=version+1 WHERE scope='provider'").Error; err != nil {
		t.Fatal(err)
	}
	deps := base.Deps
	deps.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4, CircuitErrorRate: 1})
	svcs, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(security.WithUserID(context.Background(), owner), 4*time.Second)
	defer cancel()
	events, _, err := svcs.Gateway.ChatStream(ctx, &llm.ModelDescriptor{ProviderCode: "stream-lease", BaseURL: peer.URL, ModelName: "controlled"}, nil, &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "partial until lease expires"}}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case first := <-events:
		if first.Content != "partial" {
			t.Fatal("partial stream did not start", first)
		}
	case <-ctx.Done():
		t.Fatal("first chunk missing")
	}
	if err = base.Repos.DB.Exec("UPDATE execution_capacity_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='provider'").Error; err != nil {
		t.Fatal(err)
	}
	gotError := false
	for {
		select {
		case event, ok := <-events:
			if !ok {
				if !gotError {
					t.Error("lease loss silently closed partial model stream")
				}
				return
			}
			if event.Err != nil {
				gotError = true
			}
			if event.FinishReason == "stop" {
				t.Error("expired provider stream reported successful completion")
			}
		case <-ctx.Done():
			t.Fatal("expired provider stream retained its caller")
		}
	}
}

func TestProviderLeaseLossReleasesBackpressuredStreamSlot(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	firstStopped := make(chan struct{})
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		call := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		for range 80 {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		if call == 1 {
			close(firstStopped)
		}
	}))
	defer peer.Close()
	if err := base.Repos.DB.Exec("UPDATE execution_capacity_policies SET lease_ms=1000,version=version+1 WHERE scope='provider'").Error; err != nil {
		t.Fatal(err)
	}
	deps := base.Deps
	deps.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 1, CircuitErrorRate: 1})
	svcs, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(security.WithUserID(context.Background(), owner), 8*time.Second)
	defer cancel()
	desc := &llm.ModelDescriptor{ProviderCode: "slow-stream", BaseURL: peer.URL, ModelName: "controlled"}
	request := &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "fill bounded output then stop reading"}}}
	events, _, err := svcs.Gateway.ChatStream(ctx, desc, nil, request)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); len(events) < cap(events) && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if len(events) != cap(events) {
		t.Fatal("fixture did not backpressure the actual stream")
	}
	if err = base.Repos.DB.Exec("UPDATE execution_capacity_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE scope='provider'").Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("expired lease did not stop remote HTTP")
	}
	other, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	_, _, err = svcs.Gateway.ChatStream(other, desc, nil, request)
	if err != nil || calls.Load() != 2 {
		t.Errorf("backpressured expired stream retained local provider slot: calls=%d err=%v", calls.Load(), err)
	}
	stop()
	gotError := false
	for event := range events {
		if event.Err != nil {
			gotError = true
		}
	}
	if !gotError {
		t.Error("full buffer hid terminal admission error")
	}
}
