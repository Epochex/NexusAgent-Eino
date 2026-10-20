//go:build integration

package api

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

func TestCapacityPolicyHTTPVersionAndLiveAuthority(t *testing.T) {
	db := testpg.Open(t)
	admin := model.User{Username: "capacity-admin", Email: "capacity@test.invalid", PasswordHash: "public-test-only", Role: "admin"}
	if err := db.Create(&admin).Error; err != nil {
		t.Fatal(err)
	}
	jwt := security.NewJWTManager("capacity-public-test-only-jwt-secret", time.Hour, time.Hour)
	svcs, err := service.New(service.Deps{Cfg: &config.Config{}, Repos: repo.New(db), JWT: jwt})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter))
	Register(h.Group("/", Recovery()), New(svcs), jwt)
	done := make(chan error, 1)
	go func() { done <- h.Run() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("capacity HTTP did not drain")
		}
	})
	for until := time.Now().Add(time.Second); !h.IsRunning(); {
		if time.Now().After(until) {
			t.Fatal("capacity HTTP startup")
		}
		time.Sleep(time.Millisecond)
	}
	client := &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)
	pair, err := jwt.Generate(admin.ID, "admin", admin.Username)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, suffix, body string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, "http://"+ln.Addr().String()+"/api/v1/admin/capacity"+suffix, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+pair.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, raw
	}
	if status, raw := request("GET", "", ""); status != 200 || !strings.Contains(string(raw), "owner_active_limit") {
		t.Fatalf("shared policies unavailable: status=%d body=%s", status, raw)
	}
	// Existing work survives a shrink; all new callers see the shared policy.
	_, release, err := svcs.Capacity.HoldRun(context.Background(), admin.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	body := `{"version":1,"active_limit":1,"owner_active_limit":1,"queue_limit":30,"owner_queue_limit":10,"lease_ms":2000}`
	if status, raw := request("PUT", "/run", body); status != 200 {
		t.Fatalf("policy update %d %s", status, raw)
	}
	if status, _ := request("PUT", "/run", body); status != 409 {
		t.Fatalf("stale update status=%d", status)
	}
	if status, _ := request("PUT", "/run", strings.Replace(body, `"active_limit":1`, `"active_limit":0`, 1)); status != 400 {
		t.Fatalf("invalid limits status=%d", status)
	}
	if _, releaseOther, err := capacity.New(db).HoldRun(context.Background(), admin.ID); err == nil {
		releaseOther()
		t.Error("another node ignored the shared shrink")
	}
	status, raw := request("GET", "", "")
	var result struct {
		Data struct {
			Policies []struct {
				Scope   string
				Version int64
			}
			ActiveRuns int64 `json:"active_runs"`
		}
	}
	if status != 200 || json.Unmarshal(raw, &result) != nil || result.Data.ActiveRuns != 1 {
		t.Fatalf("DB usage snapshot %d %s", status, raw)
	}
	if err := db.Model(&admin).Update("role", "user").Error; err != nil {
		t.Fatal(err)
	}
	if status, _ := request("GET", "", ""); status != 403 {
		t.Fatalf("old admin JWT retained policy access: %d", status)
	}
}
