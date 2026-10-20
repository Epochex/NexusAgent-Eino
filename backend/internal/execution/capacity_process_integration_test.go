//go:build integration

package execution_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// Actual separate cmd/worker processes share the same policies and leases. The
// observer is an independent HTTP peer and SQL queries, not worker-local gauges.
func TestSharedCapacityCompetingWorkerProcesses(t *testing.T) {
	backend, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "capacity-worker")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = backend
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("actual worker build %v %s", err, out)
	}
	db, svcs, owner, dir, dsn := processFixture(t, backend)
	if err = db.Exec("UPDATE execution_capacity_policies SET active_limit=4,owner_active_limit=2,version=version+1").Error; err != nil {
		t.Fatal(err)
	}
	var calls, active, maximum atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Header.Get("Authorization") != "Bearer public-capacity-model-key" {
			http.Error(w, "wrong fixture credential", 401)
			return
		}
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for {
			before := maximum.Load()
			if before >= n || maximum.CompareAndSwap(before, n) {
				break
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"quota-process-verified"},"finish_reason":"stop"}]}`)
	}))
	defer peer.Close()
	defer releaseAll()
	cipher, err := svcs.Encryptor.Encrypt("public-capacity-model-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := model.ModelProvider{Code: "capacity-process", Name: "capacity-process", BaseURL: peer.URL, APIKey: cipher, Status: 1}
	if err = db.Create(&provider).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&model.ModelConfig{ProviderID: provider.ID, Alias: "fault-model", ModelName: "controlled", Type: model.ModelTypeChat, ContextWindow: 8192, MaxOutput: 1024, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	owners := []int64{owner}
	for i := 1; i < 3; i++ {
		user := model.User{Username: fmt.Sprintf("capacity-process-%d", i), Email: fmt.Sprintf("capacity-process-%d@test.invalid", i), PasswordHash: "public-test-only"}
		if err = db.Create(&user).Error; err != nil {
			t.Fatal(err)
		}
		owners = append(owners, user.ID)
	}
	for _, actor := range owners {
		flow := model.Workflow{Name: "process capacity", CreatorID: actor, DSL: "name: capacity\nnodes: [{key: answer,type: llm,prompt: verify capacity}]"}
		if err = svcs.Workflow.Create(context.Background(), &flow); err != nil {
			t.Fatal(err)
		}
		for i := range 6 {
			if _, err = svcs.Workflow.Run(context.Background(), actor, &service.RunInput{WorkflowID: flow.ID, RequestKey: fmt.Sprintf("capacity-process-%d", i)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	start := func() *ownedWorker {
		child := exec.Command(binary, "-queues", "execution", "-execution-concurrency", "4")
		child.Dir = dir
		child.Env = append(os.Environ(), "NX_PG__DSN="+dsn, "NX_SECRET_KEY="+processTestKey, "NX_ENV=dev", "NX_SANDBOX__ENABLED=false", "NX_OBSERVABILITY__OTEL_ENABLED=false")
		logs := &processLog{}
		child.Stdout = logs
		child.Stderr = logs
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		worker := &ownedWorker{cmd: child, done: make(chan error, 1), log: logs}
		go func() { worker.done <- child.Wait() }()
		t.Cleanup(worker.kill)
		return worker
	}
	first, second := start(), start()
	wait := func(timeout time.Duration, description string, ready func() bool) {
		t.Helper()
		for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
			if ready() {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("%s; active=%d calls=%d first=%s second=%s", description, active.Load(), calls.Load(), first.log.String(), second.log.String())
	}
	wait(12*time.Second, "four shared model calls were not admitted", func() bool { return active.Load() == 4 })
	var rows []struct {
		OwnerID int64
		Count   int64
	}
	if err = db.Table("workflow_runs").Select("owner_id,count(*) AS count").Where("status=? AND lease_until>clock_timestamp()", execution.Running).Group("owner_id").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatal("flooded owner starved another process owner", rows)
	}
	for _, row := range rows {
		if row.Count > 2 {
			t.Fatal("per-owner process quota exceeded", rows)
		}
	}
	var firstOwned int64
	if err = db.Table("workflow_runs").Where("status=? AND lease_owner LIKE ?", execution.Running, fmt.Sprintf("%%-%d-%%", first.cmd.Process.Pid)).Count(&firstOwned).Error; err != nil {
		t.Fatal(err)
	}
	victim := first
	if firstOwned == 0 {
		victim = second
	}
	before := calls.Load()
	victim.kill()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if calls.Load() != before {
			t.Fatal("replacement oversold still-live leases", before, calls.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	wait(23*time.Second, "surviving process did not reclaim expired capacity", func() bool { return active.Load() == 4 && calls.Load() > before })
	if maximum.Load() > 4 {
		t.Fatal("actual model concurrency exceeded shared process cap", maximum.Load())
	}
	releaseAll()
	wait(25*time.Second, "accepted workflow runs did not all reach committed completion", func() bool {
		var count int64
		return db.Table("workflow_runs").Where("status=?", execution.Succeeded).Count(&count).Error == nil && count == 18
	})
	var bad int64
	if err = db.Table("workflow_runs").Where("status=? AND output::text NOT LIKE ?", execution.Succeeded, "%quota-process-verified%").Count(&bad).Error; err != nil || bad != 0 {
		t.Fatal("committed model result mismatch", bad, err)
	}
	if maximum.Load() > 4 {
		t.Fatal("draining exceeded provider quota", maximum.Load())
	}
	t.Logf("two actual worker processes: offered18 completed18 model_calls%d peak%d; killed node recovered through shared lease expiry", calls.Load(), maximum.Load())
}
