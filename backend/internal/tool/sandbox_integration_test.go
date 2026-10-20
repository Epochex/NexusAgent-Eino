//go:build integration

package tool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
)

func realSandbox(t *testing.T, memory, cpu string, timeout time.Duration) *Sandbox {
	t.Helper()
	image := os.Getenv("NX_TEST_SANDBOX_IMAGE")
	if image == "" {
		t.Skip("explicit preloaded NX_TEST_SANDBOX_IMAGE and owned Docker daemon required")
	}
	s, err := NewSandbox(SandboxConfig{Image: image, Memory: memory, CPUs: cpu, Timeout: timeout, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func TestSandboxContainerIsolationOutputOOMAndRecovery(t *testing.T) {
	s := realSandbox(t, "128m", "0.5", 5*time.Second)
	t.Setenv("NX_SANDBOX_SHOULD_NOT_LEAK", "public-test-marker")
	code := `import os,json,socket
def cg(name):
 return open('/sys/fs/cgroup/'+name).read().strip()
status=open('/proc/self/status').read().splitlines()
values={line.split(':')[0]:line.split(':')[1].strip() for line in status if ':' in line}
sock=socket.socket();sock.settimeout(.2)
print(json.dumps(dict(uid=os.geteuid(),readonly=bool(os.statvfs('/').f_flag&os.ST_RDONLY),caps=values['CapEff'],nnp=values['NoNewPrivs'],memory=cg('memory.max'),swap=cg('memory.swap.max'),cpu=cg('cpu.max'),pids=cg('pids.max'),socket=os.path.exists('/var/run/docker.sock'),leaked=os.getenv('NX_SANDBOX_SHOULD_NOT_LEAK',''),network=sock.connect_ex(('198.18.0.1',9)))))`
	result, err := s.Run(context.Background(), "python", code)
	if err != nil || result.ExitCode != 0 {
		t.Fatal("actual isolation program", result, err)
	}
	var got struct {
		UID                                int
		Readonly                           bool
		Caps, NNP, Memory, Swap, CPU, Pids string
		Socket                             bool
		Leaked                             string
		Network                            int
	}
	if json.Unmarshal([]byte(result.Stdout), &got) != nil || got.UID != 65532 || !got.Readonly || got.Caps != "0000000000000000" || got.NNP != "1" || got.Memory != "134217728" || got.Swap != "0" || got.CPU != "50000 100000" || got.Pids != "64" || got.Socket || got.Leaked != "" || got.Network == 0 {
		t.Fatalf("isolation contract %+v output=%s", got, result.Stdout)
	}
	t.Log("actual kernel restrictions:", result.Stdout)
	result, err = s.Run(context.Background(), "python", `import sys;sys.stdout.write('x'*1048576);sys.stderr.write('y'*1048576)`)
	if err != nil || result.ExitCode != 0 || len(result.Stdout) > 65560 || len(result.Stderr) > 65560 || !strings.Contains(result.Stdout, "truncated") || !strings.Contains(result.Stderr, "truncated") {
		t.Fatal("bounded actual stdout/stderr", err)
	}
	result, err = s.Run(context.Background(), "python", `a=bytearray(512*1024*1024);print(len(a))`)
	if err != nil || result == nil || !result.OOMKilled || result.ExitCode == 0 {
		t.Fatal("hard Docker OOM was not observed", result, err)
	}
	result, err = s.Run(context.Background(), "shell", "printf recovered")
	if err != nil || result.ExitCode != 0 || result.Stdout != "recovered" {
		t.Fatal("capacity did not recover after OOM", result, err)
	}
}
func TestSandboxConcurrentLimitCancellationAndWatchdog(t *testing.T) {
	s := realSandbox(t, "128m", "0.5", 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Run(ctx, "python", "import time;time.sleep(30)"); done <- err }()
	deadline := time.Now().Add(3 * time.Second)
	running := false
	for time.Now().Before(deadline) {
		rows, err := s.cli.ContainerList(context.Background(), container.ListOptions{Filters: filters.NewArgs(filters.Arg("label", "nexus.component=bounded-code-exec"))})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 {
			running = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !running {
		t.Fatal("owned sandbox did not start")
	}
	if _, err := s.Run(context.Background(), "shell", "printf should-not-run"); !errors.Is(err, ErrSandboxBusy) {
		t.Fatal("local sandbox pool ignored", err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled execution succeeded")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("canceled container did not stop")
	}
	result, err := s.Run(context.Background(), "shell", "printf slot-reused")
	if err != nil || result.ExitCode != 0 || result.Stdout != "slot-reused" {
		t.Fatal("canceled slot not reusable", result, err)
	}
	short := realSandbox(t, "128m", "0.5", time.Second)
	start := time.Now()
	if _, err = short.Run(context.Background(), "python", "while True: pass"); err == nil || time.Since(start) > 7*time.Second {
		t.Fatal("CPU deadline not bounded", err, time.Since(start))
	}
	rows, err := s.cli.ContainerList(context.Background(), container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("label", "nexus.component=bounded-code-exec"))})
	if err != nil || len(rows) != 0 {
		t.Fatal("owned execution containers leaked", len(rows), err)
	}
}
func TestSandboxActualGoProgram(t *testing.T) {
	s := realSandbox(t, "512m", "1", time.Minute)
	result, err := s.Run(context.Background(), "go", "package main\nimport \"fmt\"\nfunc main(){fmt.Print(\"actual-go\")}")
	if err != nil || result.ExitCode != 0 || result.Stdout != "actual-go" {
		t.Fatal("actual Go interpreter image", result, err)
	}
}
func TestCodeExecAuditFailureIsNotReportedAsSuccess(t *testing.T) {
	s := realSandbox(t, "128m", "0.5", 5*time.Second)
	db := testpg.Open(t)
	if err := db.Exec("DROP TABLE sandbox_executions").Error; err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	RegisterCodeExec(reg, s, db)
	handler, _ := reg.Get("code_exec")
	output, err := handler(security.WithUserID(context.Background(), 1), map[string]any{"language": "shell", "code": "printf actual-result"}, "")
	if err == nil || output != "" || !strings.Contains(err.Error(), "audit") {
		t.Fatal("uncommitted audit produced success", output, err)
	}
}
