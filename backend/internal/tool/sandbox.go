// Optional code execution on an operator-owned Docker daemon. Production must
// use a dedicated authenticated daemon; its credentials grant daemon authority.
package tool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-units"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type SandboxConfig struct {
	Image, Memory, CPUs string
	Timeout             time.Duration
	PoolSize            int
	RequireRemote       bool
}
type Sandbox struct {
	cli         *client.Client
	cfg         SandboxConfig
	slots       chan struct{}
	memory, cpu int64
}

var ErrSandboxBusy = errors.New("sandbox capacity exhausted")

func immutableSandboxImage(image string) bool {
	digest := image
	if at := strings.LastIndex(image, "@sha256:"); at >= 0 {
		digest = image[at+1:]
	}
	if !strings.HasPrefix(digest, "sha256:") {
		return false
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	return err == nil && len(raw) == 32
}
func NewSandbox(cfg SandboxConfig) (*Sandbox, error) {
	memory, err := units.RAMInBytes(cfg.Memory)
	if err != nil || memory < 64<<20 || memory > 2<<30 {
		return nil, fmt.Errorf("sandbox memory must be 64MiB..2GiB")
	}
	cpu, err := strconv.ParseFloat(cfg.CPUs, 64)
	if err != nil || math.IsNaN(cpu) || math.IsInf(cpu, 0) || cpu < .1 || cpu > 4 {
		return nil, fmt.Errorf("sandbox CPU must be 0.1..4")
	}
	if cfg.Timeout < 100*time.Millisecond || cfg.Timeout > time.Minute || cfg.PoolSize < 1 || cfg.PoolSize > 8 || !immutableSandboxImage(cfg.Image) {
		return nil, fmt.Errorf("sandbox requires immutable image digest, 100ms..1m timeout and 1..8 local slots")
	}
	if cfg.RequireRemote && (!strings.HasPrefix(os.Getenv("DOCKER_HOST"), "tcp://") || os.Getenv("DOCKER_TLS_VERIFY") != "1" || os.Getenv("DOCKER_CERT_PATH") == "") {
		return nil, fmt.Errorf("production sandbox requires dedicated TLS Docker endpoint, not the application host socket")
	}
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("sandbox Docker client unavailable")
	}
	check, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, _, err = cli.ImageInspectWithRaw(check, cfg.Image); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("preloaded sandbox image or Docker endpoint unavailable")
	}
	return &Sandbox{cli: cli, cfg: cfg, slots: make(chan struct{}, cfg.PoolSize), memory: memory, cpu: int64(cpu * 1e9)}, nil
}
func (s *Sandbox) Close() error { return s.cli.Close() }

type ExecResult struct {
	Stdout, Stderr string
	ExitCode       int
	Duration       time.Duration
	OOMKilled      bool
}

// Continue draining framed output while retaining at most 64KiB per channel.
type boundedLog struct {
	data      []byte
	truncated bool
}

func (w *boundedLog) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (64 << 10) - len(w.data)
	if remaining < len(p) {
		w.truncated = true
		p = p[:max(0, remaining)]
	}
	w.data = append(w.data, p...)
	return n, nil
}
func (w *boundedLog) String() string {
	text := strings.TrimSpace(strings.ToValidUTF8(string(w.data), ""))
	if w.truncated {
		text += "\n...[truncated]"
	}
	return text
}
func (s *Sandbox) Run(parent context.Context, language, code string) (result *ExecResult, returnedErr error) {
	if len(code) == 0 || len(code) > 64<<10 {
		return nil, fmt.Errorf("sandbox code must be 1..65536 bytes")
	}
	var command []string
	switch language {
	case "python":
		command = []string{"python3", "-c", code}
	case "shell":
		command = []string{"sh", "-c", code}
	case "go":
		command = []string{"sh", "-c", "umask 077; printf '%s' \"$1\" > /tmp/main.go; exec go run /tmp/main.go", "sandbox", code}
	default:
		return nil, fmt.Errorf("unsupported sandbox language")
	}
	// The in-container watchdog also bounds execution if Docker control is lost.
	command = append([]string{"timeout", "--signal=KILL", strconv.FormatFloat(s.cfg.Timeout.Seconds(), 'f', 3, 64) + "s"}, command...)
	select {
	case <-parent.Done():
		return nil, parent.Err()
	case s.slots <- struct{}{}:
	default:
		return nil, ErrSandboxBusy
	}
	defer func() { <-s.slots }()
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, s.cfg.Timeout)
	defer cancel()
	name := "nexus-code-" + strings.ToLower(rand.Text())
	// A known name permits cleanup even if create commits but its response is lost.
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := s.cli.ContainerRemove(cleanup, name, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
			result = nil
			returnedErr = fmt.Errorf("sandbox cleanup incomplete; inspect the dedicated runtime")
		}
	}()
	response, err := s.cli.ContainerCreate(ctx, &container.Config{Image: s.cfg.Image, Cmd: command, WorkingDir: "/sandbox", User: "65532:65532", Env: []string{"PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "HOME=/tmp", "TMPDIR=/tmp", "GOCACHE=/tmp/gocache", "GOTMPDIR=/tmp", "GOFLAGS=-p=1", "GOPROXY=off", "GOTOOLCHAIN=local", "CGO_ENABLED=0", "GOMAXPROCS=1", "PYTHONDONTWRITEBYTECODE=1"}, Labels: map[string]string{"nexus.component": "bounded-code-exec"}}, &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, Tmpfs: map[string]string{"/tmp": "rw,exec,nosuid,nodev,size=128m,mode=1777"}, Resources: container.Resources{Memory: s.memory, MemorySwap: s.memory, NanoCPUs: s.cpu, PidsLimit: int64Ptr(64), Ulimits: []*units.Ulimit{{Name: "nofile", Soft: 256, Hard: 256}, {Name: "fsize", Soft: 32 << 20, Hard: 32 << 20}}}, LogConfig: container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "256k", "max-file": "1"}}}, nil, nil, name)
	if err != nil {
		return nil, fmt.Errorf("sandbox container create: %w", err)
	}
	id := response.ID
	// Attach before start: attaching to rotated logs after a fast process starts
	// can lose the entire stdout prefix even with bounded in-memory buffers.
	attached, err := s.cli.ContainerAttach(ctx, id, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return nil, fmt.Errorf("sandbox output attachment failed: %w", err)
	}
	defer attached.Close()
	var stdout, stderr boundedLog
	copied := make(chan error, 1)
	go func() { _, e := stdcopy.StdCopy(&stdout, &stderr, attached.Reader); copied <- e }()
	if err = s.cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("sandbox container start: %w", err)
	}
	status, failures := s.cli.ContainerWait(ctx, id, container.WaitConditionNotRunning)
	var exit int
	select {
	case result := <-status:
		if result.Error != nil {
			return nil, fmt.Errorf("sandbox wait failed")
		}
		exit = int(result.StatusCode)
	case <-failures:
		return nil, fmt.Errorf("sandbox wait failed")
	case <-ctx.Done():
		return nil, fmt.Errorf("sandbox execution canceled or exceeded deadline: %w", ctx.Err())
	}
	select {
	case err = <-copied:
		if err != nil {
			return nil, fmt.Errorf("sandbox output stream failed")
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	inspected, err := s.cli.ContainerInspect(ctx, id)
	if err != nil || inspected.State == nil {
		return nil, fmt.Errorf("sandbox result unavailable")
	}
	return &ExecResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exit, Duration: time.Since(started), OOMKilled: inspected.State.OOMKilled}, nil
}
func int64Ptr(v int64) *int64 { return &v }
func truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(s[:end]) {
		end--
	}
	return s[:end] + "\n...[truncated]"
}
