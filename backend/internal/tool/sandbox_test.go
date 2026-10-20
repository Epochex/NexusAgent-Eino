package tool

import (
	"testing"
	"time"
)

func TestSandboxRejectsUnboundedOrMutableConfiguration(t *testing.T) {
	base := SandboxConfig{Image: "python@sha256:2c285c669cc837aa3bcf1af23ea1932b7b5214f9c9d3aad22417446ad91cb4fb", Memory: "128m", CPUs: "0.5", PoolSize: 1, Timeout: time.Second}
	cases := map[string]func(*SandboxConfig){"memory": func(c *SandboxConfig) { c.Memory = "0" }, "cpu": func(c *SandboxConfig) { c.CPUs = "NaN" }, "pool": func(c *SandboxConfig) { c.PoolSize = 0 }, "timeout": func(c *SandboxConfig) { c.Timeout = 0 }, "mutable-image": func(c *SandboxConfig) { c.Image = "nexus/sandbox:latest" }}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mutate(&cfg)
			if _, err := NewSandbox(cfg); err == nil {
				t.Fatal("accepted unbounded or mutable sandbox configuration")
			}
		})
	}
}

func TestProductionSandboxRejectsHostSocketWithoutRemoteTLS(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	_, err := NewSandbox(SandboxConfig{Image: "sha256:c00dbd3d8f46605de59fab4962166e83df67cd94a4a5ea4307a77bbe8d5c3825", Memory: "128m", CPUs: "0.5", PoolSize: 1, Timeout: time.Second, RequireRemote: true})
	if err == nil {
		t.Fatal("production accepted the application host Docker socket")
	}
}
