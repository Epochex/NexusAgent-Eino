package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	stdRuntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

type apiNoTools struct{}

func (apiNoTools) ListContext(context.Context, string) ([]llm.ToolDef, error) { return nil, nil }
func (apiNoTools) Invoke(context.Context, string, string, string) tool.ToolResult {
	return tool.ToolResult{Status: "error"}
}

type runnerStreamer struct {
	runner *runtime.EinoRunner
	model  *llm.ModelDescriptor
}

func (s runnerStreamer) Stream(ctx context.Context, _ int64, in *service.ChatInput) (<-chan agent.Event, error) {
	return s.runner.Run(ctx, &runtime.RunInput{RunID: "api-test", AgentName: "api-test", Messages: []llm.Message{{Role: llm.RoleUser, Content: in.Content}}, Primary: s.model})
}

func chatListener(t *testing.T, modelURL string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := runtime.NewEinoRunner(llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4}), apiNoTools{}, runtime.EinoConfig{StepTimeout: 5 * time.Second, RunTimeout: 10 * time.Second, SendTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{chat: runnerStreamer{runner: runner, model: &llm.ModelDescriptor{BaseURL: modelURL, ModelName: "stub"}}}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter), server.WithSenseClientDisconnection(stdRuntime.GOOS != "windows"), server.WithWriteTimeout(0))
	h.POST("/chat", handler.Chat)
	runDone := make(chan error, 1)
	go func() { runDone <- h.Run() }()
	deadline := time.Now().Add(time.Second)
	for !h.IsRunning() {
		if time.Now().After(deadline) {
			t.Fatal("Hertz did not start")
		}
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
		select {
		case <-runDone:
		case <-ctx.Done():
			t.Error("Hertz listener did not stop")
		}
	})
	return "http://" + ln.Addr().String() + "/chat"
}

func TestChatSSEFields(t *testing.T) {
	model := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	url := chatListener(t, model.URL)
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 3 * time.Second, Transport: transport}
	resp, err := client.Post(url, "application/json", strings.NewReader(`{"content":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	var last uint64
	foundDone := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: done") {
			foundDone = true
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var data map[string]any
		if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data); err != nil {
			t.Fatal(err)
		}
		seq, ok := data["seq"].(float64)
		if !ok || uint64(seq) <= last || data["run_id"] != "api-test" || data["time"] == nil {
			t.Fatalf("SSE identity/order missing: %+v", data)
		}
		last = uint64(seq)
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !foundDone {
		t.Fatal("SSE lost terminal event")
	}
}

func TestChatDisconnectCancelsModel(t *testing.T) {
	closed := make(chan struct{})
	model := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	})
	url := chatListener(t, model.URL)
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	resp, err := client.Post(url, "application/json", strings.NewReader(`{"content":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("HTTP disconnect left model request running")
	}
}
