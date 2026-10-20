package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	stdRuntime "runtime"
	"strings"
	"sync"
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
	return startChatListener(t, ln, handler)
}

func startChatListener(t *testing.T, ln net.Listener, handler *Handler) string {
	t.Helper()
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

type stalledListener struct {
	net.Listener
	closed chan struct{}
}
type trackedTCP struct {
	*net.TCPConn
	once   sync.Once
	closed chan struct{}
}

func (c *trackedTCP) Close() error { c.once.Do(func() { close(c.closed) }); return c.TCPConn.Close() }
func (l stalledListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tcp := c.(*net.TCPConn)
	_ = tcp.SetWriteBuffer(1024)
	return &trackedTCP{TCPConn: tcp, closed: l.closed}, nil
}

func TestChatTCPStoppedReaderReleasesConnection(t *testing.T) {
	modelClosed := make(chan struct{})
	model := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		frame, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]string{"content": strings.Repeat("x", 256<<10)}}}})
		for range 12 {
			fmt.Fprintf(w, "data: %s\n\n", frame)
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(modelClosed)
	})
	runner, err := runtime.NewEinoRunner(llm.NewGateway(&config.LLM{MaxConnsPerProvider: 1}), apiNoTools{}, runtime.EinoConfig{StepTimeout: 10 * time.Second, RunTimeout: 12 * time.Second, SendTimeout: 100 * time.Millisecond, MaxModelOutputBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverClosed := make(chan struct{})
	url := startChatListener(t, stalledListener{Listener: ln, closed: serverClosed}, &Handler{chat: runnerStreamer{runner: runner, model: &llm.ModelDescriptor{BaseURL: model.URL, ModelName: "stub"}}})
	conn, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadBuffer(1024)
	body := `{"content":"hello"}`
	_, err = fmt.Fprintf(conn, "POST /chat HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", strings.TrimPrefix(url, "http://"), len(body), body)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the TCP connection open without reading even the response headers.
	select {
	case <-serverClosed:
	case <-time.After(4 * time.Second):
		t.Error("stalled TCP writer retained its connection beyond the write bound")
	}
	_ = conn.Close() // also bounds cleanup for the RED run
	select {
	case <-modelClosed:
	case <-time.After(time.Second):
		t.Fatal("stalled TCP client retained model connection/permit")
	}
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

func TestChatIdleModelOutlivesSocketWriteBound(t *testing.T) {
	model := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(2200 * time.Millisecond)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"last\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	})
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	resp, err := client.Post(chatListener(t, model.URL), "application/json", strings.NewReader(`{"content":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || !strings.Contains(string(body), "event: done") || !strings.Contains(string(body), "last") {
		t.Fatalf("idle model was mistaken for blocked writing: %s %v", body, err)
	}
}
