package api

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

// Deliberately hold back AfterFunc's abort callback: the producer can observe
// cancellation and close its pipe before that asynchronous callback is scheduled.
// A canceled EOF must still close the TCP connection, not enter keep-alive idle.
func TestCanceledSSEEOFCannotRetainKeepAliveDuringShutdown(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter), server.WithIdleTimeout(time.Minute), server.WithWriteTimeout(0))
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = h.Shutdown(cleanup)
	})
	h.GET("/stream", func(_ context.Context, c *app.RequestContext) {
		pr, pw := io.Pipe()
		body := &sseBody{PipeReader: pr, ctx: root, conn: c.GetConn(), cancel: func() {}, timeout: time.Second, stop: func() bool { return true }}
		c.Response.Header.Set("Content-Type", "text/event-stream")
		c.Response.SetBodyStream(body, -1)
		go func() { defer pw.Close(); _, _ = io.WriteString(pw, ": ready\n\n"); <-root.Done() }()
	})
	done := make(chan error, 1)
	go func() { done <- h.Run() }()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	for until := time.Now().Add(time.Second); !h.IsRunning(); {
		if time.Now().After(until) {
			t.Fatal("listener did not start")
		}
		time.Sleep(time.Millisecond)
	}
	response, err := client.Get("http://" + ln.Addr().String() + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cancel()
	// EOF or truncated chunked body is allowed for a canceled stream. A successful
	// terminal is not being asserted; the connection must leave transport.active.
	_, _ = io.Copy(io.Discard, reader)
	response.Body.Close()
	shutdown, stop := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stop()
	err = h.Shutdown(shutdown)
	if err != nil || shutdown.Err() != nil {
		t.Errorf("canceled SSE retained keep-alive until shutdown deadline: err=%v ctx=%v", err, shutdown.Err())
	}
	transport.CloseIdleConnections()
	select {
	case err = <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("HTTP listener did not exit")
	}
}
