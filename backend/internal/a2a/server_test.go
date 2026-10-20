package a2a

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestA2AServerRejectsAnonymousExecution(t *testing.T) {
	var calls atomic.Int32
	server := NewServer(&AgentCard{Name: "auth-required"}, func(context.Context, string) (string, error) { calls.Add(1); return "private answer", nil })
	peer := httptest.NewServer(http.HandlerFunc(server.RPCHandler))
	defer peer.Close()
	response, err := http.Post(peer.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":"anonymous","method":"message/send","params":{"message":{"role":"user","parts":[{"type":"text","text":"read private data"}]}}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatal("anonymous A2A execution accepted", response.StatusCode, calls.Load())
	}
}
