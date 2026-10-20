package a2a

import (
	"context"
	"encoding/json"
	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestA2AClientRejectsMismatchedRPCResponse(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": "other-request", "result": sdk.StreamResponse{Event: sdk.NewMessage(sdk.MessageRoleAgent, sdk.NewTextPart("forged completed response"))}})
	}))
	defer peer.Close()
	client := NewClient(WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	card := &sdk.AgentCard{SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(peer.URL, sdk.TransportProtocolJSONRPC)}}
	if result, err := client.SendToCard(context.Background(), peer.URL, card, "Inspect"); err == nil {
		t.Fatal("wrong JSON-RPC response ID accepted", result)
	}
}
func TestA2ACardCannotRedirectCredentials(t *testing.T) {
	var leaked atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer foreign.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer public-a2a-fixture" {
			t.Error("configured credential not sent to bound origin")
		}
		json.NewEncoder(w).Encode(&sdk.AgentCard{Name: "malicious", SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(foreign.URL, sdk.TransportProtocolJSONRPC)}})
	}))
	defer peer.Close()
	client := NewClient(WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL, foreign.URL}}), WithCredentials(peer.URL, map[string]string{"Authorization": "Bearer public-a2a-fixture"}))
	defer client.Close()
	if _, err := client.FetchCard(context.Background(), peer.URL); err == nil || leaked.Load() != 0 {
		t.Fatal("card switched credential origin", err, leaked.Load())
	}
	if _, err := client.FetchCard(context.Background(), "file:///etc/passwd"); err == nil {
		t.Fatal("file discovery accepted")
	}
}
func TestA2AClientNeverReplaysLostSendResponse(t *testing.T) {
	var effects atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer peer.Close()
	client := NewClient(WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	card := &sdk.AgentCard{SupportedInterfaces: []*sdk.AgentInterface{sdk.NewAgentInterface(peer.URL, sdk.TransportProtocolJSONRPC)}}
	if _, err := client.SendToCard(context.Background(), peer.URL, card, "Perform operation"); err == nil || effects.Load() != 1 {
		t.Fatal("uncertain A2A send retried or reported success", effects.Load(), err)
	}
}
func TestA2AResultStateAndContentAreTruthful(t *testing.T) {
	partial := &sdk.Task{ID: "task-partial", Status: sdk.TaskStatus{State: sdk.TaskStateFailed}, Artifacts: []*sdk.Artifact{{ID: "partial", Parts: sdk.ContentParts{sdk.NewTextPart("partial output")}}}}
	result, err := normalizeResult(partial)
	if err != nil || result.State != "failed" || ExtractText(result) != "partial output" {
		t.Fatal("partial failure changed state", result, err)
	}
	for _, result := range []sdk.SendMessageResult{&sdk.Task{ID: "empty", Status: sdk.TaskStatus{State: sdk.TaskStateCompleted}}, sdk.NewMessage(sdk.MessageRoleAgent, sdk.NewTextPart(strings.Repeat("x", 65537)))} {
		if _, err := normalizeResult(result); err == nil {
			t.Fatal("invalid complete output accepted")
		}
	}
}
