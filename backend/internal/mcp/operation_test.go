package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/pkg/callmeta"
)

func TestMCPPropagatesOperationIDOnlyToToolCall(t *testing.T) {
	var calls int
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Method == "tools/call" {
			calls++
			if r.Header.Get("Idempotency-Key") != "stable-operation-id" {
				t.Errorf("missing stable operation key: %+v", r.Header)
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"content":[{"type":"text","text":"confirmed"}]}}`, request.ID)
		} else {
			if r.Header.Get("Idempotency-Key") != "" {
				t.Error("tool key attached to initialization")
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"protocolVersion":"2025-03-26","serverInfo":{"name":"test","version":"1"}}}`, request.ID)
		}
	}))
	defer peer.Close()
	for range 2 {
		client := NewClient(peer.URL, nil)
		out, err := client.CallTool(callmeta.WithOperationID(context.Background(), "stable-operation-id"), "write", map[string]any{})
		if err != nil || out != "confirmed" {
			t.Fatalf("call failed: %q %v", out, err)
		}
	}
	if calls != 2 {
		t.Fatal("tool calls not exercised")
	}
}
