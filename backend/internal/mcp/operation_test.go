package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/callmeta"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMCPPropagatesOperationIDOnlyToToolCall(t *testing.T) {
	var calls atomic.Int32
	server := sdk.NewServer(&sdk.Implementation{Name: "operation-peer", Version: "1"}, &sdk.ServerOptions{SupportedProtocolVersions: []string{"2025-11-25"}})
	server.AddTool(&sdk.Tool{Name: "write", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		calls.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "confirmed"}}}, nil
	})
	handler := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{JSONResponse: true})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			r.Body.Close()
			if err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var request struct{ Method string }
			if json.Unmarshal(raw, &request) != nil {
				t.Error("invalid RPC")
				return
			}
			if request.Method == "tools/call" {
				if r.Header.Get("Idempotency-Key") != "stable-operation-id" {
					t.Error("missing stable operation key")
				}
			} else if r.Header.Get("Idempotency-Key") != "" {
				t.Error("tool key attached to lifecycle/discovery")
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 2 {
		client := NewClient(peer.URL, nil, WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
		out, err := client.CallTool(callmeta.WithOperationID(ctx, "stable-operation-id"), "write", map[string]any{})
		closeErr := client.Close()
		if err != nil || out != "confirmed" || closeErr != nil {
			t.Fatalf("call failed: %q %v close %v", out, err, closeErr)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("tool calls not exercised")
	}
}
