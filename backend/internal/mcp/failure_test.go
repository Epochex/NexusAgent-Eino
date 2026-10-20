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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMCPDoesNotReplayLostToolResponse(t *testing.T) {
	server := sdk.NewServer(&sdk.Implementation{Name: "lost-response", Version: "1"}, nil)
	server.AddTool(&sdk.Tool{Name: "write", InputSchema: map[string]any{"type": "object"}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "must not replay"}}}, nil
	})
	handler := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	var effects atomic.Int32
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
			json.Unmarshal(raw, &request)
			if request.Method == "tools/call" {
				effects.Add(1)
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer peer.Close()
	client := NewClient(peer.URL, nil, WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.CallTool(callmeta.WithOperationID(ctx, "one-operation"), "write", map[string]any{}); err == nil || effects.Load() != 1 {
		t.Fatalf("lost response not surfaced or replayed: effects=%d error=%v", effects.Load(), err)
	}
}

func TestMCPToolResultsKeepFailureAndEnforceOutputLimit(t *testing.T) {
	server := sdk.NewServer(&sdk.Implementation{Name: "results", Version: "1"}, nil)
	for _, name := range []string{"failed", "large", "structured"} {
		server.AddTool(&sdk.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, func(_ context.Context, r *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
			switch r.Params.Name {
			case "failed":
				return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: "partial external result"}}}, nil
			case "large":
				return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: strings.Repeat("x", 65537)}}}, nil
			default:
				return &sdk.CallToolResult{StructuredContent: map[string]any{"value": "structured-result"}}, nil
			}
		})
	}
	peer := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: false}))
	defer peer.Close()
	client := NewClient(peer.URL, nil, WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := client.CallTool(ctx, "failed", map[string]any{})
	if err == nil || out != "partial external result" {
		t.Fatal("tool failure became success or lost partial output", out, err)
	}
	if _, err = client.CallTool(ctx, "large", map[string]any{}); err == nil {
		t.Fatal("oversized tool output accepted")
	}
	out, err = client.CallTool(ctx, "structured", map[string]any{})
	if err != nil || !strings.Contains(out, "structured-result") {
		t.Fatal("structured content lost", out, err)
	}
}

func TestMCPRejectsExternalSchemaAndInvalidHeaderAnnotations(t *testing.T) {
	for _, schema := range []map[string]any{
		{"type": "object", "properties": map[string]any{"secret": map[string]any{"$ref": "file:///etc/passwd"}}},
		{"type": "object", "properties": map[string]any{"secret": map[string]any{"$ref": "http://127.0.0.1/private"}}},
		{"type": "object", "properties": map[string]any{"secret": map[string]any{"type": "object", "x-mcp-header": "X-Secret"}}},
	} {
		if _, err := convertTool(&sdk.Tool{Name: "unsafe", InputSchema: schema}); err == nil {
			t.Fatal("unsafe schema was accepted")
		}
	}
}

func TestMCPValidatesArgumentsBeforeRemoteEffect(t *testing.T) {
	var calls atomic.Int32
	server := sdk.NewServer(&sdk.Implementation{Name: "argument-peer", Version: "1"}, nil)
	server.AddTool(&sdk.Tool{Name: "write", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"ticket": map[string]any{"type": "integer"}}, "required": []string{"ticket"}, "additionalProperties": false}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		calls.Add(1)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "effect"}}}, nil
	})
	peer := httptest.NewServer(sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	defer peer.Close()
	client := NewClient(peer.URL, nil, WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
	defer client.Close()
	if _, err := client.CallTool(context.Background(), "write", map[string]any{"unexpected": "value"}); err == nil || calls.Load() != 0 {
		t.Fatal("invalid arguments reached peer", calls.Load(), err)
	}
}
