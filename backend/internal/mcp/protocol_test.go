package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/callmeta"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMCPClientOfficialPeersNegotiationPaginationAndConcurrentCalls(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-11-25"} {
		for _, jsonResponse := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/json_%v", version, jsonResponse), func(t *testing.T) {
				var initialized, discovered, listed, called atomic.Int32
				server := sdk.NewServer(&sdk.Implementation{Name: "official-peer", Version: "1"}, &sdk.ServerOptions{PageSize: 1, SupportedProtocolVersions: []string{version}, InitializedHandler: func(context.Context, *sdk.InitializedRequest) { initialized.Add(1) }})
				for i := range 3 {
					server.AddTool(&sdk.Tool{Name: fmt.Sprint("echo_", i), InputSchema: map[string]any{"type": "object", "additionalProperties": false}}, func(context.Context, *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
						called.Add(1)
						return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "confirmed"}}}, nil
					})
				}
				handler := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: version == "2026-07-28", JSONResponse: jsonResponse})
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
						if err != nil {
							t.Error(err)
							return
						}
						r.Body.Close()
						r.Body = io.NopCloser(bytes.NewReader(raw))
						var request struct{ Method string }
						if json.Unmarshal(raw, &request) != nil {
							t.Error("invalid RPC")
							return
						}
						switch request.Method {
						case "server/discover":
							discovered.Add(1)
						case "tools/list":
							listed.Add(1)
						case "tools/call":
							if !strings.HasPrefix(r.Header.Get("Idempotency-Key"), "op-") {
								t.Error("missing operation key")
							}
						}
						if request.Method != "tools/call" && r.Header.Get("Idempotency-Key") != "" {
							t.Error("key leaked to lifecycle RPC")
						}
					}
					handler.ServeHTTP(w, r)
				}))
				defer peer.Close()
				client := NewClient(peer.URL, nil, WithOutboundPolicy(security.OutboundPolicy{PrivateOrigins: []string{peer.URL}}))
				if closer, ok := any(client).(io.Closer); ok {
					defer closer.Close()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				tools, err := client.ListTools(ctx)
				if err != nil {
					t.Fatal("official peer list", err)
				}
				if len(tools) != 3 || listed.Load() != 3 {
					t.Fatalf("pagination dropped tools: %d pages=%d", len(tools), listed.Load())
				}
				var group sync.WaitGroup
				for i := range 8 {
					group.Add(1)
					go func() {
						defer group.Done()
						value, err := client.CallTool(callmeta.WithOperationID(ctx, fmt.Sprint("op-", i)), "echo_0", map[string]any{})
						if err != nil || value != "confirmed" {
							t.Errorf("concurrent call %q %v", value, err)
						}
					}()
				}
				group.Wait()
				wantInit := int32(0)
				if version == "2025-11-25" {
					wantInit = 1
				}
				if initialized.Load() != wantInit || discovered.Load() != 1 || called.Load() != 8 {
					t.Fatalf("lifecycle/concurrent calls: init=%d discovery=%d calls=%d", initialized.Load(), discovered.Load(), called.Load())
				}
			})
		}
	}
}
