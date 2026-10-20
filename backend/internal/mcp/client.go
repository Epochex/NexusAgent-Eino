// Package mcp adapts the official protocol client to bounded platform tool calls.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
	validate    func(any) error
}
type clientOptions struct{ policy security.OutboundPolicy }
type ClientOption func(*clientOptions)

func WithOutboundPolicy(policy security.OutboundPolicy) ClientOption {
	return func(options *clientOptions) { options.policy = policy }
}

var ErrClientClosed = errors.New("MCP client closed")

type Client struct {
	baseURL           string
	httpClient        *http.Client
	sdkClient         *sdk.Client
	setupErr          error
	gate, catalogGate chan struct{}
	mu                sync.RWMutex
	session           *sdk.ClientSession
	initCancel        context.CancelFunc
	closed            bool
	catalogue         []ToolDef
	catalogReady      bool
}

func NewClient(endpoint string, headers map[string]string, options ...ClientOption) *Client {
	cfg := clientOptions{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	c := &Client{baseURL: endpoint, gate: make(chan struct{}, 1), catalogGate: make(chan struct{}, 1)}
	if cfg.policy.Timeout == 0 {
		cfg.policy.Timeout = 30 * time.Second
	}
	if cfg.policy.ResponseHeaderTimeout == 0 {
		cfg.policy.ResponseHeaderTimeout = min(cfg.policy.Timeout, 30*time.Second)
	}
	if err := cfg.policy.Validate(endpoint); err != nil {
		c.setupErr = err
		return c
	}
	network, err := security.NewOutboundClient(cfg.policy)
	if err != nil {
		c.setupErr = err
		return c
	}
	transport, err := newHeaderTransport(network.Transport, headers)
	if err != nil {
		network.CloseIdleConnections()
		c.setupErr = err
		return c
	}
	network.Transport = transport
	c.httpClient = network
	c.sdkClient = sdk.NewClient(&sdk.Implementation{Name: "nexus-agent-eino", Version: "0.3"}, &sdk.ClientOptions{Capabilities: &sdk.ClientCapabilities{}, MultiRoundTrip: &sdk.MultiRoundTripOptions{Disabled: true}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	// Validate before the SDK's permissive tool filtering; malformed discovery
	// must fail a sync rather than silently revoke the previous usable catalogue.
	c.sdkClient.AddSendingMiddleware(func(next sdk.MethodHandler) sdk.MethodHandler {
		return func(ctx context.Context, method string, request sdk.Request) (sdk.Result, error) {
			result, err := next(ctx, method, request)
			if err != nil {
				return result, err
			}
			if list, ok := result.(*sdk.ListToolsResult); ok {
				if len(list.Tools) > 256 {
					return nil, fmt.Errorf("MCP catalogue page exceeds 256 tools")
				}
				for _, tool := range list.Tools {
					if _, err := convertTool(tool); err != nil {
						return nil, err
					}
				}
			}
			return result, nil
		}
	})
	return c
}
func (c *Client) current() (*sdk.ClientSession, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session, c.closed
}
func (c *Client) Initialize(parent context.Context) error {
	if c.setupErr != nil {
		return c.setupErr
	}
	if session, closed := c.current(); closed {
		return ErrClientClosed
	} else if session != nil {
		return nil
	}
	select {
	case c.gate <- struct{}{}:
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-c.gate }()
	if session, closed := c.current(); closed {
		return ErrClientClosed
	} else if session != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.initCancel = cancel
	c.mu.Unlock()
	session, err := c.sdkClient.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: c.baseURL, HTTPClient: c.httpClient, MaxRetries: -1, DisableStandaloneSSE: true, MaxEventSize: 1 << 20}, nil)
	c.mu.Lock()
	c.initCancel = nil
	if err != nil {
		c.mu.Unlock()
		return errcode.ErrMCPConnFail.WithCause(err)
	}
	if c.closed {
		c.mu.Unlock()
		session.Close()
		return ErrClientClosed
	}
	c.session = session
	c.catalogReady = false
	c.catalogue = nil
	c.mu.Unlock()
	go func() {
		_ = session.Wait()
		c.mu.Lock()
		if c.session == session {
			c.session = nil
			c.catalogReady = false
			c.catalogue = nil
		}
		c.mu.Unlock()
	}()
	return nil
}
func (c *Client) ListTools(parent context.Context) ([]ToolDef, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	session, closed := c.current()
	if closed || session == nil {
		return nil, ErrClientClosed
	}
	result := make([]ToolDef, 0)
	cursor := ""
	seenCursors := map[string]bool{}
	seenNames := map[string]bool{}
	for page := 0; page < 16; page++ {
		response, err := session.ListTools(ctx, &sdk.ListToolsParams{Cursor: cursor})
		if err != nil {
			return nil, errcode.ErrMCPConnFail.WithCause(err)
		}
		if response == nil {
			return nil, fmt.Errorf("empty MCP catalogue response")
		}
		for _, remote := range response.Tools {
			tool, err := convertTool(remote)
			if err != nil {
				return nil, err
			}
			if seenNames[tool.Name] || len(result) >= 256 {
				return nil, fmt.Errorf("duplicate or oversized MCP catalogue")
			}
			seenNames[tool.Name] = true
			result = append(result, tool)
		}
		if response.NextCursor == "" {
			return result, nil
		}
		if len(response.NextCursor) > 1024 || seenCursors[response.NextCursor] {
			return nil, fmt.Errorf("invalid MCP pagination cursor")
		}
		cursor = response.NextCursor
		seenCursors[cursor] = true
	}
	return nil, fmt.Errorf("MCP catalogue exceeds 16 pages")
}
func (c *Client) catalog(parent context.Context) ([]ToolDef, error) {
	c.mu.RLock()
	if c.catalogReady {
		result := c.catalogue
		c.mu.RUnlock()
		return result, nil
	}
	c.mu.RUnlock()
	select {
	case c.catalogGate <- struct{}{}:
	case <-parent.Done():
		return nil, parent.Err()
	}
	defer func() { <-c.catalogGate }()
	c.mu.RLock()
	if c.catalogReady {
		result := c.catalogue
		c.mu.RUnlock()
		return result, nil
	}
	c.mu.RUnlock()
	catalogue, err := c.ListTools(parent)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.session == nil {
		return nil, ErrClientClosed
	}
	c.catalogue = catalogue
	c.catalogReady = true
	return catalogue, nil
}
func (c *Client) CallTool(parent context.Context, name string, args map[string]any) (string, error) {
	if !ValidToolName(name) {
		return "", fmt.Errorf("invalid MCP tool name")
	}
	if args == nil {
		args = map[string]any{}
	}
	raw, err := json.Marshal(args)
	if err != nil || len(raw) > 65536 {
		return "", fmt.Errorf("MCP arguments exceed 64KiB")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	catalogue, err := c.catalog(ctx)
	if err != nil {
		return "", err
	}
	found := false
	var validate func(any) error
	for _, tool := range catalogue {
		if tool.Name == name {
			found = true
			validate = tool.validate
			break
		}
	}
	if !found {
		return "", errcode.ErrToolNotFound
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || validate == nil {
		return "", fmt.Errorf("invalid MCP arguments")
	}
	if err = validate(value); err != nil {
		return "", fmt.Errorf("MCP arguments violate the discovered schema")
	}
	session, closed := c.current()
	if closed || session == nil {
		return "", ErrClientClosed
	}
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return "", errcode.ErrMCPToolFail.WithCause(err)
	}
	if result == nil || len(result.InputRequests) > 0 || result.RequestState != "" {
		return "", fmt.Errorf("MCP result is incomplete or requires unsupported interactive input")
	}
	var output strings.Builder
	for _, content := range result.Content {
		text, ok := content.(*sdk.TextContent)
		if !ok {
			return "", fmt.Errorf("MCP tool returned unsupported non-text content")
		}
		if output.Len() > 0 {
			output.WriteByte('\n')
		}
		if output.Len()+len(text.Text) > 65536 {
			return "", fmt.Errorf("MCP tool output exceeds 64KiB")
		}
		output.WriteString(text.Text)
	}
	if result.StructuredContent != nil {
		structured, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return "", err
		}
		if output.String() != string(structured) {
			if output.Len() > 0 {
				output.WriteByte('\n')
			}
			if output.Len()+len(structured) > 65536 {
				return "", fmt.Errorf("MCP structured output exceeds 64KiB")
			}
			output.Write(structured)
		}
	}
	if result.IsError {
		return output.String(), errcode.ErrMCPToolFail.WithMsg("MCP工具执行失败")
	}
	return output.String(), nil
}
func (c *Client) Ping(ctx context.Context) error { _, err := c.ListTools(ctx); return err }
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	session := c.session
	cancel := c.initCancel
	c.session = nil
	c.catalogue = nil
	c.catalogReady = false
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var err error
	if session != nil {
		err = session.Close()
	}
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return err
}
