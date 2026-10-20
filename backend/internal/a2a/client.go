// Package a2a adapts the official A2A protocol client to platform delegation.
package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	"github.com/chengpeng-cp/nexus-agent/internal/mcp"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

type clientOptions struct {
	policy           security.OutboundPolicy
	credentialOrigin string
	headers          map[string]string
}
type ClientOption func(*clientOptions)

func WithOutboundPolicy(policy security.OutboundPolicy) ClientOption {
	return func(c *clientOptions) { c.policy = policy }
}
func WithCredentials(origin string, headers map[string]string) ClientOption {
	return func(c *clientOptions) { c.credentialOrigin = origin; c.headers = headers }
}

type Client struct {
	httpClient *http.Client
	policy     security.OutboundPolicy
	setupErr   error
}
type credentialTransport struct {
	base    http.RoundTripper
	origin  string
	headers http.Header
}

func (t *credentialTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if len(t.headers) > 0 {
		origin, err := security.OutboundOrigin(request.URL.String())
		if err != nil || origin != t.origin {
			return nil, security.ErrOutboundTarget
		}
	}
	cloned := request.Clone(request.Context())
	for key, values := range t.headers {
		cloned.Header[key] = append([]string(nil), values...)
	}
	var requestID json.RawMessage
	if cloned.Method == http.MethodPost {
		if cloned.Body == nil {
			return nil, fmt.Errorf("missing A2A JSON-RPC request")
		}
		raw, err := io.ReadAll(io.LimitReader(cloned.Body, (1<<20)+1))
		cloned.Body.Close()
		if err != nil || len(raw) > 1<<20 {
			return nil, fmt.Errorf("invalid A2A request body")
		}
		var rpc struct {
			ID json.RawMessage `json:"id"`
		}
		if json.Unmarshal(raw, &rpc) != nil || len(rpc.ID) == 0 {
			return nil, fmt.Errorf("invalid A2A request ID")
		}
		requestID = rpc.ID
		cloned.Body = io.NopCloser(bytes.NewReader(raw))
		cloned.ContentLength = int64(len(raw))
		cloned.GetBody = nil
	}
	response, err := t.base.RoundTrip(cloned)
	if err != nil || len(requestID) == 0 || response.StatusCode != http.StatusOK {
		return response, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	response.Body.Close()
	if readErr != nil || len(raw) > 1<<20 || !utf8.Valid(raw) {
		return nil, fmt.Errorf("incomplete or oversized A2A JSON-RPC response")
	}
	var rpc struct {
		Version string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &rpc) != nil || rpc.Version != "2.0" || !bytes.Equal(bytes.TrimSpace(requestID), bytes.TrimSpace(rpc.ID)) {
		return nil, fmt.Errorf("A2A JSON-RPC response identity mismatch")
	}
	hasResult := len(rpc.Result) > 0 && string(rpc.Result) != "null"
	hasError := len(rpc.Error) > 0 && string(rpc.Error) != "null"
	if hasResult == hasError {
		return nil, fmt.Errorf("A2A response must contain exactly one result or error")
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	return response, nil
}
func (t *credentialTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func NewClient(options ...ClientOption) *Client {
	cfg := clientOptions{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	cfg.policy.MaxResponseBytes = 1 << 20
	if cfg.policy.Timeout == 0 {
		cfg.policy.Timeout = 2 * time.Minute
	}
	if cfg.policy.ResponseHeaderTimeout == 0 {
		cfg.policy.ResponseHeaderTimeout = cfg.policy.Timeout
	}
	client := &Client{policy: cfg.policy}
	if err := mcp.ValidateHeaders(cfg.headers); err != nil {
		client.setupErr = err
		return client
	}
	var origin string
	if len(cfg.headers) > 0 {
		var err error
		origin, err = security.OutboundOrigin(cfg.credentialOrigin)
		if err != nil {
			client.setupErr = fmt.Errorf("A2A credentials require a valid bound origin")
			return client
		}
	}
	network, err := security.NewOutboundClient(cfg.policy)
	if err != nil {
		client.setupErr = err
		return client
	}
	headers := http.Header{}
	for name, value := range cfg.headers {
		if strings.HasPrefix(strings.ToLower(name), "a2a-") {
			network.CloseIdleConnections()
			client.setupErr = fmt.Errorf("reserved A2A header")
			return client
		}
		headers.Set(name, value)
	}
	network.Transport = &credentialTransport{base: network.Transport, origin: origin, headers: headers}
	client.httpClient = network
	return client
}
func (c *Client) Close() {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}

func (c *Client) ValidationError() error { return c.setupErr }
func (c *Client) validateBase(raw string) error {
	if c.setupErr != nil {
		return c.setupErr
	}
	parsed, err := url.Parse(raw)
	if err != nil || len(raw) > 512 || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("invalid A2A discovery URL")
	}
	return c.policy.Validate(raw)
}

func (c *Client) FetchCard(parent context.Context, baseURL string) (*sdk.AgentCard, error) {
	if err := c.validateBase(baseURL); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	resolver := agentcard.NewResolver(c.httpClient)
	resolver.CardParser = func(raw []byte) (*sdk.AgentCard, error) {
		if len(raw) > 256<<10 || !utf8.Valid(raw) {
			return nil, fmt.Errorf("invalid or oversized A2A AgentCard")
		}
		var card sdk.AgentCard
		if err := json.Unmarshal(raw, &card); err != nil {
			return nil, fmt.Errorf("invalid A2A AgentCard")
		}
		if card.Name == "" || len(card.Name) > 128 || !utf8.ValidString(card.Name) || len(card.Description) > 16384 || len(card.SupportedInterfaces) > 16 {
			return nil, fmt.Errorf("invalid A2A AgentCard identity")
		}
		if _, err := c.Endpoint(baseURL, &card); err != nil {
			return nil, err
		}
		return &card, nil
	}
	return resolver.Resolve(ctx, baseURL)
}

// Endpoint only chooses current JSON-RPC interfaces on the registered origin.
// No card-controlled transport fallback may forward platform credentials.
func (c *Client) Endpoint(baseURL string, card *sdk.AgentCard) (*sdk.AgentInterface, error) {
	if err := c.validateBase(baseURL); err != nil {
		return nil, err
	}
	origin, _ := security.OutboundOrigin(baseURL)
	if card == nil {
		return nil, fmt.Errorf("missing AgentCard")
	}
	for _, endpoint := range card.SupportedInterfaces {
		if endpoint == nil || endpoint.ProtocolBinding != sdk.TransportProtocolJSONRPC {
			continue
		}
		// The SDK currently implements the 1.0 family. Do not silently claim old 0.2/0.3 wire compatibility.
		if endpoint.ProtocolVersion != "1.0" && endpoint.ProtocolVersion != "1.0.0" && endpoint.ProtocolVersion != "1.0.1" {
			continue
		}
		parsed, err := url.Parse(endpoint.URL)
		candidate, e := security.OutboundOrigin(endpoint.URL)
		if err != nil || e != nil || candidate != origin || parsed.RawQuery != "" || len(endpoint.URL) > 512 || c.policy.Validate(endpoint.URL) != nil {
			return nil, fmt.Errorf("A2A AgentCard endpoint must remain on its registered origin")
		}
		copy := *endpoint
		return &copy, nil
	}
	return nil, fmt.Errorf("AgentCard has no supported A2A 1.0 JSON-RPC interface")
}

func (c *Client) protocol(ctx context.Context, baseURL string, card *sdk.AgentCard) (*a2aclient.Client, error) {
	endpoint, err := c.Endpoint(baseURL, card)
	if err != nil {
		return nil, err
	}
	return a2aclient.NewFromEndpoints(ctx, []*sdk.AgentInterface{endpoint}, a2aclient.WithDefaultsDisabled(), a2aclient.WithJSONRPCTransport(c.httpClient))
}
func (c *Client) SendMessage(ctx context.Context, baseURL, text string) (*Task, error) {
	card, err := c.FetchCard(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	return c.SendToCard(ctx, baseURL, card, text)
}
func (c *Client) SendToCard(ctx context.Context, baseURL string, card *sdk.AgentCard, text string) (*Task, error) {
	return c.SendRequest(ctx, baseURL, card, text, sdk.NewMessageID(), false)
}

func (c *Client) SendRequest(ctx context.Context, baseURL string, card *sdk.AgentCard, text, messageID string, returnImmediately bool) (*Task, error) {
	if messageID == "" || len(messageID) > 128 || strings.ContainsAny(messageID, "\r\n\x00") {
		return nil, fmt.Errorf("invalid A2A message ID")
	}
	if strings.TrimSpace(text) == "" || len(text) > 32768 || !utf8.ValidString(text) {
		return nil, fmt.Errorf("A2A input must contain 1..32768 UTF-8 bytes")
	}
	client, err := c.protocol(ctx, baseURL, card)
	if err != nil {
		return nil, err
	}
	defer client.Destroy()
	message := sdk.NewMessage(sdk.MessageRoleUser, sdk.NewTextPart(text))
	message.ID = messageID
	result, err := client.SendMessage(ctx, &sdk.SendMessageRequest{Message: message, Config: &sdk.SendMessageConfig{ReturnImmediately: returnImmediately}})
	if err != nil {
		return nil, err
	}
	return normalizeResult(result)
}
func (c *Client) GetTask(ctx context.Context, baseURL string, card *sdk.AgentCard, id string) (*Task, error) {
	if id == "" || len(id) > 256 {
		return nil, fmt.Errorf("invalid A2A task ID")
	}
	client, err := c.protocol(ctx, baseURL, card)
	if err != nil {
		return nil, err
	}
	defer client.Destroy()
	result, err := client.GetTask(ctx, &sdk.GetTaskRequest{ID: sdk.TaskID(id)})
	if err != nil {
		return nil, err
	}
	return normalizeResult(result)
}
func (c *Client) CancelTask(ctx context.Context, baseURL string, card *sdk.AgentCard, id string) (*Task, error) {
	if id == "" || len(id) > 256 {
		return nil, fmt.Errorf("invalid A2A task ID")
	}
	client, err := c.protocol(ctx, baseURL, card)
	if err != nil {
		return nil, err
	}
	defer client.Destroy()
	result, err := client.CancelTask(ctx, &sdk.CancelTaskRequest{ID: sdk.TaskID(id)})
	if err != nil {
		return nil, err
	}
	return normalizeResult(result)
}

func normalizeResult(result sdk.SendMessageResult) (*Task, error) {
	out := &Task{}
	var parts sdk.ContentParts
	switch value := result.(type) {
	case *sdk.Task:
		if value == nil || value.ID == "" || len(value.ID) > 256 {
			return nil, fmt.Errorf("invalid remote A2A task")
		}
		out.ID = string(value.ID)
		switch value.Status.State {
		case sdk.TaskStateSubmitted:
			out.State = "submitted"
		case sdk.TaskStateWorking:
			out.State = "working"
		case sdk.TaskStateCompleted:
			out.State = "completed"
		case sdk.TaskStateFailed:
			out.State = "failed"
		case sdk.TaskStateCanceled:
			out.State = "canceled"
		case sdk.TaskStateRejected:
			out.State = "rejected"
		case sdk.TaskStateInputRequired:
			out.State = "input_required"
		case sdk.TaskStateAuthRequired:
			out.State = "auth_required"
		default:
			return nil, fmt.Errorf("unknown remote A2A task state")
		}
		for _, artifact := range value.Artifacts {
			if artifact != nil {
				parts = append(parts, artifact.Parts...)
			}
		}
		if len(parts) == 0 && value.Status.Message != nil {
			parts = value.Status.Message.Parts
		}
	case *sdk.Message:
		if value == nil || value.Role != sdk.MessageRoleAgent {
			return nil, fmt.Errorf("invalid remote A2A message")
		}
		out.State = "completed"
		parts = value.Parts
	default:
		return nil, fmt.Errorf("unsupported remote A2A response")
	}
	var text strings.Builder
	for _, part := range parts {
		if part == nil {
			return nil, fmt.Errorf("invalid nil A2A content part")
		}
		value, ok := part.Content.(sdk.Text)
		if !ok {
			return nil, fmt.Errorf("remote A2A response contains unsupported non-text content")
		}
		if text.Len()+len(value) > 65536 {
			return nil, fmt.Errorf("remote A2A output exceeds 64KiB")
		}
		text.WriteString(string(value))
	}
	if out.State == "completed" && strings.TrimSpace(text.String()) == "" {
		return nil, fmt.Errorf("remote A2A completed without usable output")
	}
	if text.Len() > 0 {
		out.Artifacts = []Artifact{{Name: "response", Parts: []Part{{Type: "text", Text: text.String()}}}}
	}
	return out, nil
}
func ExtractText(task *Task) string {
	if task == nil {
		return ""
	}
	var text strings.Builder
	for _, artifact := range task.Artifacts {
		for _, part := range artifact.Parts {
			if part.Type == "text" {
				text.WriteString(part.Text)
			}
		}
	}
	return text.String()
}
