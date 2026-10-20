package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/callmeta"
	"io"
	"net/http"
	"strings"
)

type headerTransport struct {
	base    http.RoundTripper
	headers http.Header
}

func ValidateHeaders(headers map[string]string) error {
	_, err := newHeaderTransport(http.DefaultTransport, headers)
	return err
}

func validHeaderName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return false
		}
	}
	return true
}
func newHeaderTransport(base http.RoundTripper, headers map[string]string) (*headerTransport, error) {
	if len(headers) > 32 {
		return nil, fmt.Errorf("too many MCP authentication headers")
	}
	copied := http.Header{}
	size := 0
	for key, value := range headers {
		lower := strings.ToLower(key)
		if !validHeaderName(key) || strings.ContainsAny(value, "\r\n\x00") || strings.HasPrefix(lower, "mcp-") {
			return nil, fmt.Errorf("invalid or reserved MCP header")
		}
		switch lower {
		case "host", "connection", "proxy-authorization", "proxy-connection", "transfer-encoding", "content-length", "content-type", "accept", "idempotency-key":
			return nil, fmt.Errorf("reserved MCP header")
		}
		size += len(key) + len(value)
		if size > 16384 {
			return nil, fmt.Errorf("MCP authentication headers exceed limit")
		}
		copied.Set(key, value)
	}
	return &headerTransport{base: base, headers: copied}, nil
}
func (t *headerTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
func (t *headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	for name, values := range t.headers {
		request.Header[name] = append([]string(nil), values...)
	}
	if request.Method == http.MethodPost && request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
		request.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(raw) > 1<<20 {
			return nil, fmt.Errorf("MCP request exceeds 1MiB")
		}
		request.Body = io.NopCloser(bytes.NewReader(raw))
		request.ContentLength = int64(len(raw))
		var rpc struct{ Method string }
		if json.Unmarshal(raw, &rpc) != nil {
			return nil, fmt.Errorf("invalid MCP request")
		}
		if rpc.Method == "tools/call" {
			if id := callmeta.OperationID(request.Context()); id != "" {
				if len(id) > 128 || strings.ContainsAny(id, "\r\n\x00") {
					return nil, fmt.Errorf("invalid MCP operation key")
				}
				request.Header.Set("Idempotency-Key", id)
			}
		}
	}
	return t.base.RoundTrip(request)
}
