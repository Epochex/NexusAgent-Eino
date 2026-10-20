package security

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOutboundDoesNotReplayToolPOSTAfterLostResponse(t *testing.T) {
	var effects atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodGet {
			fmt.Fprint(w, "warm")
			return
		}
		if effects.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		fmt.Fprint(w, "replayed")
	}))
	defer peer.Close()
	client, err := NewOutboundClient(OutboundPolicy{PrivateOrigins: []string{peer.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	warm, err := client.Get(peer.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, warm.Body)
	warm.Body.Close()
	request, err := http.NewRequest("POST", peer.URL, bytes.NewReader([]byte(`{"effect":"write"}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "stable-id-without-peer-guarantee")
	response, err := client.Do(request)
	if response != nil {
		response.Body.Close()
	}
	if err == nil || effects.Load() != 1 {
		t.Fatalf("unknown tool POST was replayed automatically: effects=%d error=%v", effects.Load(), err)
	}
}

func TestOutboundRejectsPrivateReservedAndCredentialURLs(t *testing.T) {
	policy := OutboundPolicy{}
	for _, raw := range []string{"http://127.0.0.1/x", "http://[::ffff:127.0.0.1]/", "http://169.254.169.254/latest", "http://10.0.0.1/", "http://100.64.0.1/", "http://[fd00:ec2::254]/", "file:///etc/passwd", "http://user:password@example.com/", "http://example.com/#fragment", "http://[fe80::1%25eth0]/"} {
		parsed, err := url.Parse(raw)
		if err == nil {
			err = policy.validateURL(parsed)
		}
		if err == nil {
			t.Errorf("unsafe URL accepted: %s", raw)
		}
	}
	for _, raw := range []string{"https://example.com/a", "https://8.8.8.8/"} {
		parsed, _ := url.Parse(raw)
		if err := policy.validateURL(parsed); err != nil {
			t.Fatal(raw, err)
		}
	}
}
func TestOutboundPrivateOriginIsExactAndMetadataNeverAllowed(t *testing.T) {
	policy := OutboundPolicy{PrivateOrigins: []string{"http://127.0.0.1:1234"}}
	for _, raw := range []string{"http://127.0.0.1:1234/path", "http://127.0.0.1:1235/path", "https://127.0.0.1:1234/path"} {
		parsed, _ := url.Parse(raw)
		err := policy.validateURL(parsed)
		if (err == nil) != (parsed.Scheme == "http" && parsed.Port() == "1234") {
			t.Fatal("origin grant escaped", raw, err)
		}
	}
	policy.PrivateOrigins = []string{"http://169.254.169.254"}
	parsed, _ := url.Parse(policy.PrivateOrigins[0])
	if err := policy.validateURL(parsed); err == nil {
		t.Fatal("metadata origin allowed")
	}
}
func TestOutboundPinsResolvedAddressAndRejectsMixedDNS(t *testing.T) {
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, "ok") }))
	defer peer.Close()
	parsed, _ := url.Parse(peer.URL)
	_, port, _ := net.SplitHostPort(parsed.Host)
	origin := "http://mcp.test:" + port
	var lookups atomic.Int32
	policy := OutboundPolicy{PrivateOrigins: []string{origin}, lookup: func(context.Context, string) ([]netip.Addr, error) {
		lookups.Add(1)
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}}
	client, err := NewOutboundClient(policy)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.Get(origin + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if requests.Load() != 1 || lookups.Load() != 1 {
		t.Fatal("unvalidated DNS redial", requests.Load(), lookups.Load())
	}
	wrongScheme := policy
	wrongScheme.PrivateOrigins = []string{"https://mcp.test:" + port}
	wrong, err := NewOutboundClient(wrongScheme)
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.CloseIdleConnections()
	if _, err = wrong.Get(origin + "/mcp"); err == nil {
		t.Fatal("private DNS grant crossed scheme")
	}
	if requests.Load() != 1 {
		t.Fatal("wrong-scheme request reached peer")
	}
	policy.PrivateOrigins = nil
	policy.lookup = func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")}, nil
	}
	denied, err := NewOutboundClient(policy)
	if err != nil {
		t.Fatal(err)
	}
	defer denied.CloseIdleConnections()
	if _, err = denied.Get(origin + "/mcp"); err == nil {
		t.Fatal("mixed public/private DNS accepted")
	}
	if requests.Load() != 1 {
		t.Fatal("blocked DNS request reached peer")
	}
}
func TestOutboundRejectsCrossOriginRedirectAndOversizedBodies(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); fmt.Fprint(w, "leaked") }))
	defer target.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, target.URL, 302)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, strings.Repeat("x", 65))
	}))
	defer peer.Close()
	client, err := NewOutboundClient(OutboundPolicy{PrivateOrigins: []string{peer.URL, target.URL}, MaxResponseBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if _, err = client.Get(peer.URL + "/redirect"); err == nil {
		t.Fatal("cross-origin redirect accepted")
	}
	if leaked.Load() != 0 {
		t.Fatal("redirect reached credential destination")
	}
	response, err := client.Get(peer.URL + "/large")
	if err == nil {
		_, err = io.ReadAll(response.Body)
		response.Body.Close()
	}
	if !errors.Is(err, ErrOutboundResponseTooLarge) {
		t.Fatal("oversized response not rejected", err)
	}
}
