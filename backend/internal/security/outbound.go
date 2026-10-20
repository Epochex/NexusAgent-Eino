package security

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrOutboundResponseTooLarge = errors.New("outbound response exceeds configured limit")
var ErrOutboundRequestTooLarge = errors.New("outbound request exceeds configured limit")
var ErrOutboundTarget = errors.New("outbound target denied")

// Private grants are exact scheme/host/port origins, configured by the operator.
// They never override the metadata/link-local denial. Request-controlled URLs
// cannot grant themselves access to an internal origin.
type OutboundPolicy struct {
	PrivateOrigins        []string
	MaxResponseBytes      int64
	Timeout               time.Duration
	ResponseHeaderTimeout time.Duration
	lookup                func(context.Context, string) ([]netip.Addr, error)
}

func OutboundOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", ErrOutboundTarget
	}
	return outboundOrigin(u)
}
func outboundOrigin(u *url.URL) (string, error) {
	if u == nil || u.User != nil || u.Opaque != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || len(u.String()) > 4096 {
		return "", ErrOutboundTarget
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" || strings.ContainsAny(host, "%\\\x00\r\n\t ") {
		return "", ErrOutboundTarget
	}
	if address, err := netip.ParseAddr(host); err == nil {
		host = address.Unmap().String()
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", ErrOutboundTarget
	}
	return u.Scheme + "://" + net.JoinHostPort(host, strconv.Itoa(number)), nil
}
func (p OutboundPolicy) privateAllowed(u *url.URL) bool {
	origin, err := outboundOrigin(u)
	if err != nil {
		return false
	}
	for _, raw := range p.PrivateOrigins {
		if candidate, err := OutboundOrigin(raw); err == nil && candidate == origin {
			return true
		}
	}
	return false
}
func (p OutboundPolicy) Validate(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return ErrOutboundTarget
	}
	return p.validateURL(u)
}
func (p OutboundPolicy) validateURL(u *url.URL) error {
	if _, err := outboundOrigin(u); err != nil {
		return err
	}
	if address, err := netip.ParseAddr(strings.TrimSuffix(u.Hostname(), ".")); err == nil && !outboundIPAllowed(address, p.privateAllowed(u)) {
		return ErrOutboundTarget
	}
	return nil
}

var outboundNever = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("100.100.100.200/32"), netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("fd00:ec2::254/128"),
}

func outboundIPAllowed(address netip.Addr, private bool) bool {
	address = address.Unmap()
	if !address.IsValid() || (!address.IsGlobalUnicast() && !(address.IsLoopback() && private)) || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
		return false
	}
	for _, prefix := range outboundNever {
		if prefix.Contains(address) {
			return false
		}
	}
	internal := address.IsPrivate() || address.IsLoopback() || netip.MustParsePrefix("100.64.0.0/10").Contains(address)
	return !internal || private
}

type outboundTransport struct {
	base   *http.Transport
	policy OutboundPolicy
	limit  int64
}
type outboundDialKey struct{}
type outboundDialGrant struct {
	address string
	private bool
}

func (t *outboundTransport) CloseIdleConnections() { t.base.CloseIdleConnections() }
func (t *outboundTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := t.policy.validateURL(request.URL); err != nil {
		return nil, err
	}
	if request.ContentLength > 1<<20 {
		return nil, ErrOutboundRequestTooLarge
	}
	origin, _ := outboundOrigin(request.URL)
	parsed, _ := url.Parse(origin)
	grant := outboundDialGrant{address: parsed.Host, private: t.policy.privateAllowed(request.URL)}
	request = request.Clone(context.WithValue(request.Context(), outboundDialKey{}, grant))
	// A forwarded operation key is an identity, not proof that the peer honors
	// idempotency. Prevent net/http from replaying a POST on a reused connection
	// after the remote effect has happened but its response was lost.
	request.GetBody = nil
	if request.Body != nil && request.Body != http.NoBody {
		request.Body = &outboundBody{ReadCloser: request.Body, remaining: 1 << 20, limitError: ErrOutboundRequestTooLarge}
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.ContentLength > t.limit {
		response.Body.Close()
		return nil, ErrOutboundResponseTooLarge
	}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if media == "application/json" || strings.HasSuffix(media, "+json") {
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, t.limit+1))
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if int64(len(raw)) > t.limit {
			return nil, ErrOutboundResponseTooLarge
		}
		response.Body = io.NopCloser(bytes.NewReader(raw))
		response.ContentLength = int64(len(raw))
		return response, nil
	}
	response.Body = &outboundBody{ReadCloser: response.Body, remaining: t.limit}
	return response, nil
}

type outboundBody struct {
	io.ReadCloser
	remaining  int64
	limitError error
}

func (b *outboundBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	limitError := b.limitError
	if limitError == nil {
		limitError = ErrOutboundResponseTooLarge
	}
	if b.remaining < 0 {
		return 0, limitError
	}
	maximum := int64(len(p))
	if maximum > b.remaining+1 {
		maximum = b.remaining + 1
	}
	if maximum < 1 {
		maximum = 1
	}
	n, err := b.ReadCloser.Read(p[:maximum])
	b.remaining -= int64(n)
	if b.remaining < 0 {
		b.ReadCloser.Close()
		return 0, limitError
	}
	return n, err
}

// Each dial resolves once and connects to a validated numeric address. No proxy
// or second resolver can turn the checked hostname into an unchecked target.
func NewOutboundClient(policy OutboundPolicy) (*http.Client, error) {
	policy.PrivateOrigins = append([]string(nil), policy.PrivateOrigins...)
	if len(policy.PrivateOrigins) > 128 {
		return nil, fmt.Errorf("too many private origins")
	}
	for _, raw := range policy.PrivateOrigins {
		u, err := url.Parse(raw)
		if err != nil || u.RawQuery != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("private grants must be exact HTTP origins")
		}
		if _, err = outboundOrigin(u); err != nil {
			return nil, err
		}
	}
	if policy.MaxResponseBytes == 0 {
		policy.MaxResponseBytes = 4 << 20
	}
	if policy.MaxResponseBytes < 1 || policy.MaxResponseBytes > 8<<20 {
		return nil, fmt.Errorf("outbound response limit must be 1..8MiB")
	}
	if policy.Timeout == 0 {
		policy.Timeout = 30 * time.Second
	}
	if policy.Timeout < time.Millisecond || policy.Timeout > 2*time.Minute {
		return nil, fmt.Errorf("invalid outbound timeout")
	}
	if policy.ResponseHeaderTimeout == 0 {
		policy.ResponseHeaderTimeout = min(10*time.Second, policy.Timeout)
	}
	if policy.ResponseHeaderTimeout < time.Millisecond || policy.ResponseHeaderTimeout > policy.Timeout {
		return nil, fmt.Errorf("invalid outbound response header timeout")
	}
	resolve := policy.lookup
	if resolve == nil {
		resolve = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	base := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 8, MaxConnsPerHost: 16, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: policy.ResponseHeaderTimeout, TLSHandshakeTimeout: 5 * time.Second}
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, ErrOutboundTarget
		}
		lookupCtx, stop := context.WithTimeout(ctx, 3*time.Second)
		defer stop()
		var addresses []netip.Addr
		if literal, err := netip.ParseAddr(host); err == nil {
			addresses = []netip.Addr{literal}
		} else {
			addresses, err = resolve(lookupCtx, host)
			if err != nil {
				return nil, fmt.Errorf("outbound DNS lookup failed: %w", err)
			}
		}
		if len(addresses) == 0 || len(addresses) > 16 {
			return nil, ErrOutboundTarget
		}
		grant, ok := ctx.Value(outboundDialKey{}).(outboundDialGrant)
		canonicalHost := strings.TrimSuffix(strings.ToLower(host), ".")
		if ip, err := netip.ParseAddr(canonicalHost); err == nil {
			canonicalHost = ip.Unmap().String()
		}
		if !ok || grant.address != net.JoinHostPort(canonicalHost, port) {
			return nil, ErrOutboundTarget
		}
		for _, ip := range addresses {
			if !outboundIPAllowed(ip, grant.private) {
				return nil, ErrOutboundTarget
			}
		}
		var last error
		for _, ip := range addresses {
			connection, err := (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
			if err == nil {
				return connection, nil
			}
			last = err
		}
		return nil, last
	}
	transport := &outboundTransport{base: base, policy: policy, limit: policy.MaxResponseBytes}
	return &http.Client{Transport: transport, Timeout: policy.Timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 4 {
			return fmt.Errorf("outbound redirect limit exceeded")
		}
		if err := policy.validateURL(req.URL); err != nil {
			return err
		}
		original, err := outboundOrigin(via[0].URL)
		if err != nil {
			return err
		}
		next, err := outboundOrigin(req.URL)
		if err != nil || next != original {
			return fmt.Errorf("cross-origin redirects are disabled")
		}
		return nil
	}}, nil
}
