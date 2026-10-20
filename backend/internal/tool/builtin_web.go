// Network tools use separate operator-configured search and user-fetch policies.
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

type SearchConfig struct {
	SearxngURL      string
	DomainWhitelist []string
	// These grants apply only to user-requested HTTP fetches. Search's fixed
	// operator endpoint has its own exact-origin grant and never widens this list.
	HTTPPrivateOrigins        []string
	searchClient, fetchClient *http.Client
}

func prepareWebConfig(cfg SearchConfig) (SearchConfig, func(), error) {
	closeClients := func() {
		if cfg.searchClient != nil {
			cfg.searchClient.CloseIdleConnections()
		}
		if cfg.fetchClient != nil {
			cfg.fetchClient.CloseIdleConnections()
		}
	}
	cfg.DomainWhitelist = append([]string(nil), cfg.DomainWhitelist...)
	if len(cfg.DomainWhitelist) > 128 {
		return cfg, closeClients, fmt.Errorf("HTTP domain allowlist exceeds 128 entries")
	}
	for i, domain := range cfg.DomainWhitelist {
		domain = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(domain)), ".")
		if !validDomainRule(domain) {
			return cfg, closeClients, fmt.Errorf("invalid HTTP allowlist domain")
		}
		cfg.DomainWhitelist[i] = domain
	}
	var err error
	if cfg.SearxngURL != "" {
		endpoint, e := url.Parse(cfg.SearxngURL)
		if e != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return cfg, closeClients, fmt.Errorf("invalid fixed search endpoint")
		}
		origin, e := security.OutboundOrigin(cfg.SearxngURL)
		if e != nil {
			return cfg, closeClients, e
		}
		cfg.searchClient, err = security.NewOutboundClient(security.OutboundPolicy{PrivateOrigins: []string{origin}, MaxResponseBytes: (512 << 10), Timeout: 10 * time.Second})
		if err != nil {
			return cfg, closeClients, err
		}
	}
	if len(cfg.DomainWhitelist) > 0 {
		cfg.fetchClient, err = security.NewOutboundClient(security.OutboundPolicy{PrivateOrigins: cfg.HTTPPrivateOrigins, MaxResponseBytes: (256 << 10), Timeout: 10 * time.Second})
		if err != nil {
			closeClients()
			return cfg, closeClients, err
		}
	}
	return cfg, closeClients, nil
}

// RegisterWeb owns reusable bounded clients. Unconfigured tools are not
// registered; callers must close the returned idle pools on shutdown.
func RegisterWeb(r Registry, cfg SearchConfig) (func(), error) {
	cfg, closeClients, err := prepareWebConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.searchClient != nil {
		r.Register("web_search", func(ctx context.Context, args map[string]any, _ string) (string, error) {
			return runWebSearch(ctx, cfg, args)
		})
	}
	if cfg.fetchClient != nil {
		r.Register("http_client", func(ctx context.Context, args map[string]any, _ string) (string, error) {
			return runHTTPClient(ctx, cfg, args)
		})
	}
	return closeClients, nil
}

func boundedWebBody(response *http.Response, limit int64) ([]byte, error) {
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("upstream HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("incomplete upstream response")
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("upstream response exceeds byte limit")
	}
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("upstream text is not UTF-8")
	}
	return body, nil
}

func runWebSearch(ctx context.Context, cfg SearchConfig, args map[string]any) (string, error) {
	if cfg.searchClient == nil {
		return "", fmt.Errorf("search service is not configured")
	}
	query, _ := args["query"].(string)
	if strings.TrimSpace(query) == "" || len(query) > 2048 || !utf8.ValidString(query) {
		return "", fmt.Errorf("query must contain 1..2048 UTF-8 bytes")
	}
	count := boundedInt(args["count"], 5, 20)
	endpoint, err := url.Parse(cfg.SearxngURL)
	if err != nil {
		return "", err
	}
	endpoint.RawQuery = url.Values{"q": {query}, "format": {"json"}, "language": {"zh-CN"}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", err
	}
	response, err := cfg.searchClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("search request failed: %w", err)
	}
	body, err := boundedWebBody(response, 512<<10)
	if err != nil {
		return "", err
	}
	var result struct {
		Results []struct{ Title, URL, Content string }
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("search service returned invalid JSON")
	}
	if result.Results == nil {
		return "", fmt.Errorf("search service did not return a results array")
	}
	out := make([]map[string]string, 0, count)
	for _, item := range result.Results {
		if len(out) >= count {
			break
		}
		link, err := url.Parse(item.URL)
		if err != nil || link.User != nil || link.Hostname() == "" || (link.Scheme != "http" && link.Scheme != "https") || len(item.URL) > 2048 {
			continue
		}
		out = append(out, map[string]string{"title": truncateText(item.Title, 256), "url": item.URL, "snippet": truncateText(item.Content, 200)})
	}
	raw, err := json.Marshal(out)
	return string(raw), err
}

func runHTTPClient(ctx context.Context, cfg SearchConfig, args map[string]any) (string, error) {
	if cfg.fetchClient == nil {
		return "", fmt.Errorf("HTTP fetch domain allowlist is not configured")
	}
	rawURL, _ := args["url"].(string)
	if len(rawURL) > 4096 {
		return "", fmt.Errorf("URL exceeds limit")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !domainAllowed(parsed.Hostname(), cfg.DomainWhitelist) {
		return "", fmt.Errorf("HTTP target is not in the operator domain allowlist")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	if value, exists := args["headers"]; exists {
		headers, ok := value.(map[string]any)
		if !ok || len(headers) > 3 {
			return "", fmt.Errorf("only Accept, Accept-Language and User-Agent headers are supported")
		}
		for name, value := range headers {
			switch strings.ToLower(name) {
			case "accept", "accept-language", "user-agent":
			default:
				return "", fmt.Errorf("unsupported HTTP request header")
			}
			text, ok := value.(string)
			if !ok || len(text) > 1024 || strings.ContainsAny(text, "\r\n\x00") {
				return "", fmt.Errorf("invalid HTTP request header")
			}
			request.Header.Set(name, text)
		}
	}
	response, err := cfg.fetchClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("HTTP fetch failed: %w", err)
	}
	status := response.StatusCode
	body, err := boundedWebBody(response, 256<<10)
	if err != nil {
		return "", err
	}
	result := map[string]any{"status": status, "content": truncateText(stripHTML(string(body)), 4000)}
	raw, err := json.Marshal(result)
	return string(raw), err
}

func validDomainRule(domain string) bool {
	if domain == "" || len(domain) > 253 {
		return false
	}
	if net.ParseIP(domain) != nil {
		return true
	}
	for _, label := range strings.Split(domain, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}
func domainAllowed(host string, whitelist []string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, domain := range whitelist {
		if host == domain || net.ParseIP(domain) == nil && strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

var htmlTagRe = regexp.MustCompile(`<[^>]+>\s*`)

func stripHTML(text string) string {
	return strings.Join(strings.Fields(htmlTagRe.ReplaceAllString(text, " ")), " ")
}
func truncateText(text string, max int) string {
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	return string(runes[:max]) + "..."
}
