package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrPDFNotConfigured = errors.New("isolated PDF parser is not configured")
var ErrPDFUnavailable = errors.New("isolated PDF parser is unavailable")
var ErrPDFInvalid = errors.New("PDF or parser output is invalid")

const MaxPDFResponseBytes = 6*MaxTextBytes + 2<<20

// PDFClient talks only to an operator-configured origin. Document/user input
// cannot select a destination. No proxy inheritance or HTTP redirects can send
// its credential elsewhere. The separate parser container owns hard CPU/RAM.
type PDFClient struct {
	endpoint, token string
	http            *http.Client
}

func NewPDFClient(endpoint, token string) (*PDFClient, error) {
	if endpoint == "" && token == "" {
		return nil, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" || u.Scheme != "http" && u.Scheme != "https" || u.Path != "" && u.Path != "/" || len(token) < 16 || len(token) > 4096 || strings.ContainsAny(token, "\r\n\x00") {
		return nil, ErrPDFNotConfigured
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 12 * time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 16 << 10}
	return &PDFClient{endpoint: strings.TrimSuffix(endpoint, "/") + "/parse", token: token, http: &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *PDFClient) Close() {
	if c != nil {
		c.http.CloseIdleConnections()
	}
}
func (c *PDFClient) Load(ctx context.Context, data []byte) (*LoadedDoc, error) {
	if c == nil {
		return nil, ErrPDFNotConfigured
	}
	if len(data) < 5 || len(data) > MaxDocumentBytes || !bytes.HasPrefix(data, []byte("%PDF-")) {
		return nil, ErrPDFInvalid
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, errors.Join(ErrPDFUnavailable, err)
	}
	req.Header.Set("Content-Type", "application/pdf")
	req.Header.Set("Authorization", "Bearer "+c.token)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, errors.Join(ErrPDFUnavailable, err)
	}
	defer res.Body.Close()
	if res.StatusCode == 422 || res.StatusCode == 413 {
		return nil, ErrPDFInvalid
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("%w (HTTP %d)", ErrPDFUnavailable, res.StatusCode)
	}
	media, _, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return nil, ErrPDFInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, MaxPDFResponseBytes+1))
	if err != nil {
		return nil, errors.Join(ErrPDFUnavailable, err)
	}
	if len(raw) > MaxPDFResponseBytes || !utf8.Valid(raw) {
		return nil, ErrPDFInvalid
	}
	var doc LoadedDoc
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&doc) != nil || decoder.Decode(new(any)) != io.EOF || len(doc.Pages) < 1 || len(doc.Pages) > MaxPDFPages || len(doc.Warnings) > MaxPDFPages {
		return nil, ErrPDFInvalid
	}
	total, last := 0, 0
	for _, page := range doc.Pages {
		if page.Page <= last || page.Page > MaxPDFPages || strings.TrimSpace(page.Content) == "" {
			return nil, ErrPDFInvalid
		}
		last = page.Page
		total += len(page.Content)
		if total > MaxTextBytes {
			return nil, ErrPDFInvalid
		}
	}
	for _, warning := range doc.Warnings {
		if len(warning) > 1024 {
			return nil, ErrPDFInvalid
		}
	}
	return &doc, nil
}
