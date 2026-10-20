//go:build integration && infrastructure && realmodel

package api

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type realModelObservation struct {
	VendorError       string  `json:"vendor_error,omitempty"`
	Fingerprint       string  `json:"fingerprint"`
	Status            int     `json:"status"`
	Stream            bool    `json:"stream"`
	Prompt            int     `json:"prompt_tokens"`
	Completion        int     `json:"completion_tokens"`
	CacheHit          int     `json:"cache_hit_tokens"`
	Reasoning         int     `json:"reasoning_tokens"`
	Finish            string  `json:"finish_reason"`
	FirstContentMS    float64 `json:"first_content_ms"`
	TotalMS           float64 `json:"total_ms"`
	ChargedCeilingCNY float64 `json:"peak_price_ceiling_cny"`
}

// An opt-in acceptance fixture, not a model implementation. Requests and model
// responses are forwarded unchanged; only Authorization is replaced in memory.
// No response text, reasoning text, credential or request headers are persisted.
type realBudgetRelay struct {
	mu              sync.Mutex
	url, key, path  string
	client          *http.Client
	capCNY, charged float64
	maxCalls        int
	rows            []realModelObservation
	seen            map[string]bool
	loadErr         bool
}

func newRealBudgetRelay(url, key, path string) *realBudgetRelay {
	b := &realBudgetRelay{url: url, key: key, path: path, client: &http.Client{Timeout: 90 * time.Second}, capCNY: .8, maxCalls: 24, seen: map[string]bool{}}
	if raw, err := os.ReadFile(path); err == nil {
		if json.Unmarshal(raw, &b.rows) != nil {
			b.loadErr = true
		}
	} else if !os.IsNotExist(err) {
		b.loadErr = true
	}
	for _, row := range b.rows {
		b.charged += row.ChargedCeilingCNY
		b.seen[row.Fingerprint] = true
	}
	return b
}
func (b *realBudgetRelay) save() error {
	raw, err := json.MarshalIndent(b.rows, "", "  ")
	if err != nil {
		return err
	}
	// Reserve durably before forwarding. Interrupted calls keep their full
	// reservation on restart; they cannot silently reset the spending ceiling.
	return os.WriteFile(b.path, raw, 0600)
}
func (b *realBudgetRelay) snapshot() []realModelObservation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]realModelObservation(nil), b.rows...)
}
func (b *realBudgetRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock() // exactly one billable request at a time
	if b.loadErr {
		http.Error(w, "ledger unavailable", 503)
		return
	}
	if r.Method != "POST" || r.URL.Path != "/chat/completions" {
		http.NotFound(w, r)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16385))
	if err != nil || len(raw) > 16384 {
		http.Error(w, "input bound", 413)
		return
	}
	var request struct {
		Model     string            `json:"model"`
		MaxTokens int               `json:"max_tokens"`
		Stream    bool              `json:"stream"`
		Messages  []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(raw, &request) != nil || request.Model != "deepseek-flash" || request.MaxTokens < 1 || request.MaxTokens > 1024 || len(request.Messages) > 32 {
		http.Error(w, "model/output bound", 400)
		return
	}
	hash := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(hash[:])
	if b.seen[fingerprint] {
		http.Error(w, "duplicate billable request blocked", 409)
		return
	}
	if len(b.rows) >= b.maxCalls {
		http.Error(w, "request ceiling", 429)
		return
	}
	// Peak/miss CNY prices checked 2026-10-21: input2/M, output8/M.
	// .08 reserves >35K input +1024 output tokens for a <=16KiB text body.
	const reserve = .08
	if b.charged+reserve > b.capCNY+1e-9 {
		http.Error(w, "spending ceiling", 402)
		return
	}
	index := len(b.rows)
	b.rows = append(b.rows, realModelObservation{Fingerprint: fingerprint, Stream: request.Stream, ChargedCeilingCNY: reserve})
	b.seen[fingerprint] = true
	b.charged += reserve
	if b.save() != nil {
		b.loadErr = true
		http.Error(w, "reservation failed", 503)
		return
	}
	started := time.Now()
	up, err := http.NewRequestWithContext(r.Context(), "POST", b.url+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "upstream request invalid", 502)
		return
	}
	up.Header.Set("Content-Type", "application/json")
	up.Header.Set("Authorization", "Bearer "+b.key)
	response, err := b.client.Do(up)
	if err != nil {
		b.rows[index].TotalMS = float64(time.Since(started)) / float64(time.Millisecond)
		_ = b.save()
		http.Error(w, "upstream transport failed; retry blocked", 502)
		return
	}
	defer response.Body.Close()
	row := &b.rows[index]
	row.Status = response.StatusCode
	consume := func(data []byte) {
		var frame struct {
			Choices []struct {
				Finish *string `json:"finish_reason"`
				Delta  struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				Prompt     int `json:"prompt_tokens"`
				Completion int `json:"completion_tokens"`
				Hit        int `json:"prompt_cache_hit_tokens"`
				Detail     struct {
					Reasoning int `json:"reasoning_tokens"`
				} `json:"completion_tokens_details"`
			} `json:"usage"`
		}
		if json.Unmarshal(data, &frame) != nil {
			return
		}
		if frame.Usage != nil {
			row.Prompt = frame.Usage.Prompt
			row.Completion = frame.Usage.Completion
			row.CacheHit = frame.Usage.Hit
			row.Reasoning = frame.Usage.Detail.Reasoning
		}
		for _, c := range frame.Choices {
			if c.Finish != nil {
				row.Finish = *c.Finish
			}
			if c.Delta.Content != "" && row.FirstContentMS == 0 {
				row.FirstContentMS = float64(time.Since(started)) / float64(time.Millisecond)
			}
		}
	}
	w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
	w.WriteHeader(response.StatusCode)
	if response.StatusCode != 200 {
		// Keep protocol diagnostics without another paid reproduction. Vendors
		// can echo the credential; redact it before forwarding or persistence.
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		row.VendorError = strings.ReplaceAll(string(data), b.key, "[REDACTED]")
		_, _ = io.WriteString(w, row.VendorError)
	} else if !request.Stream {
		data, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if e == nil {
			consume(data)
			_, _ = w.Write(data)
		}
	} else {
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") {
				consume([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))))
			}
			if _, e := io.WriteString(w, line+"\n"); e != nil {
				break
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
	row.TotalMS = float64(time.Since(started)) / float64(time.Millisecond)
	if row.Prompt > 0 && row.Completion >= 0 && row.Finish != "" {
		actualCeiling := float64(row.Prompt)*2/1e6 + float64(row.Completion)*8/1e6
		b.charged += actualCeiling - row.ChargedCeilingCNY
		row.ChargedCeilingCNY = actualCeiling
	}
	if b.save() != nil {
		b.loadErr = true
	}
}
