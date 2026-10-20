package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

type peerNonce struct {
	Models          int     `json:"models"`
	ModelCompleted  int     `json:"model_completed"`
	Tools           int     `json:"tools"`
	ToolCompleted   int     `json:"tool_completed"`
	Canceled        int     `json:"canceled"`
	ModelDurationMS float64 `json:"model_duration_ms"`
	ToolDurationMS  float64 `json:"tool_duration_ms"`
}
type controlledPeer struct {
	modelDelay, toolDelay                            time.Duration
	mu                                               sync.Mutex
	nonces                                           map[string]*peerNonce
	activeModels, peakModels, activeTools, peakTools int
}

func newControlledPeer(model, tool time.Duration) *controlledPeer {
	return &controlledPeer{modelDelay: model, toolDelay: tool, nonces: map[string]*peerNonce{}}
}

var noncePattern = regexp.MustCompile(`nonce=([a-zA-Z0-9_-]{1,128})`)

func (p *controlledPeer) entry(nonce string) *peerNonce {
	if row := p.nonces[nonce]; row != nil {
		return row
	}
	if len(p.nonces) >= 100000 {
		return nil
	}
	row := &peerNonce{}
	p.nonces[nonce] = row
	return row
}
func waitPeer(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *controlledPeer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "ok", "model_delay_ms": p.modelDelay.Milliseconds(), "tool_delay_ms": p.toolDelay.Milliseconds()})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		p.mu.Lock()
		defer p.mu.Unlock()
		rows := map[string]peerNonce{}
		for key, row := range p.nonces {
			if strings.HasPrefix(key, prefix) {
				rows[key] = *row
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"model_delay_ms": p.modelDelay.Milliseconds(), "tool_delay_ms": p.toolDelay.Milliseconds(), "active_models": p.activeModels, "peak_models": p.peakModels, "active_tools": p.activeTools, "peak_tools": p.peakTools, "nonces": rows})
	})
	mux.HandleFunc("/v1/chat/completions", p.model)
	remote := sdk.NewServer(&sdk.Implementation{Name: "nexus-capacity-controlled-peer", Version: "1"}, nil)
	remote.AddTool(&sdk.Tool{Name: "inspect", Description: "Read a controlled diagnostic value; no external side effects", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"nonce": map[string]any{"type": "string", "minLength": 1, "maxLength": 128}}, "required": []string{"nonce"}, "additionalProperties": false}}, func(ctx context.Context, request *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		var args struct {
			Nonce string `json:"nonce"`
		}
		if request == nil || request.Params == nil || json.Unmarshal(request.Params.Arguments, &args) != nil || args.Nonce == "" || len(args.Nonce) > 128 {
			return nil, fmt.Errorf("invalid controlled nonce")
		}
		start := time.Now()
		p.mu.Lock()
		row := p.entry(args.Nonce)
		if row == nil {
			p.mu.Unlock()
			return nil, fmt.Errorf("bounded peer ledger full")
		}
		row.Tools++
		p.activeTools++
		p.peakTools = max(p.peakTools, p.activeTools)
		p.mu.Unlock()
		err := waitPeer(ctx, p.toolDelay)
		p.mu.Lock()
		p.activeTools--
		row.ToolDurationMS += float64(time.Since(start)) / float64(time.Millisecond)
		if err == nil {
			row.ToolCompleted++
		} else {
			row.Canceled++
		}
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "tool evidence verified nonce=" + args.Nonce}}}, nil
	})
	mux.Handle("/mcp", sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return remote }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	return mux
}

func (p *controlledPeer) model(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		http.Error(w, "request too large", 413)
		return
	}
	var req struct {
		Stream   bool `json:"stream"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if json.Unmarshal(raw, &req) != nil || len(req.Messages) == 0 {
		http.Error(w, "invalid model request", 400)
		return
	}
	nonce := ""
	evidence := false
	toolMessage := false
	for _, message := range req.Messages {
		if match := noncePattern.FindStringSubmatch(message.Content); len(match) > 1 {
			if nonce != "" && nonce != match[1] {
				http.Error(w, "cross-run context", 400)
				return
			}
			nonce = match[1]
		}
		if message.Role == "tool" {
			toolMessage = true
		}
		if strings.Contains(message.Content, "tool evidence verified nonce=") {
			evidence = true
		}
	}
	if nonce == "" {
		http.Error(w, "missing controlled nonce", 400)
		return
	}
	p.mu.Lock()
	row := p.entry(nonce)
	if row == nil {
		p.mu.Unlock()
		http.Error(w, "bounded peer ledger full", 503)
		return
	}
	row.Models++
	if (toolMessage || evidence) && (!evidence || row.ToolCompleted < 1) {
		p.mu.Unlock()
		http.Error(w, "actual tool evidence required", 400)
		return
	}
	p.activeModels++
	p.peakModels = max(p.peakModels, p.activeModels)
	p.mu.Unlock()
	start := time.Now()
	success := false
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.activeModels--
		row.ModelDurationMS += float64(time.Since(start)) / float64(time.Millisecond)
		if success {
			row.ModelCompleted++
		} else {
			row.Canceled++
		}
	}()
	if err = waitPeer(r.Context(), p.modelDelay); err != nil {
		return
	}
	content := nonce
	if evidence {
		content = "completed nonce=" + nonce
	}
	if !req.Stream {
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]any{"model": "controlled", "choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}})
		success = err == nil
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	delta := map[string]any{"content": content}
	finish := "stop"
	if !evidence {
		if len(req.Tools) != 1 || req.Tools[0].Function.Name == "" {
			http.Error(w, "one actual tool definition required", 400)
			return
		}
		args, _ := json.Marshal(map[string]string{"nonce": nonce})
		key := sha256.Sum256([]byte(nonce))
		delta = map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": fmt.Sprintf("call_%x", key[:8]), "type": "function", "function": map[string]any{"name": req.Tools[0].Function.Name, "arguments": string(args)}}}}
		finish = "tool_calls"
	}
	frame, _ := json.Marshal(map[string]any{"model": "controlled", "choices": []any{map[string]any{"delta": delta, "finish_reason": finish}}})
	_, err = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", frame)
	if flush, ok := w.(http.Flusher); ok {
		flush.Flush()
	}
	success = err == nil
}
