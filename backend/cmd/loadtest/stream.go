package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type streamResult struct{ Terminal, Failed bool }

func isContentDelta(name string, data []byte) bool {
	if name != "delta" {
		return false
	}
	var value struct {
		Content string `json:"content"`
	}
	return json.Unmarshal(data, &value) == nil && value.Content != ""
}
func readEvents(reader io.Reader, observe func(string, []byte)) (streamResult, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	var name string
	var data []byte
	result := streamResult{}
	dispatch := func() {
		if name != "" {
			observe(name, data)
			if name == "done" || name == "error" {
				result.Terminal = true
				result.Failed = name == "error"
			}
		}
		name = ""
		data = nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			dispatch()
		case strings.HasPrefix(line, "event:"):
			name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:"))...)
		}
	}
	if name != "" {
		dispatch()
	}
	return result, scanner.Err()
}

type streamTarget struct {
	id        int64
	actor     loadActor
	primary   bool
	scheduled time.Time
}
type streamSlot struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	paused bool
}
type streamPool struct {
	ctx                            context.Context
	client                         *apiClient
	slots                          []*streamSlot
	seeds                          []streamTarget
	active, peak, opened, failures atomic.Int64
	observe                        func(string, float64)
}

func newStreamPool(ctx context.Context, api *apiClient, seeds []streamTarget, observe func(string, float64)) *streamPool {
	p := &streamPool{ctx: ctx, client: api, seeds: seeds, observe: observe}
	for range seeds {
		p.slots = append(p.slots, &streamSlot{})
	}
	return p
}
func (p *streamPool) change(index int, target *streamTarget, pause bool) error {
	slot := p.slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.paused && !pause {
		return nil
	}
	if slot.cancel != nil {
		slot.cancel()
		select {
		case <-slot.done:
		case <-time.After(2 * time.Second):
			return fmt.Errorf("SSE slot failed to close")
		}
	}
	slot.cancel = nil
	slot.done = nil
	slot.paused = pause
	if target == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(p.ctx)
	done := make(chan struct{})
	slot.cancel = cancel
	slot.done = done
	go func() {
		defer close(done)
		p.subscribe(ctx, *target)
		for ctx.Err() == nil {
			if waitPeer(ctx, 100*time.Millisecond) != nil {
				return
			}
			p.subscribe(ctx, p.seeds[index])
		}
	}()
	return nil
}
func (p *streamPool) resume(index int) {
	s := p.slots[index]
	s.mu.Lock()
	s.paused = false
	s.mu.Unlock()
	_ = p.change(index, &p.seeds[index], false)
}
func (p *streamPool) subscribe(ctx context.Context, target streamTarget) {
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/api/v1/workflows/runs/%d/events", p.client.base, target.id), nil)
	req.Header.Set("Authorization", "Bearer "+target.actor.Token)
	req.Header.Set("Accept", "text/event-stream")
	res, err := p.client.http.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			p.failures.Add(1)
		}
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(res.Header.Get("Content-Type"), "text/event-stream") {
		p.failures.Add(1)
		return
	}
	p.opened.Add(1)
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for old := p.peak.Load(); active > old; old = p.peak.Load() {
		if p.peak.CompareAndSwap(old, active) {
			break
		}
	}
	first := false
	_, err = readEvents(res.Body, func(name string, _ []byte) {
		if target.primary && !first && name == "step_succeeded" {
			first = true
			p.observe("workflow_first_result", float64(time.Since(target.scheduled))/1e6)
		}
	})
	if err != nil && ctx.Err() == nil {
		p.failures.Add(1)
	}
}
