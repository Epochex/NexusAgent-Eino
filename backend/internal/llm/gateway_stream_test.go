package llm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
)

func gatewayFixture(t *testing.T) (*Gateway, *ModelDescriptor, <-chan struct{}) {
	t.Helper()
	entered := make(chan struct{}, 8)
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		entered <- struct{}{}
		<-r.Context().Done()
	})
	return NewGateway(&config.LLM{MaxConnsPerProvider: 1, CircuitErrorRate: 0.5, CircuitOpen: time.Second}), &ModelDescriptor{ProviderCode: "openai", BaseURL: s.URL, ModelName: "stub"}, entered
}

func TestGatewayStreamHoldsPermit(t *testing.T) {
	g, desc, entered := gatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, _, err := g.ChatStream(ctx, desc, nil, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	defer func() {
		cancel()
		for range stream {
		}
	}()
	other, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	_, _, err = g.ChatStream(other, desc, nil, &ChatRequest{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stream permit released at headers, want deadline, got %v", err)
	}
	select {
	case <-entered:
		t.Fatal("second upstream request bypassed held permit")
	default:
	}
}

func TestGatewayStreamCancelReleasesPermit(t *testing.T) {
	g, desc, entered := gatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, _, err := g.ChatStream(ctx, desc, nil, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	for range stream {
	}
	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	stream2, _, err := g.ChatStream(next, desc, nil, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	stop()
	for range stream2 {
	}
}

func TestGatewayDoesNotFallbackAfterPartialOutput(t *testing.T) {
	var fallbackCalls atomic.Int32
	primary := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	})
	fallback := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) { fallbackCalls.Add(1); fmt.Fprint(w, "data: [DONE]\n\n") })
	g := NewGateway(&config.LLM{MaxConnsPerProvider: 1, CircuitErrorRate: 0.5, CircuitOpen: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, _, err := g.ChatStream(ctx, &ModelDescriptor{ProviderCode: "primary", BaseURL: primary.URL, ModelName: "primary"}, &ModelDescriptor{ProviderCode: "fallback", BaseURL: fallback.URL, ModelName: "fallback"}, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var events []StreamEvent
	for ev := range stream {
		events = append(events, ev)
	}
	requireStreamError(t, events)
	if fallbackCalls.Load() != 0 {
		t.Fatal("partial response was silently replayed on fallback")
	}
}

func TestGatewayQueueTimeoutDoesNotOpenProviderCircuit(t *testing.T) {
	g, desc, entered := gatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, _, err := g.ChatStream(ctx, desc, nil, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	defer func() {
		cancel()
		for range stream {
		}
	}()
	for range 20 {
		waiting, stop := context.WithTimeout(context.Background(), time.Millisecond)
		_, _, _ = g.ChatStream(waiting, desc, nil, &ChatRequest{})
		stop()
	}
	_, breaker, _ := g.providerFor(desc.ProviderCode)
	if !breaker.Allow() {
		t.Fatal("local queue deadlines opened the healthy provider circuit")
	}
}
