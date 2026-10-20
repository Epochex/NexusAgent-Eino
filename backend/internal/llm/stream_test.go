package llm

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
)

func streamFixture(t *testing.T, body string) []StreamEvent {
	t.Helper()
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, body)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := NewProvider("openai").ChatStream(ctx, &ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, &ChatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var events []StreamEvent
	for ev := range ch {
		events = append(events, ev)
	}
	return events
}

func requireStreamError(t *testing.T, events []StreamEvent) {
	t.Helper()
	for _, ev := range events {
		if ev.Err != nil {
			return
		}
	}
	t.Fatalf("stream silently completed without error: %+v", events)
}

func TestStreamReportsUnexpectedEOF(t *testing.T) {
	requireStreamError(t, streamFixture(t, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
}

func TestStreamPropagatesUpstreamError(t *testing.T) {
	requireStreamError(t, streamFixture(t, "data: {\"error\":{\"message\":\"provider failed\",\"type\":\"server_error\"}}\n\n"))
}

func TestStreamRejectsMalformedData(t *testing.T) {
	requireStreamError(t, streamFixture(t, "data: {bad json}\n\n"))
}

func TestStreamRejectsOversizedFrame(t *testing.T) {
	requireStreamError(t, streamFixture(t, "data: "+strings.Repeat("x", 513*1024)+"\n\n"))
}

func TestStreamRejectsOversizedAggregate(t *testing.T) {
	frame := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("x", 65536) + "\"}}]}\n\n"
	requireStreamError(t, streamFixture(t, strings.Repeat(frame, 80)+"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
}

func TestStreamToolCallsByIndex(t *testing.T) {
	events := streamFixture(t, `data: {"choices":[{"delta":{"tool_calls":[{"index":2,"id":"b","function":{"name":"second","arguments":"{\"b\":"}},{"index":0,"id":"a","function":{"name":"first","arguments":"{\"a\":"}}]}}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":2,"function":{"arguments":"2}"}}]},"finish_reason":"tool_calls"}]}

data: [DONE]

`)
	last := events[len(events)-1]
	if last.Err != nil || len(last.ToolCalls) != 2 {
		t.Fatalf("lost indexed tool calls: %+v", events)
	}
	if last.ToolCalls[0].ID != "a" || last.ToolCalls[0].ArgsJSON != `{"a":1}` || last.ToolCalls[1].ID != "b" || last.ToolCalls[1].ArgsJSON != `{"b":2}` {
		t.Fatalf("incorrect tool assembly/order: %+v", last.ToolCalls)
	}
}

func TestStreamUsageNotDuplicated(t *testing.T) {
	events := streamFixture(t, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
	var prompt, completion int
	for _, ev := range events {
		if ev.Err != nil {
			t.Fatal(ev.Err)
		}
		if ev.Usage != nil {
			prompt += ev.Usage.PromptTokens
			completion += ev.Usage.CompletionTokens
		}
	}
	if prompt != 11 || completion != 3 {
		t.Fatalf("usage counted more than once: %d/%d", prompt, completion)
	}
}

func TestProviderConcurrentClientInitialization(t *testing.T) {
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	})
	p := NewProvider("openai")
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			res, err := p.Chat(ctx, &ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, &ChatRequest{})
			if err != nil {
				t.Error(err)
				return
			}
			if res.Content != "ok" {
				t.Errorf("response: %+v", res)
			}
		})
	}
	wg.Wait()
}
