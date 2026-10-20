package einoadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/llmstub"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func gateway() *llm.Gateway {
	return llm.NewGateway(&config.LLM{MaxConnsPerProvider: 32, CircuitErrorRate: 0.5, CircuitOpen: time.Second})
}

func TestWithToolsIsImmutable(t *testing.T) {
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		name := "none"
		if len(req.Tools) > 0 {
			name = req.Tools[0].Function.Name
		}
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": name}, "finish_reason": "stop"}}})
	})
	base, err := NewChatModel(gateway(), &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, name := range []string{"search", "calculate"} {
		wg.Go(func() {
			info := &schema.ToolInfo{Name: name, ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{})}
			bound, err := base.WithTools([]*schema.ToolInfo{info})
			if err != nil {
				t.Error(err)
				return
			}
			info.Name = "mutated"
			res, err := bound.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			if err != nil {
				t.Error(err)
				return
			}
			if res.Content != name {
				t.Errorf("tools crossed run boundary: got %q want %q", res.Content, name)
			}
		})
	}
	wg.Wait()
	res, err := base.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "none" {
		t.Fatal("binding mutated base model")
	}
}

func TestOptionsCopyDescriptors(t *testing.T) {
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model       string  `json:"model"`
			Temperature float32 `json:"temperature"`
			MaxTokens   int     `json:"max_tokens"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.Model != "override" || req.Temperature != 0.25 || req.MaxTokens != 64 {
			t.Errorf("call options lost: %+v", req)
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	})
	primary := &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "original", Temperature: 0.7, MaxTokens: 128}
	fallback := &llm.ModelDescriptor{ModelName: "backup", Temperature: 0.9}
	m, err := NewChatModel(gateway(), primary, fallback, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}, model.WithModel("override"), model.WithTemperature(0.25), model.WithMaxTokens(64))
	if err != nil {
		t.Fatal(err)
	}
	if primary.ModelName != "original" || primary.Temperature != 0.7 || primary.MaxTokens != 128 || fallback.Temperature != 0.9 {
		t.Fatal("call mutated shared descriptors")
	}
}

func TestModelStreamPreservesErrors(t *testing.T) {
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	})
	m, _ := NewChatModel(gateway(), &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, nil, time.Second)
	stream, err := m.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var content string
	var streamErr error
	for {
		m, err := stream.Recv()
		if err != nil {
			streamErr = err
			break
		}
		if m == nil {
			t.Fatal("internal close probe escaped adapter")
		}
		content += m.Content
	}
	if content != "partial" || streamErr == nil || errors.Is(streamErr, io.EOF) {
		t.Fatalf("partial failure became success: %q %v", content, streamErr)
	}
}

func TestModelStreamCloseCancelsUpstream(t *testing.T) {
	closed := make(chan struct{})
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	})
	m, _ := NewChatModel(gateway(), &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, nil, 5*time.Second)
	stream, err := m.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	stream.Close()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("closing Eino stream did not cancel HTTP request")
	}
}

func TestModelStreamDeadlineIsError(t *testing.T) {
	s := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	m, _ := NewChatModel(gateway(), &llm.ModelDescriptor{BaseURL: s.URL, ModelName: "stub"}, nil, 50*time.Millisecond)
	stream, err := m.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, err = stream.Recv()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout became normal EOF: %v", err)
	}
}

func TestModelUsesFallbackIdentity(t *testing.T) {
	primary := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", http.StatusBadRequest) })
	backup := llmstub.New(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"choices":[{"message":{"content":"backup"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2}}`)
	})
	m, _ := NewChatModel(gateway(), &llm.ModelDescriptor{ProviderCode: "primary", BaseURL: primary.URL, ModelName: "primary"}, &llm.ModelDescriptor{ProviderCode: "backup", BaseURL: backup.URL, ModelName: "backup"}, time.Second)
	res, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Extra["model"] != "backup" || res.ResponseMeta.Usage.PromptTokens != 7 {
		t.Fatalf("fallback identity/usage lost: %+v", res)
	}
}

func TestModelRejectsUnsupportedOptions(t *testing.T) {
	m, _ := NewChatModel(gateway(), &llm.ModelDescriptor{BaseURL: "http://127.0.0.1:1", ModelName: "stub"}, nil, time.Second)
	_, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}, model.WithStop([]string{"stop"}))
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unsupported stop option was silently ignored")
	}
}
