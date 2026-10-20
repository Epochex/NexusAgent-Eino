package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A strict protocol peer models the vendor's documented multi-turn contract;
// this regression consumes no external model credits.
func TestToolCallHistoryPreservesReasoningForBothProtocols(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body openaiRequest
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Messages) != 3 {
			http.Error(w, "invalid fixture", 400)
			return
		}
		if body.Messages[0].ReasoningContent != "tool reasoning fixture" || body.Messages[2].ReasoningContent != "plain reasoning fixture" {
			http.Error(w, "reasoning_content missing from assistant history", 400)
			return
		}
		if len(body.Messages[0].ToolCalls) != 1 || body.Messages[1].ToolCallID != "lookup-1" {
			http.Error(w, "tool linkage lost", 400)
			return
		}
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"verified\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			return
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"verified"},"finish_reason":"stop"}]}`)
	}))
	defer peer.Close()
	p := &openaiCompatibleProvider{httpClient: peer.Client()}
	desc := &ModelDescriptor{BaseURL: peer.URL, ModelName: "deepseek-flash", MaxTokens: 32}
	request := &ChatRequest{MaxTokens: 32, Messages: []Message{
		{Role: RoleAssistant, ReasoningContent: "tool reasoning fixture", ToolCalls: []ToolCall{{ID: "lookup-1", Name: "inspect", ArgsJSON: "{}"}}},
		{Role: RoleTool, ToolCallID: "lookup-1", Content: "fixture evidence"},
		{Role: RoleAssistant, Content: "previous answer", ReasoningContent: "plain reasoning fixture"},
	}}
	t.Run("nonstream", func(t *testing.T) {
		reply, err := p.Chat(context.Background(), desc, request)
		if err != nil || reply.Content != "verified" {
			t.Fatalf("strict history roundtrip failed: %v", err)
		}
	})
	t.Run("stream", func(t *testing.T) {
		events, err := p.ChatStream(context.Background(), desc, request)
		if err != nil {
			t.Fatal(err)
		}
		answer := ""
		for event := range events {
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			answer += event.Content
		}
		if answer != "verified" {
			t.Fatalf("strict stream history lost: %q", answer)
		}
	})
}
