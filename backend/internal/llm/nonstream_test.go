package llm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddingRejectsMissingDuplicateAndInvalidIndices(t *testing.T) {
	for name, payload := range map[string]string{
		"missing":          `{"data":[{"index":0,"embedding":[1,2]}]}`,
		"duplicate":        `{"data":[{"index":0,"embedding":[1,2]},{"index":0,"embedding":[1,2]}]}`,
		"out_of_range":     `{"data":[{"index":0,"embedding":[1,2]},{"index":9,"embedding":[1,2]}]}`,
		"empty":            `{"data":[{"index":0,"embedding":[]},{"index":1,"embedding":[1,2]}]}`,
		"mixed_dimensions": `{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[1,2]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("malformed provider response panicked: %v", r)
				}
			}()
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, payload) }))
			defer peer.Close()
			if _, err := NewProvider("test").Embed(context.Background(), &ModelDescriptor{BaseURL: peer.URL}, []string{"one", "two"}); err == nil {
				t.Fatal("malformed embedding batch accepted")
			}
		})
	}
}
func TestNonstreamResponsesAreBounded(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat(" ", 4<<20))
		switch r.URL.Path {
		case "/embeddings":
			fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1,2]}]}`)
		default:
			fmt.Fprint(w, `{"choices":[{"message":{"content":"too large"}}]}`)
		}
	}))
	defer peer.Close()
	provider := NewProvider("test")
	desc := &ModelDescriptor{BaseURL: peer.URL}
	if _, err := provider.Embed(context.Background(), desc, []string{"one"}); err == nil {
		t.Error("oversized embedding response accepted")
	}
	if _, err := provider.Chat(context.Background(), desc, &ChatRequest{}); err == nil {
		t.Error("oversized chat response accepted")
	}
	if _, err := provider.Rerank(context.Background(), desc, "query", []string{"one"}, 1); err == nil {
		t.Error("oversized rerank response accepted")
	}
}

func TestEmbeddingAcceptsReorderedCompleteBatch(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":[{"index":1,"embedding":[2]},{"index":0,"embedding":[1]}]}`)
	}))
	defer peer.Close()
	vectors, err := NewProvider("test").Embed(context.Background(), &ModelDescriptor{BaseURL: peer.URL}, []string{"one", "two"})
	if err != nil || len(vectors) != 2 || vectors[0][0] != 1 || vectors[1][0] != 2 {
		t.Fatalf("valid reordering corrupted: %+v %v", vectors, err)
	}
}
