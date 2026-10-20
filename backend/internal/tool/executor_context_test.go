package tool

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

type canceledRepo struct{}

func (canceledRepo) GetByCode(ctx context.Context, _ string) (*model.Tool, error) {
	return nil, ctx.Err()
}

func TestKBSearchAcceptsJSONNumberLimits(t *testing.T) {
	registry := NewRegistry()
	var got int
	RegisterKBSearch(registry, func(_ context.Context, kbID int64, _ string, topK int) (string, error) {
		if kbID != 9007199254740993 {
			t.Error("KB ID rounded")
		}
		got = topK
		return "ok", nil
	})
	handler, _ := registry.Get("kb_search")
	_, err := handler(context.Background(), map[string]any{"kb_id": json.Number("9007199254740993"), "top_k": json.Number("9"), "query": "test"}, "")
	if err != nil || got != 9 {
		t.Fatalf("numeric top_k ignored: %d %v", got, err)
	}
}
func (canceledRepo) ListByCodes(ctx context.Context, _ []string) ([]model.Tool, error) {
	return nil, ctx.Err()
}
func (canceledRepo) ListByIDs(ctx context.Context, _ []int64) ([]model.Tool, error) {
	return nil, ctx.Err()
}
func TestListContextCancellation(t *testing.T) {
	exec := NewExecutor(canceledRepo{}, NewRegistry(), ExecutorConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := exec.ListContext(ctx, `[{"tool_id":1}]`)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller context not honored: %v", err)
	}
}
