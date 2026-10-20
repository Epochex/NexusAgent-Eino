package tool

import (
	"context"
	"errors"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

type canceledRepo struct{}

func (canceledRepo) GetByCode(ctx context.Context, _ string) (*model.Tool, error) {
	return nil, ctx.Err()
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
