package tool

import (
	"context"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"strings"
	"testing"
	"time"
)

type authorityRepo struct{ rows []model.Tool }

func (r *authorityRepo) GetByCode(_ context.Context, code string) (*model.Tool, error) {
	for _, row := range r.rows {
		if row.Code == code {
			return &row, nil
		}
	}
	return nil, errors.New("not found")
}
func (r *authorityRepo) ListByCodes(_ context.Context, codes []string) ([]model.Tool, error) {
	var rows []model.Tool
	for _, row := range r.rows {
		for _, code := range codes {
			if row.Code == code {
				rows = append(rows, row)
				break
			}
		}
	}
	return rows, nil
}
func (r *authorityRepo) ListByIDs(_ context.Context, ids []int64) ([]model.Tool, error) {
	var rows []model.Tool
	for _, row := range r.rows {
		for _, id := range ids {
			if row.ID == id {
				rows = append(rows, row)
				break
			}
		}
	}
	return rows, nil
}
func TestDirectExecutorInvocationChecksBindingStatusAndSchema(t *testing.T) {
	repo := &authorityRepo{rows: []model.Tool{{ID: 1, Code: "allowed", Parameters: []byte(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"],"additionalProperties":false}`), Status: 1}, {ID: 2, Code: "unbound", Parameters: []byte(`{"type":"object"}`), Status: 1}}}
	registry := NewRegistry()
	calls := 0
	handler := func(context.Context, map[string]any, string) (string, error) { calls++; return "effect", nil }
	registry.Register("allowed", handler)
	registry.Register("unbound", handler)
	executor := NewExecutor(repo, registry, ExecutorConfig{DefaultTimeout: time.Second})
	bindings := `[{"tool_id":1}]`
	if result := executor.Invoke(context.Background(), bindings, "unbound", `{}`); result.Status == "ok" || calls != 0 {
		t.Fatal("unbound handler executed", result, calls)
	}
	if result := executor.Invoke(context.Background(), bindings, "allowed", `{"q":123}`); result.Status == "ok" || calls != 0 {
		t.Fatal("invalid arguments executed", result, calls)
	}
	repo.rows[0].Status = 0
	if result := executor.Invoke(context.Background(), bindings, "allowed", `{"q":"valid"}`); result.Status == "ok" || calls != 0 {
		t.Fatal("revoked handler executed", result, calls)
	}
}

func TestToolBindingResourceBounds(t *testing.T) {
	for _, raw := range []string{strings.Repeat(" ", 65537) + "[]", "[" + strings.TrimSuffix(strings.Repeat(`{"tool_id":1},`, 129), ",") + "]"} {
		if _, err := ParseAgentBindings(raw); err == nil {
			t.Fatal("oversized bindings accepted", fmt.Sprint(len(raw)))
		}
	}
}
