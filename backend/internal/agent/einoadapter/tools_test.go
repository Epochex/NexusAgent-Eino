package einoadapter

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	platformtool "github.com/chengpeng-cp/nexus-agent/internal/tool"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type toolRepo struct{ rows []model.Tool }

func (r *toolRepo) GetByCode(ctx context.Context, code string) (*model.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, v := range r.rows {
		if v.Code == code {
			return &v, nil
		}
	}
	return nil, errors.New("missing tool")
}
func (r *toolRepo) ListByCodes(ctx context.Context, codes []string) ([]model.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []model.Tool
	for _, v := range r.rows {
		for _, code := range codes {
			if v.Code == code {
				out = append(out, v)
				break
			}
		}
	}
	return out, nil
}
func (r *toolRepo) ListByIDs(ctx context.Context, ids []int64) ([]model.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []model.Tool
	for _, v := range r.rows {
		for _, id := range ids {
			if v.ID == id {
				out = append(out, v)
				break
			}
		}
	}
	return out, nil
}

const toolSchema = `{"type":"object","properties":{"q":{"type":"string"}},"required":["q"],"additionalProperties":false}`

func toolFixture(t *testing.T, dangerous bool, handler platformtool.BuiltinHandler) (*platformtool.ProdExecutor, *toolRepo) {
	t.Helper()
	repo := &toolRepo{rows: []model.Tool{{ID: 1, Code: "search", Parameters: []byte(toolSchema), IsDangerous: dangerous, Status: 1}, {ID: 2, Code: "forbidden", Parameters: []byte(`{"type":"object"}`), Status: 1}}}
	reg := platformtool.NewRegistry()
	reg.Register("search", handler)
	reg.Register("forbidden", handler)
	return platformtool.NewExecutor(repo, reg, platformtool.ExecutorConfig{DefaultTimeout: time.Second}), repo
}
func buildTool(t *testing.T, exec *platformtool.ProdExecutor, dangerous map[string]bool, timeout time.Duration) einotool.InvokableTool {
	t.Helper()
	tools, err := BuildTools(context.Background(), exec, `[{"tool_id":1}]`, dangerous, timeout, 65536)
	if err != nil {
		t.Fatal(err)
	}
	return tools[0].(einotool.InvokableTool)
}

func TestToolRejectsUnboundName(t *testing.T) {
	var calls atomic.Int32
	exec, _ := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) { calls.Add(1); return "ok", nil })
	tools, err := BuildTools(context.Background(), exec, `[{"tool_id":1}]`, nil, time.Second, 65536)
	if err != nil {
		t.Fatal(err)
	}
	node, err := compose.NewToolNode(context.Background(), &compose.ToolsNodeConfig{Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	_, err = node.Invoke(context.Background(), &schema.Message{Role: schema.Assistant, ToolCalls: []schema.ToolCall{{ID: "bad", Type: "function", Function: schema.FunctionCall{Name: "forbidden", Arguments: `{}`}}}})
	if err == nil || calls.Load() != 0 {
		t.Fatal("unbound registry tool executed")
	}
}

func TestToolSchemaRejectsInvalidArgs(t *testing.T) {
	var calls atomic.Int32
	exec, _ := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) { calls.Add(1); return "ok", nil })
	bound := buildTool(t, exec, nil, time.Second)
	for _, args := range []string{`{}`, `{"q":3}`, `{"q":"go","extra":true}`, `{"q":"a"} {"q":"b"}`} {
		out, err := bound.InvokableRun(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out, "invalid_arguments") {
			t.Errorf("invalid args accepted: %s => %s", args, out)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached handler")
	}
	if out, err := bound.InvokableRun(context.Background(), `{"q":"go"}`); err != nil || out != "ok" || calls.Load() != 1 {
		t.Fatalf("valid call failed: %q %v", out, err)
	}
}

func TestToolDangerousFailsClosed(t *testing.T) {
	for _, authority := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "authority"}[authority], func(t *testing.T) {
			var calls atomic.Int32
			exec, _ := toolFixture(t, authority, func(context.Context, map[string]any, string) (string, error) { calls.Add(1); return "written", nil })
			flags := map[string]bool{}
			if !authority {
				flags["search"] = true
			}
			bound := buildTool(t, exec, flags, time.Second)
			out, err := bound.InvokableRun(context.Background(), `{"q":"go"}`)
			if err != nil || !strings.Contains(out, "approval_required") || calls.Load() != 0 {
				t.Fatalf("dangerous call bypassed policy: %s %v", out, err)
			}
		})
	}
}

func TestToolLimitAndTimeout(t *testing.T) {
	exec, _ := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) {
		return strings.Repeat("测", 30000), nil
	})
	out, err := buildTool(t, exec, nil, time.Second).InvokableRun(context.Background(), `{"q":"go"}`)
	if err != nil || len(out) > 65536 || !utf8.ValidString(out) || !strings.Contains(out, "[truncated]") {
		t.Fatalf("output was not bounded safely: len=%d err=%v", len(out), err)
	}
	exec, _ = toolFixture(t, false, func(ctx context.Context, _ map[string]any, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	out, err = buildTool(t, exec, nil, 20*time.Millisecond).InvokableRun(context.Background(), `{"q":"go"}`)
	if err != nil || !strings.Contains(out, "timeout") {
		t.Fatalf("timeout lost: %s %v", out, err)
	}
}

func TestToolBindingsIsolatedAcrossRuns(t *testing.T) {
	exec, _ := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) { return "ok", nil })
	bound := buildTool(t, exec, nil, time.Second)
	info, _ := bound.Info(context.Background())
	info.Name = "forbidden"
	other, err := BuildTools(context.Background(), exec, `[{"tool_id":2}]`, nil, time.Second, 65536)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := other[0].Info(context.Background())
	first, _ := bound.Info(context.Background())
	if first.Name != "search" || second.Name != "forbidden" {
		t.Fatal("tool info mutated another binding")
	}
}

func TestToolRejectsExternalSchemaReference(t *testing.T) {
	exec, repo := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) { return "ok", nil })
	for _, schemaText := range []string{`{"$ref":"http://127.0.0.1/private"}`, `{"type":"object","custom_security_rule":true}`} {
		repo.rows[0].Parameters = []byte(schemaText)
		if _, err := BuildTools(context.Background(), exec, `[{"tool_id":1}]`, nil, time.Second, 65536); err == nil {
			t.Fatalf("unsafe/unsupported schema accepted: %s", schemaText)
		}
	}
}

func TestToolDisabledDuringRunFailsClosed(t *testing.T) {
	var calls atomic.Int32
	exec, repo := toolFixture(t, false, func(context.Context, map[string]any, string) (string, error) { calls.Add(1); return "ok", nil })
	bound := buildTool(t, exec, nil, time.Second)
	repo.rows[0].Status = 0
	out, _ := bound.InvokableRun(context.Background(), `{"q":"go"}`)
	if calls.Load() != 0 || !strings.Contains(out, "tool_unavailable") {
		t.Fatalf("revoked tool executed: %s", out)
	}
}
