// 生产级工具执行器（td.md §8.7）。
//
// 统一执行管道：
//
//	查定义 → 参数 JSON Schema 校验 → 超时 ctx → panic recover
//	→ 危险工具 HITL（由 agent 层拦截） → handler 执行 → 结构化结果
//
// 教学要点（Ep 04）：工具错误必须"结构化返回给模型"而不是抛异常终止 ——
// 这是 Agent 自我修正能力的前提。
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// ToolRepo 工具定义数据访问（由 repository 实现注入）。
type ToolRepo interface {
	GetByCode(ctx context.Context, code string) (*model.Tool, error)
	ListByCodes(ctx context.Context, codes []string) ([]model.Tool, error)
	ListByIDs(ctx context.Context, ids []int64) ([]model.Tool, error)
}

// ExecutorConfig 执行器配置。
type ExecutorConfig struct {
	DefaultTimeout time.Duration
}

// ProdExecutor 生产级执行器。
type ProdExecutor struct {
	repo        ToolRepo
	registry    Registry
	cfg         ExecutorConfig
	accessCheck func(context.Context, []model.Tool) error
}

// NewExecutor 构造。
func NewExecutor(repo ToolRepo, reg Registry, cfg ExecutorConfig) *ProdExecutor {
	return &ProdExecutor{repo: repo, registry: reg, cfg: cfg}
}

// SetAccessCheck is configured during service construction, before concurrent use.
func (e *ProdExecutor) SetAccessCheck(check func(context.Context, []model.Tool) error) {
	e.accessCheck = check
}

// List 按 Agent 的工具绑定 JSON（[{tool_id, config}]）产出模型可见的工具定义。
// 兼容只带 tool_id 的绑定（前端表单）：按 ID 查库回填定义；带 tool_code 的按 code 查询。
func (e *ProdExecutor) List(agentToolsJSON string) ([]llm.ToolDef, error) {
	return e.ListContext(context.Background(), agentToolsJSON)
}

func (e *ProdExecutor) ListContext(ctx context.Context, agentToolsJSON string) ([]llm.ToolDef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bindings, err := ParseAgentBindings(agentToolsJSON)
	if err != nil {
		return nil, err
	}
	if len(bindings) == 0 {
		return nil, nil
	}

	// 按绑定中已有的标识分两路查询（tool_id 必有；tool_code 可选）
	ids := make([]int64, 0, len(bindings))
	codes := make([]string, 0, len(bindings))
	for _, b := range bindings {
		if b.ToolCode != "" {
			codes = append(codes, b.ToolCode)
		} else {
			ids = append(ids, b.ToolID)
		}
	}
	tools, err := e.repo.ListByCodes(ctx, codes)
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		byIDs, err := e.repo.ListByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		tools = append(tools, byIDs...)
	}

	if e.accessCheck != nil {
		if err := e.accessCheck(ctx, tools); err != nil {
			return nil, err
		}
	}
	defs := make([]llm.ToolDef, 0, len(tools))
	seen := map[string]bool{}
	resolvedIDs := map[int64]bool{}
	for _, t := range tools {
		if t.Status != 1 {
			continue
		}
		if e.registry == nil {
			return nil, errcode.ErrToolNotFound
		}
		if _, ok := e.registry.Get(t.Code); !ok {
			return nil, errcode.ErrToolNotFound.WithMsg("工具当前未注册")
		}
		resolvedIDs[t.ID] = true
		if seen[t.Code] { // 两条查询路径可能重复命中
			continue
		}
		seen[t.Code] = true
		var params map[string]any
		if jsonErr := json.Unmarshal(t.Parameters, &params); jsonErr != nil {
			return nil, fmt.Errorf("invalid schema for tool %s: %w", t.Code, jsonErr)
		}
		capability := ""
		if t.Type == model.ToolTypeMCP {
			capability = "mcp"
		}
		defs = append(defs, llm.ToolDef{
			SourceIdentity: t.SourceIdentity,
			BindingVersion: t.BindingVersion,
			Capability:     capability,
			Name:           t.Code,
			Description:    t.Description,
			Parameters:     params,
			Dangerous:      t.IsDangerous,
			ReplayPolicy:   t.ReplayPolicy,
		})
	}
	for _, binding := range bindings {
		if binding.ToolCode != "" && !seen[binding.ToolCode] || binding.ToolCode == "" && !resolvedIDs[binding.ToolID] {
			return nil, errcode.ErrToolNotFound
		}
	}
	return defs, nil
}

// Invoke 执行一次工具调用。
func (e *ProdExecutor) Invoke(ctx context.Context, agentToolsJSON, name, argsJSON string) (res ToolResult) {
	ctx, span := observability.StartSpan(ctx, "tool."+name, attribute.String("tool.name", name))
	start := time.Now()
	res.ToolName = name
	defer func() {
		if recover() != nil {
			res.Status = "error"
			res.Output = "{\"error\":\"tool handler panicked\"}"
		}
		res.Elapsed = time.Since(start).Milliseconds()
		span.SetAttributes(attribute.String("tool.status", res.Status), attribute.Int64("tool.elapsed_ms", res.Elapsed))
		if res.Status != "" && res.Status != "ok" {
			span.SetStatus(codes.Error, res.Status)
		}
		span.End()
	}()
	fail := func(message string) ToolResult {
		res.Status = "error"
		raw, _ := json.Marshal(map[string]string{"error": message})
		res.Output = string(raw)
		return res
	}
	timeout := e.cfg.DefaultTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	definitions, err := e.ListContext(runCtx, agentToolsJSON)
	if err != nil {
		return fail("tool binding or authority unavailable")
	}
	var selected *llm.ToolDef
	for i := range definitions {
		if definitions[i].Name == name {
			selected = &definitions[i]
			break
		}
	}
	if selected == nil {
		return fail("tool is not bound or has been revoked")
	}
	if expected, ok := ExpectedBindingVersion(runCtx, name); ok && expected != selected.BindingVersion {
		return fail("tool binding changed before invocation")
	}
	runCtx = WithBindingVersion(runCtx, name, selected.BindingVersion)
	handler, ok := e.registry.Get(name)
	if !ok {
		return fail("tool handler unavailable")
	}
	if len(argsJSON) > 65536 {
		return fail("tool arguments exceed 64KiB")
	}
	if argsJSON == "" {
		argsJSON = "{}"
	}
	var args map[string]any
	decoder := json.NewDecoder(strings.NewReader(argsJSON))
	decoder.UseNumber()
	if decoder.Decode(&args) != nil || args == nil || decoder.Decode(new(any)) != io.EOF {
		return fail("exactly one JSON object is required")
	}
	if err = validateToolArguments(selected.Parameters, args); err != nil {
		return fail("tool arguments violate the configured schema")
	}
	if metadata, err := e.repo.GetByCode(runCtx, name); err == nil && metadata != nil && metadata.TimeoutMs > 0 {
		var stop context.CancelFunc
		runCtx, stop = context.WithTimeout(runCtx, time.Duration(metadata.TimeoutMs)*time.Millisecond)
		defer stop()
	}
	output, err := handler(runCtx, args, argsJSON)
	switch {
	case runCtx.Err() != nil:
		res.Status = "timeout"
		if err == nil {
			err = runCtx.Err()
		}
	case err != nil:
		res.Status = "error"
	default:
		res.Status = "ok"
	}
	if err != nil && output == "" {
		raw, _ := json.Marshal(map[string]string{"error": err.Error()})
		output = string(raw)
	}
	res.Output = output
	return res
}

// ctxErr 取 ctx 错误（若已超时/取消）。
func ctxErr(ctx context.Context) error {
	return ctx.Err()
}

// AgentToolBinding 解析后的 Agent 工具绑定。
type AgentToolBinding struct {
	ToolID   int64          `json:"tool_id"`
	ToolCode string         `json:"tool_code,omitempty"`
	Config   map[string]any `json:"config,omitempty"`
}

// ParseAgentBindings 解析 agents.tools JSONB。
// 兼容两种格式：[{tool_id}] / [{tool_id, tool_code, config}]（seed 时补 tool_code）。
func ParseAgentBindings(raw string) ([]AgentToolBinding, error) {
	if len(raw) > 65536 {
		return nil, errcode.ErrInvalidParam.WithMsg("工具绑定超过64KiB")
	}
	if raw == "" || raw == "[]" || raw == "null" {
		return nil, nil
	}
	var out []AgentToolBinding
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, errcode.ErrInvalidParam.WithCause(fmt.Errorf("parse agent tools: %w", err))
	}
	if len(out) > 128 {
		return nil, errcode.ErrInvalidParam.WithMsg("Agent工具绑定超过128个")
	}
	return out, nil
}
