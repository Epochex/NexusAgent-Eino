package workflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type ApprovalInfo struct {
	Path     string `json:"path"`
	ArgsHash string `json:"args_hash"`
	Prompt   string `json:"prompt"`
}
type ApprovalDecision struct {
	Approved bool   `json:"approved"`
	ArgsHash string `json:"args_hash"`
}
type graphState struct{ Values map[string]any }

func init() {
	schema.RegisterName[*graphState]("nexus_workflow_state_v1")
	schema.RegisterName[*ApprovalInfo]("nexus_workflow_approval_v1")
}

type compiler struct {
	deps  Deps
	slots chan struct{}
	nodes int
	stack map[int64]bool
}
type workflowGraph = compose.Graph[map[string]any, map[string]any]

// Compile turns the DSL into real Eino branches and nested graphs. All graph
// state is checkpointed by Eino; operation durability is supplied by the driver.
func Compile(ctx context.Context, d *DSL, deps Deps, store compose.CheckPointStore) (compose.Runnable[map[string]any, map[string]any], error) {
	if d == nil || store == nil {
		return nil, fmt.Errorf("definition and checkpoint store required")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	c := &compiler{deps: deps, slots: make(chan struct{}, 4), stack: map[int64]bool{}}
	g, err := c.build(ctx, d, nil, "root", 0)
	if err != nil {
		return nil, err
	}
	return g.Compile(ctx, compose.WithCheckPointStore(store), compose.WithNodeTriggerMode(compose.AllPredecessor), compose.WithGraphName("nexus_workflow_v1"))
}

func (c *compiler) build(ctx context.Context, d *DSL, parallel []string, path string, depth int) (*workflowGraph, error) {
	if depth > 8 {
		return nil, fmt.Errorf("workflow nesting exceeds 8")
	}
	nodes := map[string]Node{}
	owned := map[string]bool{}
	for _, n := range d.Nodes {
		nodes[n.Key] = n
		for _, branch := range n.Branches {
			owned[branch] = true
		}
	}
	var roots []string
	if parallel != nil {
		roots = append(roots, parallel...)
	} else {
		for _, n := range d.Nodes {
			if !owned[n.Key] {
				roots = append(roots, n.Key)
			}
		}
	}
	if len(roots) == 0 {
		return nil, fmt.Errorf("empty executable graph")
	}
	g := compose.NewGraph[map[string]any, map[string]any](compose.WithGenLocalState(func(context.Context) *graphState { return &graphState{Values: map[string]any{}} }))
	if err := g.AddLambdaNode("__input", compose.InvokableLambda(func(_ context.Context, in map[string]any) (map[string]any, error) { return map[string]any{}, nil }), compose.WithStatePreHandler(func(_ context.Context, in map[string]any, s *graphState) (map[string]any, error) {
		copy, err := copyMap(in)
		if err == nil {
			s.Values = copy
		}
		return in, err
	})); err != nil {
		return nil, err
	}
	if err := g.AddLambdaNode("__output", compose.InvokableLambda(func(_ context.Context, in map[string]any) (map[string]any, error) { return in, nil }), compose.WithStatePreHandler(func(_ context.Context, _ map[string]any, s *graphState) (map[string]any, error) {
		if parallel == nil {
			result, err := copyMap(s.Values)
			if err != nil {
				return nil, err
			}
			if d.OutKey != "" {
				value, ok := result[d.OutKey]
				if !ok {
					return nil, fmt.Errorf("output node %s did not execute", d.OutKey)
				}
				result["result"] = value
			}
			return result, nil
		}
		result := map[string]any{}
		for _, key := range parallel {
			result[key] = s.Values[key]
		}
		return copyMap(result)
	})); err != nil {
		return nil, err
	}
	if err := g.AddEdge(compose.START, "__input"); err != nil {
		return nil, err
	}
	next := func(key string, condition bool) string {
		if parallel != nil {
			return "__output"
		}
		target := d.Next(key, condition)
		if target == "" {
			return "__output"
		}
		return target
	}
	snapshot := compose.WithStatePreHandler(func(_ context.Context, _ map[string]any, s *graphState) (map[string]any, error) {
		return copyMap(s.Values)
	})
	for _, key := range roots {
		n, ok := nodes[key]
		if !ok {
			return nil, fmt.Errorf("unknown node %s", key)
		}
		c.nodes++
		if c.nodes > 128 {
			return nil, fmt.Errorf("expanded workflow exceeds 128 nodes")
		}
		nodePath := path + "/" + key
		post := compose.WithStatePostHandler(func(_ context.Context, out map[string]any, s *graphState) (map[string]any, error) {
			for k, v := range out {
				s.Values[k] = v
			}
			return map[string]any{}, nil
		})
		switch n.Type {
		case "parallel", "subflow":
			var child *workflowGraph
			var err error
			if n.Type == "parallel" {
				child, err = c.build(ctx, d, n.Branches, nodePath, depth+1)
			} else {
				if c.deps.LookupWorkflow == nil {
					return nil, fmt.Errorf("frozen subworkflow lookup required")
				}
				if c.stack[n.Workflow] {
					return nil, fmt.Errorf("recursive subworkflow %d", n.Workflow)
				}
				c.stack[n.Workflow] = true
				definition, lookupErr := c.deps.LookupWorkflow(ctx, n.Workflow)
				if lookupErr != nil {
					return nil, lookupErr
				}
				if err = definition.Validate(); err != nil {
					return nil, err
				}
				child, err = c.build(ctx, definition, nil, nodePath, depth+1)
				delete(c.stack, n.Workflow)
			}
			if err != nil {
				return nil, err
			}
			collect := compose.WithStatePostHandler(func(_ context.Context, out map[string]any, s *graphState) (map[string]any, error) {
				s.Values[n.Key] = out
				return map[string]any{}, nil
			})
			if err = g.AddGraphNode(key, child, snapshot, collect, compose.WithGraphCompileOptions(compose.WithNodeTriggerMode(compose.AllPredecessor))); err != nil {
				return nil, err
			}
		default:
			node := n
			if err := g.AddLambdaNode(key, compose.InvokableLambda(func(ctx context.Context, state map[string]any) (map[string]any, error) {
				value, err := c.invoke(ctx, nodePath, &node, state)
				if err != nil {
					return nil, err
				}
				result := map[string]any{node.Key: value}
				if node.Type == "human" {
					result["approved"] = true
				}
				return result, nil
			}), snapshot, post); err != nil {
				return nil, err
			}
		}
	}
	// Eino validates edges eagerly: register every node before adding routes.
	for _, key := range roots {
		n := nodes[key]
		if n.Type == "condition" {
			if parallel != nil {
				return nil, fmt.Errorf("parallel condition requires a subworkflow branch")
			}
			yes, no := next(key, true), next(key, false)
			branch := compose.NewGraphBranch(func(ctx context.Context, _ map[string]any) (string, error) {
				var condition bool
				err := compose.ProcessState(ctx, func(_ context.Context, s *graphState) error { condition, _ = s.Values[key].(bool); return nil })
				if condition {
					return yes, err
				}
				return no, err
			}, map[string]bool{yes: true, no: true})
			if err := g.AddBranch(key, branch); err != nil {
				return nil, err
			}
		} else if err := g.AddEdge(key, next(key, true)); err != nil {
			return nil, err
		}
	}
	if parallel != nil {
		for _, key := range roots {
			if err := g.AddEdge("__input", key); err != nil {
				return nil, err
			}
		}
	} else {
		if err := g.AddEdge("__input", d.StartKey()); err != nil {
			return nil, err
		}
	}
	if err := g.AddEdge("__output", compose.END); err != nil {
		return nil, err
	}
	return g, nil
}

func (c *compiler) invoke(ctx context.Context, path string, n *Node, state map[string]any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timeout := 60 * time.Second
	if n.TimeoutS > 0 {
		timeout = time.Duration(n.TimeoutS) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wasInterrupted, _, _ := compose.GetInterruptState[*ApprovalInfo](ctx)
	var proposal *ApprovalInfo
	if n.Type == "human" {
		proposal = newApproval(path, RenderTemplate(n.Prompt, state), nil)
	}
	if n.Type == "tool" && c.deps.Tools != nil {
		binding, _ := json.Marshal([]map[string]string{{"tool_code": n.ToolCode}})
		defs, err := c.deps.Tools.ListContext(ctx, string(binding))
		if err != nil {
			return nil, err
		}
		if len(defs) != 1 || defs[0].Name != n.ToolCode {
			return nil, fmt.Errorf("tool binding missing")
		}
		if defs[0].Dangerous || wasInterrupted {
			args, err := RenderArgs(n.Args, state)
			if err != nil {
				return nil, err
			}
			raw, _ := json.Marshal(args)
			proposal = newApproval(path, fmt.Sprintf("调用工具 %s：%s", n.ToolCode, raw), defs[0].Parameters)
		}
	}
	if proposal != nil {
		_, hasData, decision := compose.GetResumeContext[ApprovalDecision](ctx)
		if !hasData {
			return nil, compose.StatefulInterrupt(ctx, proposal, proposal)
		}
		if !decision.Approved || decision.ArgsHash != proposal.ArgsHash {
			return nil, fmt.Errorf("approval rejected or arguments changed")
		}
		if n.Type == "human" {
			return true, nil
		}
		args, err := RenderArgs(n.Args, state)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(args)
		ctx = einoadapter.WithToolApproval(ctx, n.ToolCode, string(raw))
	}
	if n.Type == "condition" {
		return evalExpr(n.Expr, state)
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if c.deps.ExecuteNode != nil {
		return c.deps.ExecuteNode(ctx, path, n, state)
	}
	return ExecuteLeaf(ctx, c.deps, n, state)
}

func newApproval(path, prompt string, parameters any) *ApprovalInfo {
	raw, _ := json.Marshal(map[string]any{"path": path, "prompt": prompt, "parameters": parameters})
	sum := sha256.Sum256(raw)
	return &ApprovalInfo{Path: path, Prompt: prompt, ArgsHash: hex.EncodeToString(sum[:])}
}

// ExecuteLeaf retains the existing gateway/RAG and enforces the same schema,
// fresh authority and sensitive-tool capability as ordinary Agent execution.
func ExecuteLeaf(ctx context.Context, deps Deps, n *Node, state map[string]any) (any, error) {
	engine := New(deps)
	switch n.Type {
	case "llm":
		value, _, _, err := engine.runLLM(ctx, n, state)
		if err == nil && strings.TrimSpace(fmt.Sprint(value)) == "" {
			return nil, errcode.ErrModelUnavail.WithMsg("模型未生成有效回答")
		}
		return value, err
	case "kb":
		value, _, _, err := engine.runKB(ctx, n, state)
		return value, err
	case "tool":
		args, err := RenderArgs(n.Args, state)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		binding, _ := json.Marshal([]map[string]string{{"tool_code": n.ToolCode}})
		tools, err := einoadapter.BuildTools(ctx, deps.Tools, string(binding), nil, 60*time.Second, 65536)
		if err != nil {
			return nil, err
		}
		if len(tools) != 1 {
			return nil, fmt.Errorf("tool binding unavailable")
		}
		out, err := tools[0].(einotool.InvokableTool).InvokableRun(ctx, string(raw))
		if err != nil {
			return nil, err
		}
		var observation struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal([]byte(out), &observation) == nil && observation.Error != "" {
			return nil, fmt.Errorf("tool %s: %s", observation.Error, observation.Message)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported leaf %s", n.Type)
	}
}
func copyMap(in map[string]any) (map[string]any, error) {
	if in == nil {
		return map[string]any{}, nil
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, fmt.Errorf("workflow state exceeds 4 MiB")
	}
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&out); err != nil {
		return nil, err
	}
	for key, value := range out {
		out[key] = numericValue(value)
	}
	return out, nil
}
func numericValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case map[string]any:
		for key, value := range v {
			v[key] = numericValue(value)
		}
	case []any:
		for i, value := range v {
			v[i] = numericValue(value)
		}
	}
	return value
}

func RenderArgs(args map[string]any, state map[string]any) (map[string]any, error) {
	var render func(any) (any, error)
	render = func(value any) (any, error) {
		switch v := value.(type) {
		case string:
			if strings.HasPrefix(v, "{{") && strings.HasSuffix(v, "}}") && strings.Count(v, "{{") == 1 {
				if got, ok := lookupTemplate(strings.TrimSpace(v[2:len(v)-2]), state); ok {
					return got, nil
				}
			}
			text := RenderTemplate(v, state)
			if strings.Contains(text, "{{") {
				return nil, fmt.Errorf("unresolved tool argument template")
			}
			return text, nil
		case map[string]any:
			out := map[string]any{}
			for key, x := range v {
				got, err := render(x)
				if err != nil {
					return nil, err
				}
				out[key] = got
			}
			return out, nil
		case []any:
			out := make([]any, len(v))
			for i, x := range v {
				got, err := render(x)
				if err != nil {
					return nil, err
				}
				out[i] = got
			}
			return out, nil
		default:
			return value, nil
		}
	}
	value, err := render(args)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return map[string]any{}, nil
	}
	return value.(map[string]any), nil
}
