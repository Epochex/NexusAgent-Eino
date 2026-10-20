// Package workflow 工作流引擎（td.md §8.8，Ep 12-13）。
//
// 本文件：YAML DSL 解析与静态校验。
// DSL 设计原则（教学点）：编排与执行分离 —— YAML 描述"做什么"，
// 引擎解释"怎么做"；表达式用 expr-lang（与内置 calculator 工具同源）。
//
// 示例 DSL：
//
//	name: 周报生成
//	input: [topic, author]
//	nodes:
//	  - key: search
//	    type: kb
//	    kb_id: 1
//	    query: "{{topic}} 最新进展"
//	  - key: write
//	    type: llm
//	    prompt: |
//	      根据资料撰写周报。资料：{{search.result}}
//	    model: deepseek-chat
//	  - key: audit
//	    type: human
//	    prompt: 请审批周报
//	  - key: send
//	    type: tool
//	    tool_code: http_request
//	    args: {url: "..."}
//	edges: [search, write, audit, send]     # 线性简写
//	# 或结构化：edges: [{from: search, to: write}]
package workflow

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// DSL 工作流定义顶层结构。
type DSL struct {
	Name   string   `yaml:"name" json:"name"`
	Desc   string   `yaml:"desc" json:"desc"`
	Input  []string `yaml:"input" json:"input"` // 工作流入参名
	Nodes  []Node   `yaml:"nodes" json:"nodes"`
	Edges  EdgeList `yaml:"edges" json:"edges"` // 线性简写 [a,b,c] / 结构化 [{from,to}] / 省略时按节点声明顺序线性连接
	OutKey string   `yaml:"out" json:"out"`     // 输出取哪个节点的 result（默认最后节点）
}

// Node 工作流节点。
type Node struct {
	Key      string         `yaml:"key" json:"key"`             // 节点唯一键
	Type     string         `yaml:"type" json:"type"`           // llm/tool/kb/condition/parallel/human/subflow
	Prompt   string         `yaml:"prompt" json:"prompt"`       // llm：提示词模板（{{var}} 插值）
	Model    string         `yaml:"model" json:"model"`         // llm：模型别名（空用默认）
	KbID     int64          `yaml:"kb_id" json:"kb_id"`         // kb：知识库
	Query    string         `yaml:"query" json:"query"`         // kb：检索词模板
	ToolCode string         `yaml:"tool_code" json:"tool_code"` // tool：工具 code
	Args     map[string]any `yaml:"args" json:"args"`           // tool：参数（值支持 {{var}} 模板）
	Expr     string         `yaml:"expr" json:"expr"`           // condition：布尔表达式（对 state 求值）
	Then     string         `yaml:"then" json:"then"`           // condition：为真走 then
	Else     string         `yaml:"else" json:"else"`           // condition：为假走 else
	Branches []string       `yaml:"branches" json:"branches"`   // parallel：并行分支节点 key
	Workflow int64          `yaml:"workflow" json:"workflow"`   // subflow：子工作流 ID
	TimeoutS int            `yaml:"timeout_s" json:"timeout_s"` // 节点超时（秒）
}

// Edge 边（from/to 为节点 key；condition 节点用 label 区分支路）。
type Edge struct {
	From  string `yaml:"from" json:"from"`
	To    string `yaml:"to" json:"to"`
	Label string `yaml:"label" json:"label"` // then/else
}

// EdgeList 支持两种 YAML 形态的边列表：
//
//	线性简写：edges: [a, b, c]      → 相邻两两连边（与包注释的示例一致）
//	结构化：  edges: [{from: a, to: b}] → 逐条解析（condition 需要 label 时用）
//
// 此前 Edge 是结构体，线性简写的字符串列表无法反序列化，导致前端示例 DSL 直接校验失败。
type EdgeList []Edge

func (l *EdgeList) UnmarshalYAML(value *yaml.Node) error {
	// 结构化形态：元素是映射
	if value.Kind == yaml.SequenceNode && len(value.Content) > 0 &&
		value.Content[0].Kind == yaml.MappingNode {
		for _, mapping := range value.Content {
			for i := 0; i+1 < len(mapping.Content); i += 2 {
				key := mapping.Content[i].Value
				if key != "from" && key != "to" && key != "label" {
					return fmt.Errorf("unknown edge field %s", key)
				}
			}
		}
		var es []Edge
		if err := value.Decode(&es); err != nil {
			return err
		}
		*l = es
		return nil
	}
	// 线性简写：元素是字符串 key
	var keys []string
	if err := value.Decode(&keys); err != nil {
		return err
	}
	es := make([]Edge, 0, max(len(keys)-1, 0))
	for i := 0; i+1 < len(keys); i++ {
		es = append(es, Edge{From: keys[i], To: keys[i+1]})
	}
	*l = es
	return nil
}

// ParseDSL 解析 YAML 文本。
func ParseDSL(text string) (*DSL, error) {
	var d DSL
	if len(text) > 1<<20 {
		return nil, errcode.ErrWorkflowDSL.WithMsg("DSL exceeds 1 MiB")
	}
	decoder := yaml.NewDecoder(strings.NewReader(text))
	decoder.KnownFields(true)
	if err := decoder.Decode(&d); err != nil {
		return nil, errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("yaml: %w", err))
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errcode.ErrWorkflowDSL.WithMsg("exactly one YAML document required")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Validate 静态校验：结构完整性 + 键唯一 + 边引用合法 + 无环。
func (d *DSL) Validate() error {
	if d.Name == "" {
		return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("name is required"))
	}
	if len(d.Nodes) == 0 || len(d.Nodes) > 128 {
		return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("at least one node"))
	}

	keys := map[string]bool{}
	for i := range d.Nodes {
		n := &d.Nodes[i]
		if n.Key == "" {
			return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("node[%d] key required", i))
		}
		if len(n.Key) > 64 || strings.HasPrefix(n.Key, "__") || strings.ContainsAny(n.Key, "/.:; \t\n{}") {
			return errcode.ErrWorkflowDSL.WithMsg("invalid/reserved node key %q", n.Key)
		}
		if n.TimeoutS < 0 || n.TimeoutS > 300 {
			return errcode.ErrWorkflowDSL.WithMsg("node timeout must be 0..300 seconds")
		}
		if keys[n.Key] {
			return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("duplicate node key: %s", n.Key))
		}
		keys[n.Key] = true
		if !validNodeType(n.Type) {
			return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("node %s: unknown type %q", n.Key, n.Type))
		}
	}
	owned := map[string]string{}
	for _, n := range d.Nodes {
		if n.Type == "parallel" && (len(n.Branches) == 0 || len(n.Branches) > 32) {
			return errcode.ErrWorkflowDSL.WithMsg("parallel requires 1..32 branches")
		}
		for _, child := range n.Branches {
			if !keys[child] || owned[child] != "" {
				return errcode.ErrWorkflowDSL.WithMsg("invalid or multiply owned branch %q", child)
			}
			owned[child] = n.Key
		}
		for _, target := range []string{n.Then, n.Else} {
			if target != "" && !keys[target] {
				return errcode.ErrWorkflowDSL.WithMsg("unknown branch target %q", target)
			}
		}
		if n.Type == "subflow" && n.Workflow <= 0 {
			return errcode.ErrWorkflowDSL.WithMsg("subworkflow ID required")
		}
		if n.Type == "tool" && n.ToolCode == "" {
			return errcode.ErrWorkflowDSL.WithMsg("tool code required")
		}
		if n.Type == "kb" && n.KbID <= 0 {
			return errcode.ErrWorkflowDSL.WithMsg("knowledge base ID required")
		}
	}

	// 边省略 → 线性连接（教学最常见形态）
	if len(d.Edges) == 0 && len(d.Nodes) > 1 {
		var roots []string
		for _, n := range d.Nodes {
			if owned[n.Key] == "" {
				roots = append(roots, n.Key)
			}
		}
		for i := 0; i+1 < len(roots); i++ {
			d.Edges = append(d.Edges, Edge{From: roots[i], To: roots[i+1]})
		}
	}
	for _, e := range d.Edges {
		if !keys[e.From] || !keys[e.To] {
			return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("edge %s->%s references unknown node", e.From, e.To))
		}
		if owned[e.From] != "" || owned[e.To] != "" {
			return errcode.ErrWorkflowDSL.WithMsg("parallel branch nodes cannot also be root edges")
		}
		if e.Label != "" && e.Label != "then" && e.Label != "else" {
			return errcode.ErrWorkflowDSL.WithMsg("invalid edge label")
		}
		if e.Label != "" {
			for i := range d.Nodes {
				if d.Nodes[i].Key == e.From {
					if d.Nodes[i].Type != "condition" {
						return errcode.ErrWorkflowDSL.WithMsg("labeled edge requires condition")
					}
					if e.Label == "then" {
						if d.Nodes[i].Then != "" && d.Nodes[i].Then != e.To {
							return errcode.ErrWorkflowDSL.WithMsg("conflicting then edge")
						}
						d.Nodes[i].Then = e.To
					} else {
						if d.Nodes[i].Else != "" && d.Nodes[i].Else != e.To {
							return errcode.ErrWorkflowDSL.WithMsg("conflicting else edge")
						}
						d.Nodes[i].Else = e.To
					}
				}
			}
		}
	}
	counts := map[string]int{}
	for _, edge := range d.Edges {
		counts[edge.From]++
	}
	for _, n := range d.Nodes {
		if counts[n.Key] > 1 && n.Type != "condition" {
			return errcode.ErrWorkflowDSL.WithMsg("multiple successors require a condition/parallel node")
		}
	}
	if d.OutKey != "" && !keys[d.OutKey] {
		return errcode.ErrWorkflowDSL.WithMsg("unknown output node")
	}
	if err := d.checkCycle(); err != nil {
		return err
	}
	adj := map[string][]string{}
	for _, edge := range d.Edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
	}
	for _, node := range d.Nodes {
		adj[node.Key] = append(adj[node.Key], node.Branches...)
		if node.Then != "" {
			adj[node.Key] = append(adj[node.Key], node.Then)
		}
		if node.Else != "" {
			adj[node.Key] = append(adj[node.Key], node.Else)
		}
	}
	seen := map[string]bool{}
	var visit func(string)
	visit = func(key string) {
		if seen[key] {
			return
		}
		seen[key] = true
		for _, next := range adj[key] {
			visit(next)
		}
	}
	visit(d.StartKey())
	for _, node := range d.Nodes {
		if !seen[node.Key] {
			return errcode.ErrWorkflowDSL.WithMsg("unreachable node %q", node.Key)
		}
	}
	return nil
}

// checkCycle DFS 染色判环（condition 的 then/else 分支节点视为后继）。
func (d *DSL) checkCycle() error {
	adj := map[string][]string{}
	for _, e := range d.Edges {
		adj[e.From] = append(adj[e.From], e.To)
	}
	for i := range d.Nodes {
		n := &d.Nodes[i]
		if n.Type == "parallel" {
			adj[n.Key] = append(adj[n.Key], n.Branches...)
		}
		if n.Type == "condition" {
			if n.Then != "" {
				adj[n.Key] = append(adj[n.Key], n.Then)
			}
			if n.Else != "" {
				adj[n.Key] = append(adj[n.Key], n.Else)
			}
		}
	}

	const (
		white = 0 // 未访问
		gray  = 1 // 访问中
		black = 2 // 完成
	)
	color := map[string]int{}
	var dfs func(k string) error
	dfs = func(k string) error {
		color[k] = gray
		for _, next := range adj[k] {
			switch color[next] {
			case gray:
				return errcode.ErrWorkflowDSL.WithCause(fmt.Errorf("cycle detected at node %s", next))
			case white:
				if err := dfs(next); err != nil {
					return err
				}
			}
		}
		color[k] = black
		return nil
	}
	for i := range d.Nodes {
		if color[d.Nodes[i].Key] == white {
			if err := dfs(d.Nodes[i].Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// Next 找节点的后继（condition 按 then/else 分支）。
func (d *DSL) Next(key string, condResult bool) string {
	// condition 节点优先看显式分支
	for i := range d.Nodes {
		n := &d.Nodes[i]
		if n.Key == key && n.Type == "condition" {
			target := n.Else
			if condResult {
				target = n.Then
			}
			if target != "" {
				return target
			}
		}
	}
	for _, e := range d.Edges {
		if e.From == key {
			return e.To
		}
	}
	return "" // 终点
}

// StartKey 入口节点（第一条边的 from，或首个节点）。
func (d *DSL) StartKey() string {
	if len(d.Edges) > 0 {
		return d.Edges[0].From
	}
	owned := map[string]bool{}
	for _, n := range d.Nodes {
		for _, branch := range n.Branches {
			owned[branch] = true
		}
	}
	for _, n := range d.Nodes {
		if !owned[n.Key] {
			return n.Key
		}
	}
	return ""
}

// OutKeyOrDefault 输出节点。
func (d *DSL) OutKeyOrDefault() string {
	if d.OutKey != "" {
		return d.OutKey
	}
	return d.Nodes[len(d.Nodes)-1].Key
}

func validNodeType(t string) bool {
	switch t {
	case "llm", "tool", "kb", "condition", "parallel", "human", "subflow":
		return true
	}
	return false
}

// RenderTemplate 渲染 {{var}} 插值模板（缺失变量保持原样，便于调试）。
func RenderTemplate(tpl string, state map[string]any) string {
	if !strings.Contains(tpl, "{{") {
		return tpl
	}
	return templatePattern.ReplaceAllStringFunc(tpl, func(match string) string {
		if value, ok := lookupTemplate(strings.TrimSpace(match[2:len(match)-2]), state); ok {
			return toStringVal(value)
		}
		return match
	})
}

var templatePattern = regexp.MustCompile(`\{\{\s*[^{}]+\s*\}\}`)

func lookupTemplate(path string, state map[string]any) (any, bool) {
	var current any = state
	for _, segment := range strings.Split(path, ".") {
		if mapping, ok := current.(map[string]any); ok {
			value, exists := mapping[segment]
			if !exists {
				return nil, false
			}
			current = value
		} else if segment != "result" {
			return nil, false
		}
	}
	return current, true
}

// toStringVal 状态值转字符串（嵌套结构 JSON 化）。
func toStringVal(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		b, _ := yaml.Marshal(t)
		return strings.TrimRight(string(b), "\n")
	}
}
