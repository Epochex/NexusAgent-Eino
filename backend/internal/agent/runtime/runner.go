// Agent 运行时：生产级 ReAct 循环（td.md §8.3）。
//
// 与 naive 版的差异（Ep 03 分屏对照的讲点）：
//
//	① 事件流架构：Run 返回 <-chan Event，SSE/WebSocket/CLI 消费同一份事件
//	② 决策链采集：每一步 Thought/Action/Observation 进 agent_trace
//	③ 指标与安全阀完整：迭代上限 / 单步+总超时 / Token 预算
//	④ 危险工具标记（HITL 由 workflow 层做审批编排）
package runtime

import (
	"context"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
)

// Runner Agent 运行器统一接口（Host/Expert/CLI 都面向它编程）。
type Runner interface {
	Run(ctx context.Context, in *RunInput) (<-chan agent.Event, error)
}

// RunInput 一次 Agent 运行的输入（service 层组装）。
type RunInput struct {
	PreparationWarnings []string
	RunID               string
	SessionID           string                             // 会话（trace 用）
	AgentName           string                             // agent 显示名
	Messages            []llm.Message                      // 组装好的消息（含记忆/RAG 上下文）
	ToolsJSON           string                             // agents.tools JSONB
	Primary             *llm.ModelDescriptor               // 主模型
	Fallback            *llm.ModelDescriptor               // 备用模型
	MaxIterations       int                                // 迭代上限
	Refs                []map[string]any                   // RAG 引用（随流推送）
	Trace               *observability.AgentTraceCollector // 决策链收集器
	DangerousTools      map[string]bool                    // 危险工具集合（is_dangerous）
}
