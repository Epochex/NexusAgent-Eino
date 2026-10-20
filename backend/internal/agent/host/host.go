// Package host Host-Expert 多智能体协作（td.md §8.3.2，Ep 11）。
//
// 职责分工：
//
//	Host（路由者）：理解意图 → 选择最合适的 Expert → 委派 → 聚合转述
//	Expert（执行者）：具备专属 system prompt / 工具集 / 模型配置的 ReAct Agent
//
// 教学要点（与单 Agent 的本质区别）：
//
//	① 上下文隔离：每个 Expert 独立上下文，避免工具集互相干扰；
//	② 意图路由：Host 用"选择"而非"执行"，Token 更省、可维护性更强；
//	③ 事件透传：Expert 的事件流原样转发 + 补充 delegate/merge 标记，
//	   前端可完整展示"Host 委派 → Expert 干活 → Host 收口"全过程。
package host

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/runtime"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// Expert 一个专家 Agent 的配置（由 service 层从 agents 表组装）。
type Expert struct {
	Name           string               // 显示名
	Description    string               // 能力描述（路由依据，写给 Host 看）
	System         string               // 专属 system prompt
	ToolsJSON      string               // agents.tools JSONB
	Primary        *llm.ModelDescriptor // 主模型
	Fallback       *llm.ModelDescriptor // 备模型
	MaxIter        int
	Runner         runtime.Runner // 每个 Expert 独立运行器
	DangerousTools map[string]bool
}

// RouterChat 路由分类调用：Host 用主模型做一次"选人"非流式调用。
type RouterChat func(ctx context.Context, primary, fallback *llm.ModelDescriptor, prompt string) (*llm.ChatResponse, *llm.ModelDescriptor, error)

// DirectRunner 无专家匹配时的直答 Runner（零工具）。
type DirectRunner func(ctx context.Context, in *runtime.RunInput) (<-chan agent.Event, error)

// HostRouter Host 路由 Agent（实现 runtime.Runner 接口，可无缝替换单 Agent）。
type HostRouter struct {
	experts      []Expert
	chat         RouterChat   // 路由调用（service 注入，避免 host 直依赖 gateway 装配）
	directRunner DirectRunner // 直答 Runner
	buffer       int
	sendTimeout  time.Duration
	runTimeout   time.Duration
}

// New 构造。
func New(experts []Expert, chat RouterChat, direct DirectRunner) *HostRouter {
	return &HostRouter{experts: experts, chat: chat, directRunner: direct, buffer: 128, sendTimeout: 2 * time.Second, runTimeout: 300 * time.Second}
}

func (h *HostRouter) WithRunTimeout(timeout time.Duration) *HostRouter {
	next := *h
	if timeout > 0 {
		next.runTimeout = timeout
	}
	return &next
}

func (h *HostRouter) WithStreamLimits(buffer int, timeout time.Duration) *HostRouter {
	next := *h
	if buffer > 0 {
		next.buffer = buffer
	}
	if timeout > 0 {
		next.sendTimeout = timeout
	}
	return &next
}

// Run isolates caller input and owns cancellation for routing and the child run.
func (h *HostRouter) Run(ctx context.Context, in *runtime.RunInput) (<-chan agent.Event, error) {
	if in == nil {
		return nil, errcode.ErrInvalidParam
	}
	input := *in
	input.Messages = append(input.Messages[:0:0], in.Messages...)
	if input.RunID == "" {
		input.RunID = rand.Text()
	}
	runCtx, cancel := context.WithTimeout(ctx, h.runTimeout)
	events := make(chan agent.Event, h.buffer)
	go func() {
		defer close(events)
		defer cancel()
		var seq uint64
		emit := func(ev agent.Event) bool {
			sendCtx := runCtx
			if (ev.Type == agent.TypeDone || ev.Type == agent.TypeError) && runCtx.Err() != nil {
				var done context.CancelFunc
				sendCtx, done = context.WithTimeout(context.WithoutCancel(runCtx), h.sendTimeout)
				defer done()
			}
			timer := time.NewTimer(h.sendTimeout)
			defer timer.Stop()
			ev.RunID = input.RunID
			ev.Seq = seq + 1
			ev.At = time.Now().UTC()
			select {
			case events <- ev:
				seq++
				return true
			case <-sendCtx.Done():
				return false
			case <-timer.C:
				cancel()
				return false
			}
		}
		h.loop(runCtx, &input, emit)
	}()
	return events, nil
}

func (h *HostRouter) loop(ctx context.Context, in *runtime.RunInput, emit func(agent.Event) bool) {
	question := lastUserMessage(in.Messages)
	if question == "" {
		emit(agent.EvtError(errcode.ErrInvalidParam.Code, "empty question"))
		return
	}
	expert, err := h.route(ctx, in, question)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			emit(agent.EvtError(errcode.ErrModelTimeout.Code, "Host 路由超时"))
		} else if errors.Is(err, context.Canceled) {
			emit(agent.EvtDone("canceled"))
		} else {
			emit(agent.EvtError(errcode.From(err).Code, errcode.From(err).Message))
		}
		return
	}
	var child <-chan agent.Event
	input := *in
	merge := false
	expertName := "host"
	if expert == nil {
		input.ToolsJSON = "[]"
		input.DangerousTools = nil
		if h.directRunner == nil {
			emit(agent.EvtError(errcode.ErrNotImplement.Code, "host direct runner not configured"))
			return
		}
		if !emit(agent.EvtAgentEvent("delegate", "host", "host 直答")) {
			return
		}
		child, err = h.directRunner(ctx, &input)
	} else {
		expertName = expert.Name
		input.AgentName = expert.Name
		input.ToolsJSON = expert.ToolsJSON
		input.DangerousTools = expert.DangerousTools
		input.Primary, input.Fallback = expert.Primary, expert.Fallback
		input.Messages = buildExpertMessages(expert.System, in.Messages)
		if expert.MaxIter > 0 {
			input.MaxIterations = expert.MaxIter
		}
		if !emit(agent.EvtAgentEvent("delegate", expert.Name, expert.Description)) || !emit(agent.Evt(agent.TypeMeta, map[string]any{"expert": expert.Name})) {
			return
		}
		if expert.Runner == nil {
			emit(agent.EvtError(errcode.ErrNotImplement.Code, "expert runner not configured"))
			return
		}
		child, err = expert.Runner.Run(ctx, &input)
		merge = true
	}
	if err != nil {
		emit(agent.EvtError(errcode.From(err).Code, errcode.From(err).Message))
		return
	}
	var final strings.Builder
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				emit(agent.EvtError(errcode.ErrModelTimeout.Code, "Host 运行超时"))
			} else {
				emit(agent.EvtDone("canceled"))
			}
			return
		case ev, ok := <-child:
			if !ok {
				emit(agent.EvtError(errcode.ErrInternal.Code, "expert stream ended without terminal"))
				return
			}
			if ev.Type == agent.TypeError {
				emit(ev)
				return
			}
			if ev.Type == agent.TypeDone {
				if merge && ev.Data["finish_reason"] != "canceled" {
					if !emit(agent.EvtAgentEvent("merge", expertName, truncateRunes(final.String(), 120))) {
						return
					}
				}
				emit(ev)
				return
			}
			if ev.Type == agent.TypeDelta {
				final.WriteString(str(ev.Data["content"]))
			}
			if !emit(ev) {
				return
			}
		}
	}
}

// route 意图路由（LLM 分类）。
func (h *HostRouter) route(ctx context.Context, in *runtime.RunInput, question string) (*Expert, error) {
	if len(h.experts) == 0 {
		return nil, nil
	}

	var b strings.Builder
	for i, e := range h.experts {
		fmt.Fprintf(&b, "%d. %s: %s\n", i+1, e.Name, e.Description)
	}
	prompt := fmt.Sprintf(
		"你是任务路由器。根据用户问题，从下列专家中选择最合适的一位。\n"+
			"只输出专家序号（如 2）；都不合适输出 0。\n\n专家清单:\n%s\n用户问题: %s",
		b.String(), question)

	// 路由用 Host 自己的模型（网关失败转移与重试兜底）
	resp, _, err := h.chatForRoute(ctx, in, prompt)
	if err != nil {
		return nil, err
	}

	idx := parseRouteIndex(resp.Content, len(h.experts))
	if idx == 0 {
		return nil, nil
	}
	return &h.experts[idx-1], nil
}

// chatForRoute 路由分类调用（Host 的主模型）。
func (h *HostRouter) chatForRoute(ctx context.Context, in *runtime.RunInput, prompt string) (*llm.ChatResponse, *llm.ModelDescriptor, error) {
	if h.chat == nil {
		return nil, nil, errcode.ErrNotImplement.WithMsg("host router chat function not configured")
	}
	return h.chat(ctx, in.Primary, in.Fallback, prompt)
}

// buildExpertMessages 专家上下文组装：专属 system + 最近对话。
func buildExpertMessages(system string, msgs []llm.Message) []llm.Message {
	out := make([]llm.Message, 0, len(msgs)+1)
	if system != "" {
		out = append(out, llm.Message{Role: llm.RoleSystem, Content: system})
	}
	// 跳过原 system（专家有自己的）
	for _, m := range msgs {
		if m.Role == llm.RoleSystem {
			continue
		}
		out = append(out, m)
	}
	return out
}

// lastUserMessage 取最后一条用户消息。
func lastUserMessage(msgs []llm.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == llm.RoleUser {
			return msgs[i].Content
		}
	}
	return ""
}

// parseRouteIndex 解析路由输出（容错：提取 0~max 的数字）。
func parseRouteIndex(content string, max int) int {
	var n int
	if err := json.Unmarshal([]byte(strings.TrimSpace(content)), &n); err == nil {
		if n >= 0 && n <= max {
			return n
		}
	}
	// 容错扫描数字字符
	for _, r := range content {
		if r >= '0' && r <= '9' {
			v := int(r - '0')
			if v <= max {
				return v
			}
			return 0 // 序号超出范围视为不匹配
		}
	}
	return 0
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// truncateRunes 截断（决策链摘要用）。
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
