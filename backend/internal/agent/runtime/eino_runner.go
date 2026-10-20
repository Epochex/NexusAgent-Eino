package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/agent/einoadapter"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/cloudwego/eino/adk"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type EinoConfig struct {
	StepTimeout, RunTimeout, SendTimeout                                                   time.Duration
	InputTokenBudget, MaxOutputBytes, MaxModelOutputBytes, EventBuffer, MaxConcurrentTools int
}
type EinoRunner struct {
	gateway einoadapter.GatewayClient
	source  einoadapter.ToolSource
	cfg     EinoConfig
}

func NewEinoRunner(g einoadapter.GatewayClient, source einoadapter.ToolSource, cfg EinoConfig) (*EinoRunner, error) {
	if g == nil || source == nil || cfg.StepTimeout <= 0 || cfg.RunTimeout <= 0 {
		return nil, fmt.Errorf("model gateway, tool source and positive execution timeouts required")
	}
	if cfg.InputTokenBudget < 0 || cfg.MaxOutputBytes < 0 || cfg.MaxModelOutputBytes < 0 || cfg.EventBuffer < 0 || cfg.MaxConcurrentTools < 0 || cfg.SendTimeout < 0 {
		return nil, fmt.Errorf("negative runtime limits")
	}
	if cfg.InputTokenBudget == 0 {
		cfg.InputTokenBudget = 128000
	}
	if cfg.MaxOutputBytes == 0 {
		cfg.MaxOutputBytes = 65536
	}
	if cfg.MaxModelOutputBytes == 0 {
		cfg.MaxModelOutputBytes = 1 << 20
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = 128
	}
	if cfg.MaxConcurrentTools == 0 {
		cfg.MaxConcurrentTools = 4
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = 2 * time.Second
	}
	if cfg.MaxOutputBytes < 64 {
		return nil, fmt.Errorf("tool output limit must be >=64")
	}
	return &EinoRunner{gateway: g, source: source, cfg: cfg}, nil
}

func (r *EinoRunner) Run(ctx context.Context, in *RunInput) (<-chan agent.Event, error) {
	if in == nil || in.Primary == nil {
		return nil, errcode.ErrInvalidParam
	}
	input := *in
	input.Messages = append(input.Messages[:0:0], in.Messages...)
	messages, err := einoadapter.ToMessages(input.Messages)
	if err != nil {
		return nil, err
	}
	model, err := einoadapter.NewChatModel(r.gateway, input.Primary, input.Fallback, r.cfg.StepTimeout)
	if err != nil {
		return nil, err
	}
	tools, err := einoadapter.BuildTools(ctx, r.source, input.ToolsJSON, input.DangerousTools, r.cfg.StepTimeout, r.cfg.MaxOutputBytes)
	if err != nil {
		return nil, err
	}
	var infos []*schema.ToolInfo
	for _, tool := range tools {
		info, err := tool.Info(ctx)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	if err = validateInputBudget(messages, infos, r.cfg.InputTokenBudget); err != nil {
		return nil, err
	}
	if input.RunID == "" {
		input.RunID = rand.Text()
	}
	if input.MaxIterations <= 0 {
		input.MaxIterations = 10
	}
	runCtx, cancel := context.WithTimeout(ctx, r.cfg.RunTimeout)
	sink := newEventSink(r.cfg.EventBuffer, r.cfg.SendTimeout, input.RunID, cancel)
	middleware := &runMiddleware{sink: sink, budget: r.cfg.InputTokenBudget, slots: make(chan struct{}, r.cfg.MaxConcurrentTools), trace: input.Trace, agentName: input.AgentName}
	a, err := adk.NewChatModelAgent(runCtx, &adk.ChatModelAgentConfig{Name: input.AgentName, Model: model, MaxIterations: input.MaxIterations, ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: tools}}, Handlers: []adk.ChatModelAgentMiddleware{middleware}})
	if err != nil {
		cancel()
		sink.Close()
		return nil, err
	}
	go r.loopEino(runCtx, cancel, &input, messages, adk.NewRunner(runCtx, adk.RunnerConfig{Agent: a, EnableStreaming: true}), middleware, sink)
	return sink.events, nil
}

func (r *EinoRunner) loopEino(ctx context.Context, cancel context.CancelFunc, in *RunInput, messages []*schema.Message, runner *adk.Runner, middleware *runMiddleware, sink *eventSink) {
	defer sink.Close()
	defer cancel()
	var runErr error
	var prompt, completion int
	var used, finish string
	var final strings.Builder
	generatedBytes := 0
	if len(in.Refs) > 0 {
		if err := sink.Send(ctx, agent.Evt(agent.TypeRefs, map[string]any{"items": in.Refs})); err != nil {
			return
		}
	}
	iterator := runner.Run(ctx, messages)
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			runErr = event.Err
			cancel()
			break
		}
		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}
		output := event.Output.MessageOutput
		consume := func(msg *schema.Message) error {
			if msg == nil || msg.Role != schema.Assistant {
				return nil
			}
			generatedBytes += len(msg.Content) + len(msg.ReasoningContent)
			for _, call := range msg.ToolCalls {
				generatedBytes += len(call.ID) + len(call.Function.Name) + len(call.Function.Arguments)
			}
			if generatedBytes > r.cfg.MaxModelOutputBytes {
				return errcode.ErrBudget.WithMsg("Agent 生成内容超过字节上限")
			}
			if msg.Content != "" {
				final.WriteString(msg.Content)
				if err := sink.Send(ctx, agent.EvtDelta(msg.Content)); err != nil {
					return err
				}
			}
			if msg.ReasoningContent != "" {
				if err := sink.Send(ctx, agent.EvtReasoning(msg.ReasoningContent)); err != nil {
					return err
				}
			}
			for _, call := range msg.ToolCalls {
				if err := sink.Send(ctx, agent.EvtToolCall(call.ID, call.Function.Name, call.Function.Arguments)); err != nil {
					return err
				}
			}
			if name, ok := msg.Extra["model"].(string); ok && name != "" {
				used = name
			}
			if msg.ResponseMeta != nil {
				if msg.ResponseMeta.FinishReason != "" {
					finish = msg.ResponseMeta.FinishReason
				}
				if usage := msg.ResponseMeta.Usage; usage != nil {
					prompt += usage.PromptTokens
					completion += usage.CompletionTokens
				}
			}
			return nil
		}
		if output.IsStreaming {
			stream := output.MessageStream
			for {
				msg, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					runErr = err
					break
				}
				if err = consume(msg); err != nil {
					runErr = err
					break
				}
			}
			stream.Close()
		} else {
			runErr = consume(output.Message)
		}
		if runErr != nil {
			cancel()
			break
		}
	}
	if metrics := observability.MetricsInstance(); metrics != nil {
		metrics.AgentIterations.Observe(float64(middleware.steps.Load()))
	}
	if sink.Failed() {
		return
	}
	// Preserve the original error before canceling model/tool work. A provider
	// failure must not turn into "user canceled" because cleanup cancels ctx.
	if runErr == nil {
		runErr = ctx.Err()
	}
	finalCtx, done := context.WithTimeout(context.Background(), r.cfg.SendTimeout)
	defer done()
	usage := agent.EvtUsage(prompt, completion, "", used)
	usage.Data["cost_available"] = false
	if err := sink.Send(finalCtx, usage); err != nil {
		return
	}
	var terminal agent.Event
	switch {
	case errors.Is(runErr, context.Canceled):
		terminal = agent.EvtDone("canceled")
	case errors.Is(runErr, context.DeadlineExceeded):
		terminal = agent.EvtError(errcode.ErrModelTimeout.Code, "Agent 运行或模型调用超时")
	case errors.Is(runErr, adk.ErrExceedMaxIterations):
		terminal = agent.EvtError(errcode.ErrMaxIteration.Code, errcode.ErrMaxIteration.Message)
	case runErr != nil:
		be := errcode.From(runErr)
		terminal = agent.EvtError(be.Code, be.Message)
	default:
		if finish == "" {
			finish = "stop"
		}
		terminal = agent.EvtDone(finish)
		if in.Trace != nil {
			step := in.Trace.Add(observability.EventAnswer, in.AgentName, "")
			in.Trace.Done(step, final.String(), "ok")
		}
	}
	_ = sink.Send(finalCtx, terminal)
}

type runMiddleware struct {
	adk.BaseChatModelAgentMiddleware
	sink      *eventSink
	budget    int
	slots     chan struct{}
	steps     atomic.Int64
	trace     *observability.AgentTraceCollector
	agentName string
}

func (m *runMiddleware) BeforeModelRewriteState(ctx context.Context, state *adk.ChatModelAgentState, _ *adk.ModelContext) (context.Context, *adk.ChatModelAgentState, error) {
	if err := validateInputBudget(state.Messages, state.ToolInfos, m.budget); err != nil {
		return ctx, state, err
	}
	m.steps.Add(1)
	return ctx, state, nil
}
func (m *runMiddleware) AfterModelRewriteState(ctx context.Context, state *adk.ChatModelAgentState, _ *adk.ModelContext) (context.Context, *adk.ChatModelAgentState, error) {
	if len(state.Messages) == 0 {
		return ctx, state, fmt.Errorf("empty model state")
	}
	last := state.Messages[len(state.Messages)-1]
	if len(last.ToolCalls) > 32 {
		return ctx, state, errcode.ErrInvalidParam.WithMsg("单步工具调用数量超过 32")
	}
	seen := map[string]bool{}
	for _, call := range last.ToolCalls {
		if call.ID == "" || seen[call.ID] {
			return ctx, state, errcode.ErrInvalidParam.WithMsg("工具调用 ID 缺失或重复")
		}
		seen[call.ID] = true
	}
	// Validate the final model step, rather than text accumulated before a
	// tool call. Reasoning alone is not a user-facing answer in this runner.
	if len(last.ToolCalls) == 0 && strings.TrimSpace(last.Content) == "" {
		return ctx, state, errcode.ErrModelUnavail.WithMsg("模型未生成有效回答")
	}
	return ctx, state, nil
}
func (m *runMiddleware) WrapInvokableToolCall(_ context.Context, next adk.InvokableToolCallEndpoint, tool *adk.ToolContext) (adk.InvokableToolCallEndpoint, error) {
	name, id := tool.Name, tool.CallID
	return func(ctx context.Context, args string, opts ...einotool.Option) (string, error) {
		select {
		case m.slots <- struct{}{}:
			defer func() { <-m.slots }()
		case <-ctx.Done():
			return "", ctx.Err()
		}
		start := time.Now()
		var traceStep *observability.TraceStep
		if m.trace != nil {
			traceStep = m.trace.AddTool(m.agentName, name, args)
		}
		output, err := next(ctx, args, opts...)
		status := "ok"
		var observation struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal([]byte(output), &observation)
		if err != nil || observation.Error != "" {
			status = "error"
		}
		if observation.Error == "timeout" {
			status = "timeout"
		}
		if observation.Error == "approval_required" {
			status = "denied"
		}
		if m.trace != nil {
			m.trace.Done(traceStep, output, status)
		}
		if sendErr := m.sink.Send(ctx, agent.EvtToolResult(id, output, time.Since(start).Milliseconds(), status)); sendErr != nil {
			return "", sendErr
		}
		return output, err
	}, nil
}
