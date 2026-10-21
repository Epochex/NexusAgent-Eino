package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
)

type chatRun struct {
	Warnings    []string
	ID          string
	Session     *model.Session
	Agent       *model.Agent
	UserMessage *model.Message
	Refs        []engine.Ref
	Trace       *observability.AgentTraceCollector
}

var errSlowChatConsumer = errors.New("chat subscriber exceeded delivery deadline")

// pump owns the final outcome. Text is provisional; done is published only
// after CreateMessage has committed. It never silently drops a full channel.
func (s *ChatService) pump(ctx context.Context, cancel context.CancelFunc, run chatRun, events <-chan agent.Event, out chan<- agent.Event) {
	defer close(out)
	defer cancel()
	timeout := s.svcs.Cfg.Agent.SendTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var seq uint64
	emit := func(sendCtx context.Context, ev agent.Event) error {
		seq++
		ev.RunID = run.ID
		ev.Seq = seq
		ev.At = time.Now().UTC()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-sendCtx.Done():
			return sendCtx.Err()
		default:
		}
		select {
		case out <- ev:
			return nil
		case <-sendCtx.Done():
			return sendCtx.Err()
		case <-timer.C:
			return errSlowChatConsumer
		}
	}
	started := time.Now()
	firstToken := -1
	deliveryErr := emit(ctx, agent.Evt(agent.TypeMeta, map[string]any{"session_id": run.Session.ID, "agent": run.Agent.Name, "message_id": run.UserMessage.ID, "preparation_warnings": run.Warnings}))
	var content, reasoning strings.Builder
	var protocolHistory chatProtocolHistory
	var calls []llm.ToolCall
	var prompt, completion int
	var modelName string
	var terminal *agent.Event
	if deliveryErr != nil {
		cancel()
	}
consume:
	for deliveryErr == nil {
		select {
		case <-ctx.Done():
			deliveryErr = ctx.Err()
			break consume
		case ev, ok := <-events:
			if !ok {
				break consume
			}
			if terminal != nil {
				next := agent.EvtError(errcode.ErrInternal.Code, "终态后收到额外事件")
				terminal = &next
				cancel()
				break consume
			}
			if ev.Type == agent.TypeDone || ev.Type == agent.TypeError {
				copy := ev
				terminal = &copy
				continue
			}
			protocolHistory.add(ev)
			switch ev.Type {
			case agent.TypeDelta:
				if firstToken < 0 {
					firstToken = int(time.Since(started).Milliseconds())
				}
				content.WriteString(strVal(ev.Data["content"]))
			case agent.TypeReasoning:
				reasoning.WriteString(strVal(ev.Data["content"]))
			case agent.TypeToolCall:
				calls = append(calls, llm.ToolCall{ID: strVal(ev.Data["id"]), Name: strVal(ev.Data["name"]), ArgsJSON: strVal(ev.Data["args"])})
			case agent.TypeUsage:
				prompt = intOf(ev.Data["prompt_tokens"])
				completion = intOf(ev.Data["completion_tokens"])
				modelName = strVal(ev.Data["model"])
			}
			if deliveryErr = emit(ctx, ev); deliveryErr != nil {
				cancel()
				break consume
			}
		}
	}
	status := model.MsgStatusFailed
	switch {
	case errors.Is(deliveryErr, errSlowChatConsumer):
		ev := agent.EvtError(errcode.ErrRateLimited.Code, "客户端消费过慢，生成已停止")
		terminal = &ev
	case errors.Is(deliveryErr, context.Canceled):
		status = model.MsgStatusStopped
		ev := agent.EvtDone("canceled")
		terminal = &ev
	case deliveryErr != nil:
		ev := agent.EvtError(errcode.ErrModelTimeout.Code, "对话已超时")
		terminal = &ev
	case terminal == nil:
		ev := agent.EvtError(errcode.ErrInternal.Code, "模型流未正常结束")
		terminal = &ev
	case terminal.Type == agent.TypeDone:
		status = model.MsgStatusDone
		if terminal.Data["finish_reason"] == "canceled" {
			status = model.MsgStatusStopped
		}
	}
	assistant := &model.Message{SessionID: run.Session.ID, Role: llm.RoleAssistant, Content: content.String(), ReasoningContent: reasoning.String(), ModelName: modelName, PromptTokens: prompt, CompletionTokens: completion, Status: status, LatencyMs: int(time.Since(started).Milliseconds())}
	if firstToken >= 0 {
		assistant.FirstTokenMs = firstToken
	}
	if len(calls) > 0 {
		assistant.ToolCalls, _ = json.Marshal(calls)
	}
	if len(run.Refs) > 0 {
		assistant.Refs, _ = json.Marshal(run.Refs)
	}
	metadata := map[string]any{}
	if status == model.MsgStatusDone {
		if history := protocolHistory.complete(); len(history) > 0 {
			metadata["model_history"] = history
		}
	}
	if run.Trace != nil {
		metadata["agent_trace"] = run.Trace.Snapshot()
	}
	if len(run.Warnings) > 0 {
		metadata["preparation_warnings"] = run.Warnings
	}
	assistant.Metadata, _ = json.Marshal(metadata)
	// Request cancellation must not cancel the final write. The independent
	// deadline also bounds lock waits and outages, including usage/stat updates.
	persistCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer done()
	repo := s.svcs.Repos.Session
	if err := repo.CreateMessage(persistCtx, assistant); err != nil {
		observability.LogWarn("persist assistant message failed", "run_id", run.ID, "session_id", run.Session.ID, "error", err.Error())
		ev := agent.EvtError(errcode.ErrInternal.Code, "消息保存失败，请刷新会话确认结果")
		ev.Data["persisted"] = false
		if errors.Is(err, capacity.ErrLeaseLost) {
			// The successful INSERT rolled back. Preserve the provisional answer
			// as failed, without granting a stale executor success authority.
			assistant.ID = 0
			assistant.Status = model.MsgStatusFailed
			ev = agent.EvtError(errcode.ErrInternal.Code, "执行租约已失效，答案未确认成功")
			ev.Data["persisted"] = false
			if saveErr := repo.CreateMessage(persistCtx, assistant); saveErr == nil {
				ev.Data["persisted"] = true
				ev.Data["assistant_message_id"] = assistant.ID
			}
		}
		terminal = &ev
	} else {
		// Optional accounting cannot turn a committed answer into a failed answer.
		if err := repo.UpdateSessionStats(persistCtx, run.Session.ID, int64(prompt+completion)); err != nil {
			observability.LogWarn("update chat statistics failed", "run_id", run.ID)
		}
		if err := repo.RetitleSession(persistCtx, run.Session.ID, truncateTitle(run.UserMessage.Content)); err != nil {
			observability.LogWarn("update chat title failed", "run_id", run.ID)
		}
		if err := s.svcs.Repos.Usage.CreateUsage(persistCtx, &model.UsageLog{UserID: run.Session.UserID, AgentID: run.Agent.ID, SessionID: &run.Session.ID, MessageID: assistant.ID, Scene: model.SceneChat, Model: modelName, PromptTokens: prompt, CompletionTokens: completion}); err != nil {
			observability.LogWarn("write usage record failed", "run_id", run.ID)
		}
		data := make(map[string]any, len(terminal.Data)+2)
		for key, value := range terminal.Data {
			data[key] = value
		}
		data["assistant_message_id"] = assistant.ID
		data["persisted"] = true
		terminal.Data = data
		if status == model.MsgStatusDone {
			s.extractMemoryAfterChat(ctx, run)
		}
	}
	if !errors.Is(deliveryErr, errSlowChatConsumer) {
		finalCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer stop()
		_ = emit(finalCtx, *terminal)
	}
}

func (s *ChatService) extractMemoryAfterChat(ctx context.Context, run chatRun) {
	every := s.svcs.Cfg.Agent.MemoryExtractEvery
	if !run.Agent.MemoryLongEnabled || s.memory == nil || every <= 0 {
		return
	}
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if _, _, err := s.memory.EnqueueSession(bg, run.Session.UserID, run.Session.ID, false, every, s.svcs.Cfg.Worker.MaxRetry); err != nil {
		observability.LogWarn("memory extraction enqueue failed", "run_id", run.ID)
	}
}
