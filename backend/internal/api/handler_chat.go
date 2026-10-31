// 对话接口：SSE 流式对话 / 会话与消息管理（td.md §6.4 SSE 协议）。
//
// SSE 事件序列（event: 名 + data: JSON）：
//
//	meta → (reasoning/plan/tool_call/tool_result/agent_event/refs/usage)*
//	     → delta* → done | error
//
// 教学要点：handler 只做"事件流转写"—— Channel ↔ SSE 的桥接，
// 组装/记忆/工具循环全部在 service 与 agent 层。
package api

import (
	"context"
	"encoding/json"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/network"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// ChatReq 对话请求。
type ChatReq struct {
	SessionID string `json:"session_id"`
	AgentID   int64  `json:"agent_id"`
	Content   string `json:"content"`
	// 本次发送使用的模型（对话页选择器）；0/未传 = 沿用 Agent 绑定模型
	ModelConfigID int64 `json:"model_config_id"`
}

// Chat POST /api/v1/chat/stream —— SSE 流式对话。
func (h *Handler) Chat(ctx context.Context, c *app.RequestContext) {
	var req ChatReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}

	streamCtx, cancel := h.streamContext(ctx)
	events, err := h.chat.Stream(streamCtx, userID(c), &service.ChatInput{
		SessionID:     req.SessionID,
		AgentID:       req.AgentID,
		Content:       req.Content,
		ModelConfigID: req.ModelConfigID,
	})
	if err != nil {
		cancel()
		Fail(c, err)
		return
	}

	// 流式响应：把事件 Channel 转写为 SSE。
	// SetBodyStream(reader, -1) = 未知长度的 chunked 流式响应。
	pr, pw := io.Pipe()
	// The response body owns cancellation through the last network flush.
	// Refresh the socket deadline per chunk; an idle model is not a slow reader.
	body := &sseBody{PipeReader: pr, ctx: streamCtx, conn: c.GetConn(), cancel: cancel, timeout: 2 * time.Second}
	body.stop = context.AfterFunc(streamCtx, body.abort)
	c.Response.Header.Set("Content-Type", "text/event-stream")
	c.Response.Header.Set("Cache-Control", "no-cache")
	c.Response.Header.Set("Connection", "keep-alive")
	c.Response.Header.Set("X-Accel-Buffering", "no") // Nginx 不缓冲
	c.Response.SetBodyStream(body, -1)

	// SSE 是长连接：handler 在这里就返回了，但事件流还要写很久。
	// 所以从 Tracing 中间件手里接管根 span，等流真正写完再结束它，
	// 否则 Jaeger 里会看到"根 span 只有几毫秒、子 span 却好几秒"。
	streamSpan := TakeRootSpan(c)
	if streamSpan != nil {
		body.endSpan = func() { streamSpan.End() }
	}

	go func() {
		defer pw.Close()
		heartbeat := 15 * time.Second
		// Hertz connstate does not support Windows. A small comment write wakes
		// the body writer to notice a closed socket even while the model is idle.
		if runtime.GOOS == "windows" {
			heartbeat = 250 * time.Millisecond
		}
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			var ev agent.Event
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				if _, err := io.WriteString(pw, ": keepalive\n\n"); err != nil {
					return
				}
				continue
			case next, ok := <-events:
				if !ok {
					return
				}
				ev = next
			}
			payload := make(map[string]any, len(ev.Data)+3)
			for key, value := range ev.Data {
				payload[key] = value
			}
			payload["run_id"] = ev.RunID
			payload["seq"] = ev.Seq
			payload["time"] = ev.At
			data, err := json.Marshal(payload)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, werr := writeSSE(pw, ev.Type, data); werr != nil {
				return // 客户端断开
			}
		}
	}()
}

// sseBody bridges Hertz's socket writer and the producer without retaining the
// pooled RequestContext. Close is called after the final flush or a write error.
type sseBody struct {
	*io.PipeReader
	ctx     context.Context
	conn    network.Conn
	cancel  context.CancelFunc
	timeout time.Duration
	mu      sync.Mutex
	closed  bool
	aborted bool
	stop    func() bool
	endSpan func()
}

func (b *sseBody) Read(p []byte) (int, error) {
	n, err := b.PipeReader.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	// The producer can close its pipe before AfterFunc schedules abort. Never
	// turn cancellation into a normal chunked EOF and an idle keep-alive socket.
	if b.ctx != nil && b.ctx.Err() != nil {
		b.aborted = true
		// Hertz converts reader errors into a final chunk. Expire the write
		// deadline too, so that flush fails and Hertz releases this connection.
		_ = b.conn.SetWriteDeadline(time.Now())
		return 0, b.ctx.Err()
	}
	if b.aborted {
		return 0, context.Canceled
	}
	if setErr := b.conn.SetWriteTimeout(b.timeout); setErr != nil {
		return 0, setErr
	}
	return n, err
}

func (b *sseBody) abort() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.aborted = true
	// Interrupt Flush without racing Hertz's ownership of Conn.Close/stater.
	_ = b.conn.SetWriteDeadline(time.Now())
	_ = b.PipeReader.CloseWithError(context.Canceled)
}

func (b *sseBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.stop()
	err := b.PipeReader.Close()
	b.cancel()
	_ = b.conn.SetWriteTimeout(0)
	if b.endSpan != nil {
		b.endSpan()
	}
	return err
}

// writeSSE 写一条 SSE 事件。
func writeSSE(w io.Writer, event string, data []byte) (int, error) {
	return io.WriteString(w, "event: "+event+"\ndata: "+string(data)+"\n\n")
}

// ListSessions GET /api/v1/sessions —— 会话列表。
func (h *Handler) ListSessions(ctx context.Context, c *app.RequestContext) {
	q := parsePagination(c)
	list, total, err := h.svcs.Repos.Session.ListSessions(ctx, userID(c), q.Offset(), q.Limit())
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// ListMessages GET /api/v1/sessions/:id/messages —— 会话消息（含决策链 metadata）。
func (h *Handler) ListMessages(ctx context.Context, c *app.RequestContext) {
	sid := c.Param("id")
	session, err := h.svcs.Repos.Session.GetSession(ctx, sid)
	if err != nil {
		Fail(c, err)
		return
	}
	if session.UserID != userID(c) {
		forbidden(c)
		return
	}
	msgs, err := h.svcs.Repos.Session.ListMessages(ctx, sid, 200)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, msgs)
}

// RenameSession PUT /api/v1/sessions/:id —— 会话改名。
func (h *Handler) RenameSession(ctx context.Context, c *app.RequestContext) {
	sid := c.Param("id")
	session, err := h.svcs.Repos.Session.GetSession(ctx, sid)
	if err != nil {
		Fail(c, err)
		return
	}
	if session.UserID != userID(c) {
		forbidden(c)
		return
	}
	var req struct {
		Title string `json:"title"`
	}
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.Repos.Session.RetitleSession(ctx, sid, req.Title); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// DeleteSession DELETE /api/v1/sessions/:id
func (h *Handler) DeleteSession(ctx context.Context, c *app.RequestContext) {
	sid := c.Param("id")
	session, err := h.svcs.Repos.Session.GetSession(ctx, sid)
	if err != nil {
		Fail(c, err)
		return
	}
	if session.UserID != userID(c) {
		forbidden(c)
		return
	}
	if err := h.svcs.Repos.Session.DeleteSession(ctx, sid); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}
