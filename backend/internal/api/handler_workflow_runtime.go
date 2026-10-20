package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/cloudwego/hertz/pkg/app"
)

func (h *Handler) CancelRun(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	run, err := h.svcs.Workflow.Cancel(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, run)
}
func (h *Handler) RunOperations(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	ops, err := h.svcs.Workflow.Operations(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, ops)
}
func (h *Handler) ReconcileRun(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	var req struct {
		OperationID string          `json:"operation_id"`
		ArgsHash    string          `json:"args_hash"`
		Output      json.RawMessage `json:"output"`
		Evidence    string          `json:"evidence"`
	}
	if err = c.BindAndValidate(&req); err != nil {
		Fail(c, errcode.ErrInvalidParam.WithCause(err))
		return
	}
	run, err := h.svcs.Workflow.Reconcile(ctx, userID(c), id, req.OperationID, req.ArgsHash, string(req.Output), req.Evidence)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, run)
}

// JSON is a bounded replay page; SSE tails the same authoritative event log.
// A subscription owns only its reader context, never the persisted execution.
func (h *Handler) RunEvents(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	raw := string(c.Query("after"))
	if raw == "" {
		raw = string(c.GetHeader("Last-Event-ID"))
	}
	if raw == "" {
		raw = "0"
	}
	after, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || after < 0 {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	owner := userID(c)
	events, err := h.svcs.Workflow.Events(ctx, owner, id, after)
	if err != nil {
		Fail(c, err)
		return
	}
	if !strings.Contains(string(c.GetHeader("Accept")), "text/event-stream") {
		OK(c, events)
		return
	}
	// Maximum subscriber lifetime bounds idle sockets; clients resume by sequence.
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	pr, pw := io.Pipe()
	body := &sseBody{PipeReader: pr, conn: c.GetConn(), cancel: cancel, timeout: 2 * time.Second}
	body.stop = context.AfterFunc(streamCtx, body.abort)
	if span := TakeRootSpan(c); span != nil {
		body.endSpan = func() { span.End() }
	}
	c.Response.Header.Set("Content-Type", "text/event-stream")
	c.Response.Header.Set("Cache-Control", "no-cache")
	c.Response.Header.Set("X-Accel-Buffering", "no")
	c.Response.SetBodyStream(body, -1)
	go func() {
		defer pw.Close()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			for _, event := range events {
				data, e := json.Marshal(event)
				if e != nil {
					return
				}
				if _, e = fmt.Fprintf(pw, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, data); e != nil {
					return
				}
				after = event.Seq
			}
			// Drain all pages before checking terminal status to avoid dropping the tail.
			if len(events) < 128 {
				run, e := h.svcs.Workflow.GetWorkflowRun(streamCtx, owner, id)
				if e != nil {
					return
				}
				if (run.Status == execution.Succeeded || run.Status == execution.Failed || run.Status == execution.Canceled) && after >= run.LastSeq {
					return
				}
				select {
				case <-streamCtx.Done():
					return
				case <-ticker.C:
				}
				if _, e = io.WriteString(pw, ": keepalive\n\n"); e != nil {
					return
				}
			}
			events, err = h.svcs.Workflow.Events(streamCtx, owner, id, after)
			if err != nil {
				data, _ := json.Marshal(map[string]string{"message": "event replay unavailable; fetch run snapshot and reconnect"})
				_, _ = writeSSE(pw, "error", data)
				return
			}
		}
	}()
}
