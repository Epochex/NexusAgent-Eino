// 工作流接口：CRUD / 校验 / 执行 / HITL 审批。
package api

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// ListWorkflows GET /api/v1/workflows
func (h *Handler) ListWorkflows(ctx context.Context, c *app.RequestContext) {
	list, total, err := h.svcs.Workflow.List(ctx, userID(c), parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// GetWorkflow GET /api/v1/workflows/:id
func (h *Handler) GetWorkflow(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	w, err := h.svcs.Workflow.Get(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, w)
}

// CreateWorkflow POST /api/v1/workflows
func (h *Handler) CreateWorkflow(ctx context.Context, c *app.RequestContext) {
	w := &model.Workflow{}
	if err := c.BindAndValidate(w); err != nil {
		Fail(c, err)
		return
	}
	w.CreatorID = userID(c) // creator_id 非空外键：以当前登录用户为创建者
	if err := h.svcs.Workflow.Create(ctx, w); err != nil {
		Fail(c, err)
		return
	}
	OK(c, w)
}

// UpdateWorkflow PUT /api/v1/workflows/:id
func (h *Handler) UpdateWorkflow(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var req struct {
		Name        *string `json:"name"`
		Version     *int    `json:"version"`
		Description *string `json:"description"`
		DSL         *string `json:"dsl"`
		IsActive    *bool   `json:"is_active"`
		Status      *int16  `json:"status"`
	}
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.Workflow.Update(ctx, userID(c), id, &service.WorkflowUpdateInput{
		Name: req.Name, Description: req.Description, DSL: req.DSL,
		Version:  req.Version,
		IsActive: req.IsActive, Status: req.Status,
	}); err != nil {
		Fail(c, err)
		return
	}
	w, err := h.svcs.Workflow.Get(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, w)
}

// DeleteWorkflow DELETE /api/v1/workflows/:id
func (h *Handler) DeleteWorkflow(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.Workflow.Delete(ctx, userID(c), id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// ValidateWorkflow POST /api/v1/workflows/validate —— DSL 静态校验。
func (h *Handler) ValidateWorkflow(ctx context.Context, c *app.RequestContext) {
	var req struct {
		DSL string `json:"dsl"`
	}
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.Workflow.Validate(req.DSL); err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"valid": true})
}

// RunWorkflowReq 执行请求。
type RunWorkflowReq struct {
	WorkflowID int64           `json:"workflow_id"`
	Input      json.RawMessage `json:"input"`
	RequestKey string          `json:"request_key"`
}

// RunWorkflow persists a submission; the worker owns subsequent execution.
func (h *Handler) RunWorkflow(ctx context.Context, c *app.RequestContext) {
	var req RunWorkflowReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, errcode.ErrInvalidParam.WithCause(err))
		return
	}
	input := map[string]any{}
	if len(req.Input) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(req.Input))
		decoder.UseNumber()
		if err := decoder.Decode(&input); err != nil || input == nil {
			Fail(c, errcode.ErrInvalidParam)
			return
		}
	}
	headerKey := string(c.GetHeader("Idempotency-Key"))
	if headerKey != "" {
		if req.RequestKey != "" && req.RequestKey != headerKey {
			Fail(c, errcode.ErrInvalidParam)
			return
		}
		req.RequestKey = headerKey
	}
	run, err := h.svcs.Workflow.Run(ctx, userID(c), &service.RunInput{
		WorkflowID: req.WorkflowID, Input: input, RequestKey: req.RequestKey,
	})
	if err != nil {
		Fail(c, err)
		return
	}
	c.JSON(202, envelope{Code: 0, Message: "accepted", Data: run})
}

// ApproveReq HITL 审批请求。
type ApproveReq struct {
	InterruptID string `json:"interrupt_id"`
	ArgsHash    string `json:"args_hash"`
	Approved    bool   `json:"approved"`
	Comment     string `json:"comment"`
}

// ApproveRun POST /api/v1/workflows/runs/:id/approve
func (h *Handler) ApproveRun(ctx context.Context, c *app.RequestContext) {
	runID, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var req ApproveReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	run, err := h.svcs.Workflow.ApproveDecision(ctx, userID(c), runID, req.InterruptID, req.ArgsHash, req.Approved, req.Comment)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, run)
}

// ListRuns GET /api/v1/workflows/runs?workflow_id=
func (h *Handler) ListRuns(ctx context.Context, c *app.RequestContext) {
	wfID, _ := parseID(string(c.Query("workflow_id")))
	list, total, err := h.svcs.Workflow.ListRuns(ctx, userID(c), wfID, parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// GetRun GET /api/v1/workflows/runs/:id —— 执行详情（含步骤轨迹）。
func (h *Handler) GetRun(ctx context.Context, c *app.RequestContext) {
	runID, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	run, err := h.svcs.Workflow.GetWorkflowRun(ctx, userID(c), runID)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, run)
}
