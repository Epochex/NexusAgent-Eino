// 集成接口：MCP 服务端管理 / A2A 远程 Agent 注册与委派。
package api

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// ---- MCP（admin 管理）----

// ListMCPServers GET /api/v1/admin/mcp/servers
func (h *Handler) ListMCPServers(ctx context.Context, c *app.RequestContext) {
	list, err := h.svcs.MCP.List(ctx)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, list)
}

// CreateMCPServer POST /api/v1/admin/mcp/servers
func (h *Handler) CreateMCPServer(ctx context.Context, c *app.RequestContext) {
	srv := &model.MCPServer{}
	if err := c.BindAndValidate(srv); err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.MCP.Create(ctx, srv); err != nil {
		Fail(c, err)
		return
	}
	OK(c, srv)
}

// UpdateMCPServer PUT /api/v1/admin/mcp/servers/:id
func (h *Handler) UpdateMCPServer(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	srv := &model.MCPServer{}
	if err := c.BindAndValidate(srv); err != nil {
		Fail(c, err)
		return
	}
	srv.ID = id
	if err := h.svcs.MCP.Update(ctx, srv); err != nil {
		Fail(c, err)
		return
	}
	OK(c, srv)
}

// DeleteMCPServer DELETE /api/v1/admin/mcp/servers/:id
func (h *Handler) DeleteMCPServer(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.MCP.Delete(ctx, id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// SyncMCPServer POST /api/v1/admin/mcp/servers/:id/sync —— 工具同步。
func (h *Handler) SyncMCPServer(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	tools, err := h.svcs.MCP.SyncTools(ctx, id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, tools)
}

// CheckMCPServer POST /api/v1/admin/mcp/servers/:id/health —— 健康检查。
func (h *Handler) CheckMCPServer(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.MCP.HealthCheck(ctx, id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"status": "connected"})
}

// CallMCPToolReq MCP 工具直调请求（调试）。
type CallMCPToolReq struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// CallMCPTool POST /api/v1/admin/mcp/servers/:id/call
func (h *Handler) CallMCPTool(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var req CallMCPToolReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	out, err := h.svcs.MCP.CallTool(ctx, id, req.Name, req.Args)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"output": out})
}

// ---- A2A（admin 管理）----

func (h *Handler) MigrateMCPCredentials(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err = h.svcs.MCP.MigrateCredentials(ctx, id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

func (h *Handler) ListMCPTools(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	rows, total, err := h.svcs.MCP.Tools(ctx, id, parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": rows, "total": total})
}

func (h *Handler) ReviewMCPTool(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	toolID, err := strconv.ParseInt(c.Param("toolID"), 10, 64)
	if err != nil || toolID < 1 {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	var input service.MCPToolPolicyInput
	if err = c.BindAndValidate(&input); err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	row, err := h.svcs.MCP.UpdateToolPolicy(ctx, id, toolID, &input)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, row)
}

// ListA2AAgents GET /api/v1/admin/a2a/agents
func (h *Handler) ListA2AAgents(ctx context.Context, c *app.RequestContext) {
	list, err := h.svcs.A2A.List(ctx)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, list)
}

// RegisterA2AAgent POST /api/v1/admin/a2a/agents
func (h *Handler) RegisterA2AAgent(ctx context.Context, c *app.RequestContext) {
	var req service.A2ARegisterInput
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	ag, err := h.svcs.A2A.Register(ctx, &req)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, ag)
}

// DeleteA2AAgent DELETE /api/v1/admin/a2a/agents/:id
func (h *Handler) DeleteA2AAgent(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.A2A.Delete(ctx, id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// CheckA2AAgent POST /api/v1/admin/a2a/agents/:id/health
func (h *Handler) CheckA2AAgent(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.A2A.HealthCheck(ctx, id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"status": "connected"})
}

type SendA2ATaskReq = service.A2ADelegateInput

// SendA2ATask POST /api/v1/admin/a2a/agents/:id/tasks —— 委派任务给远程 Agent。
func (h *Handler) SendA2ATask(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var req SendA2ATaskReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	res, err := h.svcs.A2A.Delegate(ctx, id, &req)
	if err != nil {
		Fail(c, err)
		return
	}
	if res.State == "dispatching" || res.State == "submitted" || res.State == "working" || res.State == "unknown" {
		c.JSON(202, envelope{Code: 0, Message: "accepted", Data: res})
		return
	}
	OK(c, res)
}

func (h *Handler) UpdateA2AAgent(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var input service.A2AUpdateInput
	if err = c.BindAndValidate(&input); err != nil {
		Fail(c, errcode.ErrInvalidParam)
		return
	}
	result, err := h.svcs.A2A.Update(ctx, id, &input)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
func (h *Handler) SyncA2AAgent(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	result, err := h.svcs.A2A.Sync(ctx, id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
func (h *Handler) GetA2ADelegation(ctx context.Context, c *app.RequestContext) {
	result, err := h.svcs.A2A.Delegation(ctx, c.Param("taskID"), c.Query("refresh") == "true")
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
func (h *Handler) CancelA2ADelegation(ctx context.Context, c *app.RequestContext) {
	result, err := h.svcs.A2A.CancelDelegation(ctx, c.Param("taskID"))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
func (h *Handler) ListA2ADelegations(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	rows, total, err := h.svcs.A2A.Delegations(ctx, id, parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": rows, "total": total})
}
