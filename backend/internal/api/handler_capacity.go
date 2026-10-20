package api

import (
	"context"

	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/cloudwego/hertz/pkg/app"
)

func (h *Handler) CapacitySnapshot(ctx context.Context, c *app.RequestContext) {
	result, err := h.svcs.CapacitySnapshot(ctx)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
func (h *Handler) UpdateCapacity(ctx context.Context, c *app.RequestContext) {
	var p capacity.Policy
	if err := c.BindAndValidate(&p); err != nil {
		Fail(c, errcode.ErrInvalidParam.WithCause(err))
		return
	}
	result, err := h.svcs.UpdateCapacity(ctx, c.Param("scope"), p)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}
