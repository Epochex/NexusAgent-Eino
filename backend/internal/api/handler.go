// Handler 依赖持有与通用辅助。
package api

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/agent"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// Handler HTTP 处理器集合（持有全部业务服务）。
type Handler struct {
	svcs *service.Services
	chat interface {
		Stream(context.Context, int64, *service.ChatInput) (<-chan agent.Event, error)
	}
}

// New 构造。
func New(svcs *service.Services) *Handler { return &Handler{svcs: svcs, chat: svcs.Chat} }

// A streamed response outlives the handler call, so its body owns this cancel
// function. Process shutdown interrupts both Chat and idle workflow subscribers.
func (h *Handler) streamContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	if h.svcs == nil || h.svcs.Root == nil {
		return ctx, cancel
	}
	stop := context.AfterFunc(h.svcs.Root, cancel)
	if h.svcs.Root.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

// pathID 解析路径参数 :id。
func pathID(c *app.RequestContext) (int64, error) {
	return parseID(c.Param("id"))
}

// parseID 字符串 ID 解析。
func parseID(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// userID 从中间件注入的上下文中取当前用户 ID。
func userID(c *app.RequestContext) int64 {
	if v, ok := c.Get(claimsKey); ok {
		if claims, ok := v.(*userClaims); ok {
			return claims.UserID
		}
	}
	return 0
}

// isAdmin 当前用户是否管理员。
func isAdmin(c *app.RequestContext) bool {
	if v, ok := c.Get(claimsKey); ok {
		if claims, ok := v.(*userClaims); ok {
			return claims.Role == "admin"
		}
	}
	return false
}
