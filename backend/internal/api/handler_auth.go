// 认证接口：注册 / 登录 / 刷新 / 登出 / 我的信息。
package api

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// RegisterReq 注册请求。
type RegisterReq struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

// Register POST /api/v1/auth/register
func (h *Handler) Register(ctx context.Context, c *app.RequestContext) {
	var req RegisterReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	u, err := h.svcs.Auth.Register(ctx, &service.RegisterInput{
		Username: req.Username, Email: req.Email, Password: req.Password,
	})
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"id": u.ID, "username": u.Username, "role": u.Role})
}

// LoginReq 登录请求。
type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Login POST /api/v1/auth/login
func (h *Handler) Login(ctx context.Context, c *app.RequestContext) {
	var req LoginReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	if req.Username == "" || req.Password == "" {
		Fail(c, errcode.ErrMissingParam.WithMsg("用户名和密码不能为空"))
		return
	}
	pair, err := h.svcs.Auth.Login(ctx, req.Username, req.Password)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, pair)
}

// RefreshReq 刷新请求。
type RefreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

// Refresh POST /api/v1/auth/refresh
func (h *Handler) Refresh(ctx context.Context, c *app.RequestContext) {
	var req RefreshReq
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	pair, err := h.svcs.Auth.Refresh(ctx, req.RefreshToken)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, pair)
}

// Logout POST /api/v1/auth/logout
func (h *Handler) Logout(ctx context.Context, c *app.RequestContext) {
	if err := h.svcs.Auth.Logout(ctx, userID(c)); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// Me GET /api/v1/auth/me
func (h *Handler) Me(ctx context.Context, c *app.RequestContext) {
	u, err := h.svcs.Repos.User.GetByID(ctx, userID(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"id": u.ID, "username": u.Username, "email": u.Email, "role": u.Role})
}
