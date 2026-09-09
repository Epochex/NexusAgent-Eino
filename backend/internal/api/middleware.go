// HTTP 中间件：CORS / JWT 认证 / 管理员门禁 / 恢复与访问日志。
package api

import (
	"context"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

// claimsKey 请求上下文中认证信息的键。
const claimsKey = "auth_claims"

// userClaims 认证中间件向 handler 传递的身份信息。
type userClaims = security.Claims

// CORS 跨域中间件（教学版全放开；生产按需收紧 Origin）。
func CORS() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Access-Control-Max-Age", "86400")
		if string(c.Method()) == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next(ctx)
	}
}

// Recovery panic 恢复：单请求异常不拖垮进程，返回 500。
func Recovery() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			if r := recover(); r != nil {
				logError("panic recovered", "path", string(c.Path()), "err", toString(r))
				c.AbortWithStatus(500)
			}
		}()
		c.Next(ctx)
	}
}

// AccessLog 访问日志（method path status 耗时）。
func AccessLog() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		logInfo("access",
			"method", string(c.Method()),
			"path", string(c.Path()),
			"status", itoa(c.Response.StatusCode()),
			"ms", itoa(int(time.Since(start).Milliseconds())),
		)
	}
}

// Auth JWT 认证：校验 Bearer Token 并把身份注入请求上下文。
func Auth(jwt *security.JWTManager) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		// SSE 场景 EventSource 不便带 header，允许 ?token= 降级（教学便利）
		token := ""
		if auth := string(c.GetHeader("Authorization")); strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		} else {
			token = string(c.Query("token"))
		}
		if token == "" {
			abort(c, 401, errUnauthorized)
			return
		}
		claims, err := jwt.Parse(token, false)
		if err != nil {
			abort(c, 401, errTokenInvalid)
			return
		}
		c.Set(claimsKey, claims)
		c.Next(ctx)
	}
}

// AdminOnly 管理员门禁（挂在 Auth 之后）。
func AdminOnly() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if !isAdmin(c) {
			abort(c, 403, errForbidden)
			return
		}
		c.Next(ctx)
	}
}

// abort 简单错误终止（避免中间件层反向依赖 response.go 的 envelope）。
func abort(c *app.RequestContext, status int, body envelope) {
	c.AbortWithStatusJSON(status, body)
}

// 中间件层错误包络常量。
var (
	errUnauthorized   = envelope{Code: 20101, Message: "未登录"}
	errTokenInvalid   = envelope{Code: 20103, Message: "无效凭证"}
	errForbidden      = envelope{Code: 20201, Message: "无权限"}
)
