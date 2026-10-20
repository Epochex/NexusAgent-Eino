// 内置工具：代码执行（接 Docker 沙箱）与知识库检索（接 RAG 引擎）。
package tool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

// RegisterCodeExec 注册 code_exec（沙箱执行 + 审计落库）。
// sandbox 为 nil 时返回明确错误（P0 可选开关）。
func RegisterCodeExec(r Registry, sandbox *Sandbox, db *gorm.DB) {
	r.Register("code_exec", func(ctx context.Context, args map[string]any, raw string) (string, error) {
		language, _ := args["language"].(string)
		code, _ := args["code"].(string)
		if language == "" || code == "" {
			return "", fmt.Errorf("language and code are required")
		}
		owner, ok := security.UserIDFromContext(ctx)
		if !ok {
			return "", fmt.Errorf("authenticated identity required for code execution")
		}
		if sandbox == nil {
			return "", fmt.Errorf("sandbox disabled (enable in config)")
		}
		if db == nil {
			return "", fmt.Errorf("code execution requires an audit database")
		}

		result, err := sandbox.Run(ctx, language, code)
		if err != nil {
			return "", err
		}

		// 审计记录（代码只存 hash）
		if db != nil {
			sum := sha256.Sum256([]byte(code))
			exec := &model.SandboxExecution{
				UserID:     owner,
				ToolCallID: "exec_" + rand.Text(),
				Language:   language,
				CodeHash:   fmt.Sprintf("%x", sum),
				ExitCode:   result.ExitCode,
				DurationMs: int(result.Duration.Milliseconds()),
				StdoutHead: truncate(result.Stdout, 8192),
				StderrHead: truncate(result.Stderr, 8192),
			}
			auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			err = db.WithContext(auditCtx).Create(exec).Error
			cancel()
			if err != nil {
				return "", fmt.Errorf("code execution audit commit failed; inspect effects before retry")
			}
		}

		out, _ := json.Marshal(map[string]any{
			"exit_code": result.ExitCode,
			"stdout":    result.Stdout,
			"stderr":    result.Stderr,
			"duration":  result.Duration.Milliseconds(),
		})
		return string(out), nil
	})
}

// KBSearchFunc 知识库检索函数（由 service 层注入，避免 tool→rag 直接依赖）。
type KBSearchFunc func(ctx context.Context, kbID int64, query string, topK int) (string, error)

// RegisterKBSearch 注册 kb_search。
func RegisterKBSearch(r Registry, fn KBSearchFunc) {
	r.Register("kb_search", func(ctx context.Context, args map[string]any, _ string) (string, error) {
		var kbID int64
		switch v := args["kb_id"].(type) {
		case float64:
			kbID = int64(v)
		case json.Number:
			kbID, _ = v.Int64()
		default:
			return "", fmt.Errorf("kb_id must be a number")
		}
		query, _ := args["query"].(string)
		topK := boundedInt(args["top_k"], 5, 100)
		if query == "" {
			return "", fmt.Errorf("query is required")
		}
		return fn(ctx, kbID, query, topK)
	})
}
