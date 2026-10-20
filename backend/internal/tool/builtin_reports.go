package tool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"gorm.io/gorm"
	"time"
)

func RegisterDBQuery(reg Registry, db *gorm.DB) {
	reg.Register("db_query", func(ctx context.Context, args map[string]any, _ string) (string, error) {
		return runDBQuery(ctx, db, args)
	})
}

// The model selects a report, never SQL. Database privileges are not a parser
// for SELECT expressions or stored functions with external side effects.
func runDBQuery(ctx context.Context, db *gorm.DB, args map[string]any) (string, error) {
	owner, ok := security.UserIDFromContext(ctx)
	if !ok || owner < 1 {
		return "", fmt.Errorf("authenticated reporting owner required")
	}
	for key := range args {
		if key != "report" && key != "limit" {
			return "", fmt.Errorf("only report and limit are accepted; arbitrary SQL is disabled")
		}
	}
	report, _ := args["report"].(string)
	var statement string
	switch report {
	case "recent_runs":
		statement = "SELECT id,workflow_id,status,created_at,started_at,finished_at FROM workflow_runs WHERE owner_id=? ORDER BY id DESC LIMIT ?"
	case "pending_approvals":
		statement = "SELECT a.run_id,a.interrupt_id,a.expires_at FROM execution_approvals a JOIN workflow_runs r ON r.id=a.run_id WHERE r.owner_id=? AND r.status='waiting_approval' AND a.decision='pending' AND a.expires_at>NOW() ORDER BY a.id DESC LIMIT ?"
	case "usage_summary":
		statement = "SELECT scene,COUNT(*) AS requests,COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,COALESCE(SUM(completion_tokens),0) AS completion_tokens FROM usage_logs WHERE user_id=? GROUP BY scene ORDER BY scene LIMIT ?"
	default:
		return "", fmt.Errorf("unknown report; choose recent_runs, pending_approvals or usage_summary")
	}
	if db == nil {
		return "", fmt.Errorf("reporting database unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var result []map[string]any
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL statement_timeout='1500ms'").Error; err != nil {
			return err
		}
		if err := tx.Exec("SET LOCAL lock_timeout='250ms'").Error; err != nil {
			return err
		}
		return tx.Raw(statement, owner, boundedInt(args["limit"], 20, 100)).Scan(&result).Error
	}, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", fmt.Errorf("report query failed: %w", err)
	}
	if result == nil {
		result = []map[string]any{}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	if len(raw) > 65536 {
		return "", fmt.Errorf("report exceeds output limit")
	}
	return string(raw), nil
}
