package execution

import (
	"context"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Expiry also applies while nobody owns an execution slot (human approval or
// manual reconciliation), rather than relying on Claim to see runnable jobs.
func (s *Store) ExpireRuns(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 256 {
		return 0, ErrInvalidInput
	}
	expired := 0
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		var runs []Run
		if err := tx.Where("definition IS NOT NULL AND status IN ? AND deadline_at<=clock_timestamp()", []string{Queued, Running, RetryWait, Waiting, NeedsAttention}).Order("deadline_at,id").Limit(limit).Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Find(&runs).Error; err != nil {
			return err
		}
		for i := range runs {
			if err := expireRun(tx, &runs[i]); err != nil {
				return err
			}
			expired++
		}
		return nil
	})
	return expired, err
}

// expireRun requires the caller to hold the run row lock in this transaction.
// Claim and maintenance must retire external intent before publishing failure.
func expireRun(tx *gorm.DB, run *Run) error {
	var ops []Operation
	if err := tx.Where("run_id=? AND status='running'", run.ID).Find(&ops).Error; err != nil {
		return err
	}
	for _, op := range ops {
		status := "abandoned"
		if op.ReplayPolicy == "manual" {
			status = "unknown"
		}
		if err := tx.Model(&op).Updates(map[string]any{"status": status, "error": "run deadline exceeded during operation", "finished_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE execution_attempts SET status=?,error='run deadline exceeded during operation',finished_at=NOW() WHERE operation_id=? AND attempt=? AND status='running'`, status, op.ID, op.Attempt).Error; err != nil {
			return err
		}
		if status == "unknown" {
			if err := appendEvent(tx, run.ID, "operation_unknown", map[string]any{"operation_id": op.ID, "reason": "deadline"}); err != nil {
				return err
			}
		}
	}
	if err := tx.Model(run).Updates(map[string]any{"status": Failed, "error": "run deadline exceeded", "generation": gorm.Expr("generation+1"), "finished_at": gorm.Expr("NOW()"), "lease_owner": "", "lease_until": nil}).Error; err != nil {
		return err
	}
	return appendEvent(tx, run.ID, Failed, map[string]any{"error": "run deadline exceeded"})
}
