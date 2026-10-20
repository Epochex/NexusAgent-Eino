package execution

import (
	"context"
	"gorm.io/gorm"
	"time"
)

// Prune only completed old runs and confirmed notifications. Active approvals,
// blocked runs and unpublished outbox rows remain replayable regardless of age.
func (s *Store) PruneEvents(ctx context.Context, retention time.Duration, limit int) error {
	if retention < 24*time.Hour || retention > 365*24*time.Hour || limit < 1 || limit > 10000 {
		return ErrInvalidInput
	}
	before := time.Now().Add(-retention)
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec(`WITH batch AS (
   SELECT e.run_id,e.seq FROM execution_events e JOIN workflow_runs r ON r.id=e.run_id
   JOIN execution_outbox o ON o.run_id=e.run_id AND o.seq=e.seq
   WHERE r.status IN ('succeeded','failed','canceled') AND r.finished_at<?
   AND e.created_at<? AND o.published_at IS NOT NULL ORDER BY e.created_at LIMIT ?
  ) DELETE FROM execution_events e USING batch b WHERE e.run_id=b.run_id AND e.seq=b.seq`, before, before, limit).Error; err != nil {
			return err
		}
		return tx.Exec(`WITH batch AS (
   SELECT o.id FROM execution_outbox o JOIN workflow_runs r ON r.id=o.run_id
   WHERE r.status IN ('succeeded','failed','canceled') AND r.finished_at<?
   AND o.created_at<? AND o.published_at IS NOT NULL
   AND NOT EXISTS(SELECT 1 FROM execution_events e WHERE e.run_id=o.run_id AND e.seq=o.seq)
   ORDER BY o.id LIMIT ?
  ) DELETE FROM execution_outbox o USING batch b WHERE o.id=b.id`, before, before, limit).Error
	})
}
