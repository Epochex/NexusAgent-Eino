package execution

import (
	"context"
	"time"
)

// History is display-only; callers must use active pending IDs from Get to
// approve. Old re-interrupted IDs must never become selectable by accident.
func (s *Store) ApprovalHistory(ctx context.Context, ownerID, runID int64) ([]Approval, bool, error) {
	if _, err := s.Get(ctx, ownerID, runID); err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var history []Approval
	if err := s.db.WithContext(ctx).Where("run_id=? AND decision<>'pending'", runID).Order("id DESC").Limit(257).Find(&history).Error; err != nil {
		return nil, false, err
	}
	truncated := len(history) > 256
	if truncated {
		history = history[:256]
	}
	return history, truncated, nil
}
