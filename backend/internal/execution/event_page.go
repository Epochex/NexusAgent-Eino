package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// EventPage's cursor, terminal status and events share one PostgreSQL statement
// snapshot. A terminal read must never race ahead of its last persisted event.
// It deliberately excludes definitions, checkpoints, approvals and operations.
type EventPage struct {
	Status  string
	NextSeq int64
	Events  []Event
}

func (s *Store) ReadEventPage(parent context.Context, ownerID, id, after int64, limit int) (*EventPage, error) {
	if after < 0 {
		return nil, ErrInvalidInput
	}
	if limit <= 0 || limit > 256 {
		limit = 128
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var row struct {
		Status            string
		NextSeq, FirstSeq int64
		EventsJSON        string
	}
	result := s.db.WithContext(ctx).Raw(`SELECT r.status,r.next_seq,
 CASE WHEN r.next_seq>? THEN (SELECT COALESCE(MIN(seq),0) FROM execution_events WHERE run_id=r.id) ELSE 0 END AS first_seq,
 (CASE WHEN r.next_seq>? THEN (
  SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY p.seq),'[]'::jsonb) FROM (
   SELECT run_id,seq,type,schema_version,data,created_at AS time FROM execution_events
   WHERE run_id=r.id AND seq>? ORDER BY seq LIMIT ?
  ) p
 ) ELSE '[]'::jsonb END)::text AS events_json
 FROM workflow_runs r WHERE r.id=? AND r.owner_id=? AND r.definition IS NOT NULL`, after, after, after, limit, id, ownerID).Scan(&row)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, ErrNotFound
	}
	if after > row.NextSeq {
		return nil, ErrInvalidInput
	}
	if row.NextSeq > after && (row.FirstSeq == 0 || after < row.FirstSeq-1) {
		return nil, ErrCursorExpired
	}
	page := &EventPage{Status: row.Status, NextSeq: row.NextSeq}
	if err := json.Unmarshal([]byte(row.EventsJSON), &page.Events); err != nil {
		return nil, fmt.Errorf("stored execution event page invalid: %w", err)
	}
	return page, nil
}
