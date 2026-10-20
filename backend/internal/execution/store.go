package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"io"
	"time"
)

type Store struct{ db *gorm.DB }

func NewStore(db *gorm.DB) *Store { return &Store{db: db} }
func (s *Store) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(fn)
}
func object(raw []byte, max int) (datatypes.JSON, error) {
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	var m map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if len(raw) > max || decoder.Decode(&m) != nil || m == nil {
		return nil, ErrInvalidInput
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, ErrInvalidInput
	}
	normalized, err := json.Marshal(m)
	return datatypes.JSON(normalized), err
}
func hash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

func (s *Store) Enqueue(ctx context.Context, ownerID, workflowID int64, definition, input json.RawMessage, deadline time.Time) (*Run, error) {
	return s.EnqueueRequest(ctx, ownerID, workflowID, definition, input, "", deadline)
}

func (s *Store) EnqueueRequest(ctx context.Context, ownerID, workflowID int64, definition, input json.RawMessage, requestKey string, deadline time.Time) (*Run, error) {
	def, err := object(definition, 1<<20)
	if err != nil {
		return nil, err
	}
	in, err := object(input, 1<<20)
	if err != nil {
		return nil, err
	}
	if ownerID <= 0 || workflowID <= 0 || !deadline.After(time.Now()) || deadline.After(time.Now().Add(7*24*time.Hour)) {
		return nil, ErrInvalidInput
	}
	if len(requestKey) > 64 {
		return nil, ErrInvalidInput
	}
	run := &Run{OwnerID: ownerID, WorkflowID: workflowID, Definition: def, DefinitionHash: hash(def), EngineVersion: EngineVersion, GraphSchema: GraphSchema, Status: Queued, Input: in, DeadlineAt: deadline, AvailableAt: time.Now(), PendingIDs: datatypes.JSON(`[]`)}
	if requestKey != "" {
		run.RequestKey = &requestKey
	}
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		if requestKey != "" {
			existing, err := findRequest(tx, ownerID, workflowID, requestKey, in)
			if err != nil {
				return err
			}
			if existing != nil {
				*run = *existing
				return nil
			}
		}
		var found struct {
			ID      int64
			Version int
		}
		res := tx.Table("workflows").Select("id,version").Where("id=? AND creator_id=? AND status=1 AND archived_at IS NULL", workflowID, ownerID).Clauses(clause.Locking{Strength: "SHARE"}).Take(&found)
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if res.Error != nil {
			return res.Error
		}
		if err := publishSnapshot(tx, run, found.Version); err != nil {
			return err
		}
		create := tx
		if requestKey != "" {
			create = tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "owner_id"}, {Name: "request_key"}}, DoNothing: true})
		}
		result := create.Create(run)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			existing, err := findRequest(tx, ownerID, workflowID, requestKey, in)
			if err != nil {
				return err
			}
			if existing == nil {
				return ErrConflict
			}
			*run = *existing
			return nil
		}
		return appendEvent(tx, run.ID, "queued", map[string]any{"definition_hash": run.DefinitionHash})
	})
	return run, err
}

func (s *Store) Claim(ctx context.Context, worker string, lease time.Duration) (*Run, error) {
	return s.ClaimSupported(ctx, worker, lease, nil)
}
func (s *Store) ClaimSupported(ctx context.Context, worker string, lease time.Duration, capabilities []string) (*Run, error) {
	if worker == "" || len(worker) > 128 || lease < time.Second || lease > 10*time.Minute {
		return nil, ErrInvalidInput
	}
	var claimed *Run
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		var run Run
		query := tx.Where("definition IS NOT NULL AND available_at<=NOW() AND (status IN ? OR (status=? AND lease_until<NOW()))", []string{Queued, RetryWait}, Running)
		if capabilities != nil {
			raw, err := json.Marshal(capabilities)
			if err != nil {
				return ErrInvalidInput
			}
			query = query.Where("COALESCE(definition->'required_capabilities','[]'::jsonb) <@ ?::jsonb", string(raw))
		}
		err := query.Order("available_at,id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var live bool
		if err := tx.Raw("SELECT deadline_at>clock_timestamp() FROM workflow_runs WHERE id=?", run.ID).Scan(&live).Error; err != nil {
			return err
		}
		if !live {
			if err := tx.Model(&run).Updates(map[string]any{"status": Failed, "error": "run deadline exceeded", "generation": gorm.Expr("generation+1"), "finished_at": gorm.Expr("NOW()"), "lease_owner": "", "lease_until": nil}).Error; err != nil {
				return err
			}
			return appendEvent(tx, run.ID, Failed, map[string]any{"error": "run deadline exceeded"})
		}
		res := tx.Raw(`UPDATE workflow_runs SET status=?,generation=generation+1,lease_owner=?,lease_until=clock_timestamp()+(? * INTERVAL '1 millisecond'),started_at=COALESCE(started_at,NOW()) WHERE id=? RETURNING *`, Running, worker, lease.Milliseconds(), run.ID).Scan(&run)
		if res.Error != nil {
			return res.Error
		}
		if err := appendEvent(tx, run.ID, "running", map[string]any{"generation": run.Generation}); err != nil {
			return err
		}
		claimed = &run
		return nil
	})
	return claimed, err
}

func (s *Store) Renew(ctx context.Context, run *Run, lease time.Duration) error {
	if lease < time.Second || lease > 10*time.Minute {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	res := fenced(s.db.WithContext(ctx), run).Updates(map[string]any{"lease_until": gorm.Expr("clock_timestamp()+(? * INTERVAL '1 millisecond')", lease.Milliseconds())})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrLeaseLost
	}
	return nil
}
func fenced(tx *gorm.DB, run *Run) *gorm.DB {
	return tx.Model(&Run{}).Where("id=? AND generation=? AND lease_owner=? AND status=? AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()", run.ID, run.Generation, run.LeaseOwner, Running)
}
func lockLease(tx *gorm.DB, run *Run) error {
	var row Run
	err := fenced(tx, run).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrLeaseLost
	}
	if err == nil && (row.DefinitionHash != run.DefinitionHash || row.EngineVersion != run.EngineVersion || row.GraphSchema != run.GraphSchema) {
		return ErrVersionMismatch
	}
	return err
}

func (s *Store) Finish(ctx context.Context, run *Run, status string, output json.RawMessage, message string) error {
	if !terminal(status) && status != NeedsAttention {
		return ErrInvalidInput
	}
	out, err := object(output, 4<<20)
	if err != nil {
		return err
	}
	message = boundedError(message)
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, run); err != nil {
			return err
		}
		var finished any = gorm.Expr("NOW()")
		pendingIDs := datatypes.JSON(`[]`)
		if status == NeedsAttention {
			finished = nil
			// A later fault may occur after approval but before the next Eino
			// checkpoint. Keep the approved resume target for reconciliation.
			pendingIDs = run.PendingIDs
		}
		if err := tx.Model(&Run{}).Where("id=?", run.ID).Updates(map[string]any{"status": status, "output": out, "error": message, "finished_at": finished, "lease_owner": "", "lease_until": nil, "pending_ids": pendingIDs}).Error; err != nil {
			return err
		}
		return appendEvent(tx, run.ID, status, map[string]any{"error": message})
	})
}
func (s *Store) Get(ctx context.Context, ownerID, id int64) (*Run, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var run Run
	err := s.db.WithContext(ctx).Where("id=? AND owner_id=? AND definition IS NOT NULL", id, ownerID).Take(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	if err = json.Unmarshal(run.PendingIDs, &ids); err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		err = s.db.WithContext(ctx).Where("run_id=? AND interrupt_id IN ?", id, ids).Order("id").Find(&run.Approvals).Error
	}
	return &run, err
}

func (s *Store) Suspend(ctx context.Context, run *Run, pending []Approval) error {
	if len(pending) == 0 || len(pending) > 32 {
		return ErrInvalidInput
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, run); err != nil {
			return err
		}
		ids := make([]string, 0, len(pending))
		seen := map[string]bool{}
		for _, a := range pending {
			if a.InterruptID == "" || len(a.InterruptID) > 256 || len(a.ArgsHash) > 64 || a.ArgsHash == "" || len(a.Path) > 1024 || len(a.Prompt) > 65536 || seen[a.InterruptID] {
				return ErrInvalidInput
			}
			seen[a.InterruptID] = true
			a.RunID = run.ID
			a.Decision = "pending"
			a.ExpiresAt = run.DeadlineAt
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "run_id"}, {Name: "interrupt_id"}}, DoNothing: true}).Create(&a).Error; err != nil {
				return err
			}
			var existing Approval
			if err := tx.Where("run_id=? AND interrupt_id=?", run.ID, a.InterruptID).Take(&existing).Error; err != nil {
				return err
			}
			if existing.ArgsHash != a.ArgsHash || existing.Path != a.Path {
				return ErrVersionMismatch
			}
			ids = append(ids, a.InterruptID)
		}
		raw, _ := json.Marshal(ids)
		if err := tx.Model(&Run{}).Where("id=?", run.ID).Updates(map[string]any{"status": Waiting, "pending_ids": datatypes.JSON(raw), "lease_owner": "", "lease_until": nil}).Error; err != nil {
			return err
		}
		return appendEvent(tx, run.ID, Waiting, map[string]any{"interrupt_ids": ids})
	})
}

func (s *Store) Decide(ctx context.Context, ownerID, runID int64, interruptID, argsHash string, approved bool) error {
	return s.DecideWithComment(ctx, ownerID, runID, interruptID, argsHash, approved, "")
}
func (s *Store) DecideWithComment(ctx context.Context, ownerID, runID int64, interruptID, argsHash string, approved bool, comment string) error {
	if len(comment) > 2048 {
		return ErrInvalidInput
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		var run Run
		if err := tx.Where("id=? AND owner_id=? AND definition IS NOT NULL", runID, ownerID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			return err
		}
		var ids []string
		if err := json.Unmarshal(run.PendingIDs, &ids); err != nil {
			return err
		}
		if interruptID == "" {
			if len(ids) != 1 {
				return ErrConflict
			}
			interruptID = ids[0]
		}
		var a Approval
		err := tx.Where("run_id=? AND interrupt_id=?", runID, interruptID).Take(&a).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if argsHash != "" && argsHash != a.ArgsHash {
			return ErrConflict
		}
		decision := "rejected"
		if approved {
			decision = "approved"
		}
		if a.Decision != "pending" {
			if a.Decision == decision {
				return nil
			}
			return ErrConflict
		}
		active := false
		for _, id := range ids {
			active = active || id == interruptID
		}
		if !active || run.Status != Waiting && run.Status != Queued {
			return ErrConflict
		}
		result := tx.Model(&a).Where("decision='pending' AND expires_at>clock_timestamp()").Updates(map[string]any{"decision": decision, "decided_by": ownerID, "decided_at": gorm.Expr("NOW()"), "comment": comment})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrConflict
		}
		status := Queued
		updates := map[string]any{"status": Queued, "available_at": gorm.Expr("NOW()")}
		if !approved {
			status = Canceled
			updates["status"] = status
			updates["generation"] = gorm.Expr("generation+1")
			updates["finished_at"] = gorm.Expr("NOW()")
			updates["lease_owner"] = ""
			updates["lease_until"] = nil
		}
		if err := tx.Model(&Run{}).Where("id=?", runID).Updates(updates).Error; err != nil {
			return err
		}
		if err := appendEvent(tx, runID, "approval_decided", map[string]any{"interrupt_id": interruptID, "decision": decision, "args_hash": a.ArgsHash, "comment": comment}); err != nil {
			return err
		}
		return appendEvent(tx, runID, status, map[string]any{"reason": "approval"})
	})
}
func (s *Store) Cancel(ctx context.Context, ownerID, id int64) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		var run Run
		err := tx.Where("id=? AND owner_id=? AND definition IS NOT NULL", id, ownerID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if terminal(run.Status) {
			return nil
		}
		// Cancel cannot undo a remote call already sent. Preserve uncertainty
		// before fencing the executor so operators can inspect what happened.
		var inflight []Operation
		if err = tx.Where("run_id=? AND status='running'", id).Find(&inflight).Error; err != nil {
			return err
		}
		for _, op := range inflight {
			status := "abandoned"
			if op.ReplayPolicy == "manual" {
				status = "unknown"
			}
			if err = tx.Model(&Operation{}).Where("id=?", op.ID).Updates(map[string]any{"status": status, "error": "run canceled while operation in flight", "finished_at": gorm.Expr("NOW()")}).Error; err != nil {
				return err
			}
			if err = tx.Exec(`UPDATE execution_attempts SET status=?,error='run canceled while operation in flight',finished_at=NOW() WHERE operation_id=? AND attempt=? AND status='running'`, status, op.ID, op.Attempt).Error; err != nil {
				return err
			}
			if status == "unknown" {
				if err = appendEvent(tx, id, "operation_unknown", map[string]any{"operation_id": op.ID, "path": op.Path, "reason": "canceled_in_flight"}); err != nil {
					return err
				}
			}
		}
		if err = tx.Model(&run).Updates(map[string]any{"status": Canceled, "generation": gorm.Expr("generation+1"), "finished_at": gorm.Expr("NOW()"), "lease_owner": "", "lease_until": nil}).Error; err != nil {
			return err
		}
		return appendEvent(tx, id, Canceled, map[string]any{"reason": "user"})
	})
}

func appendEvent(tx *gorm.DB, id int64, kind string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return ErrInvalidInput
	}
	var seq int64
	if err = tx.Raw(`UPDATE workflow_runs SET next_seq=next_seq+1,state_version=state_version+1 WHERE id=? RETURNING next_seq`, id).Scan(&seq).Error; err != nil {
		return err
	}
	event := Event{RunID: id, Seq: seq, Type: kind, SchemaVersion: 1, Data: raw}
	if err = tx.Create(&event).Error; err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return tx.Exec(`INSERT INTO execution_outbox(run_id,seq,topic,payload) VALUES (?,?,?,?::jsonb)`, id, seq, "run."+kind, string(payload)).Error
}

func (s *Store) Events(ctx context.Context, ownerID, id, after int64, limit int) ([]Event, error) {
	run, err := s.Get(ctx, ownerID, id)
	if err != nil {
		return nil, err
	}
	if after < 0 || after > run.NextSeq {
		return nil, ErrInvalidInput
	}
	if limit <= 0 || limit > 256 {
		limit = 128
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var min int64
	if err = s.db.WithContext(ctx).Raw(`SELECT COALESCE(MIN(seq),0) FROM execution_events WHERE run_id=?`, id).Scan(&min).Error; err != nil {
		return nil, err
	}
	if run.NextSeq > after && (min == 0 || after < min-1) {
		return nil, ErrCursorExpired
	}
	var events []Event
	err = s.db.WithContext(ctx).Where("run_id=? AND seq>?", id, after).Order("seq").Limit(limit).Find(&events).Error
	return events, err
}

func (s *Store) String() string { return fmt.Sprintf("execution.Store(%p)", s.db) }
