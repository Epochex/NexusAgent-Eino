package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrUnknownOperation = errors.New("external operation outcome unknown; reconcile before resuming")
var ErrAttemptsExhausted = errors.New("operation retry attempts exhausted")

type Operation struct {
	ID           string         `json:"id"`
	RunID        int64          `json:"run_id"`
	Path         string         `json:"path"`
	Kind         string         `json:"kind"`
	ArgsHash     string         `json:"args_hash"`
	ReplayPolicy string         `json:"replay_policy"`
	Status       string         `json:"status"`
	Generation   int64          `json:"generation"`
	Output       datatypes.JSON `json:"output,omitempty"`
	Error        string         `json:"error,omitempty"`
	Attempt      int            `json:"attempt"`
	StartedAt    time.Time      `json:"started_at"`
	FinishedAt   *time.Time     `json:"finished_at,omitempty"`
}

func (Operation) TableName() string { return "execution_operations" }

func (s *Store) BeginOperation(ctx context.Context, run *Run, path, kind, argsHash, policy string) (*Operation, bool, error) {
	if path == "" || len(path) > 1024 || kind == "" || len(kind) > 32 || argsHash == "" || len(argsHash) > 64 {
		return nil, false, ErrInvalidInput
	}
	if policy != "readonly" && policy != "idempotent" && policy != "manual" {
		return nil, false, ErrInvalidInput
	}
	var op Operation
	var cached bool
	var outcome error
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, run); err != nil {
			return err
		}
		err := tx.Where("run_id=? AND path=?", run.ID, path).Take(&op).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			if op.ArgsHash != argsHash || op.Kind != kind || op.ReplayPolicy != policy {
				return ErrConflict
			}
			if op.Status == Succeeded {
				cached = true
				return nil
			}
			if op.Status == "unknown" {
				outcome = ErrUnknownOperation
				return nil
			}
			if op.Status == Running && op.Generation == run.Generation {
				return ErrConflict
			}
			if op.ReplayPolicy == "manual" {
				if err := tx.Model(&op).Updates(map[string]any{"status": "unknown", "error": "previous executor lost before outcome committed", "finished_at": gorm.Expr("NOW()")}).Error; err != nil {
					return err
				}
				if err := tx.Exec(`UPDATE execution_attempts SET status='unknown',error=?,finished_at=NOW() WHERE operation_id=? AND attempt=?`, op.Error, op.ID, op.Attempt).Error; err != nil {
					return err
				}
				outcome = ErrUnknownOperation
				return appendEvent(tx, run.ID, "operation_unknown", map[string]any{"operation_id": op.ID, "path": path})
			}
			if op.Attempt >= 3 {
				return ErrAttemptsExhausted
			}
			if err := tx.Exec(`UPDATE execution_attempts SET status='abandoned',error='executor lease lost',finished_at=NOW() WHERE operation_id=? AND attempt=? AND status='running'`, op.ID, op.Attempt).Error; err != nil {
				return err
			}
			op.Attempt++
			op.Generation = run.Generation
			op.Status = Running
			op.Error = ""
			op.FinishedAt = nil
			op.StartedAt = time.Now()
			if err := tx.Model(&Operation{}).Where("id=?", op.ID).Updates(map[string]any{"attempt": op.Attempt, "generation": op.Generation, "status": Running, "error": "", "finished_at": nil, "started_at": gorm.Expr("NOW()")}).Error; err != nil {
				return err
			}
		} else {
			op = Operation{ID: hash([]byte(fmt.Sprintf("%d:%s", run.ID, path))), RunID: run.ID, Path: path, Kind: kind, ArgsHash: argsHash, ReplayPolicy: policy, Status: Running, Generation: run.Generation, Attempt: 1, StartedAt: time.Now()}
			if err := tx.Create(&op).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec(`INSERT INTO execution_attempts(operation_id,generation,attempt,status) VALUES (?,?,?,'running')`, op.ID, op.Generation, op.Attempt).Error; err != nil {
			return err
		}
		return appendEvent(tx, run.ID, "step_started", map[string]any{"operation_id": op.ID, "path": op.Path, "attempt": op.Attempt})
	})
	if err != nil {
		return nil, false, err
	}
	return &op, cached, outcome
}

func (s *Store) CompleteOperation(ctx context.Context, run *Run, op *Operation, output any, callErr error) error {
	raw, err := json.Marshal(output)
	if err != nil || len(raw) > 4<<20 {
		return ErrInvalidInput
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, run); err != nil {
			return err
		}
		status := Succeeded
		message := ""
		if callErr != nil {
			status = Failed
			message = boundedError(callErr.Error())
			if op.ReplayPolicy == "manual" {
				status = "unknown"
			}
		}
		result := tx.Model(&Operation{}).Where("id=? AND run_id=? AND generation=? AND attempt=? AND status=?", op.ID, run.ID, run.Generation, op.Attempt, Running).Updates(map[string]any{"status": status, "output": datatypes.JSON(raw), "error": message, "finished_at": gorm.Expr("NOW()")})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrConflict
		}
		if err := tx.Exec(`UPDATE execution_attempts SET status=?,error=?,finished_at=NOW() WHERE operation_id=? AND attempt=? AND generation=?`, status, message, op.ID, op.Attempt, run.Generation).Error; err != nil {
			return err
		}
		return appendEvent(tx, run.ID, "step_"+status, map[string]any{"operation_id": op.ID, "path": op.Path, "attempt": op.Attempt, "error": message})
	})
}

func (s *Store) Retry(ctx context.Context, run *Run, delay time.Duration, message string) error {
	if delay < 0 || delay > time.Minute {
		return ErrInvalidInput
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, run); err != nil {
			return err
		}
		if err := tx.Model(&Run{}).Where("id=?", run.ID).Updates(map[string]any{"status": RetryWait, "error": boundedError(message), "available_at": gorm.Expr("LEAST(deadline_at,clock_timestamp()+(? * INTERVAL '1 millisecond'))", delay.Milliseconds()), "lease_owner": "", "lease_until": nil}).Error; err != nil {
			return err
		}
		return appendEvent(tx, run.ID, RetryWait, map[string]any{"delay_ms": delay.Milliseconds(), "error": boundedError(message)})
	})
}

// ResolveOperation records an explicit owner's reconciliation, never a blind
// automatic replay. Evidence remains in the reliable audit event.
func (s *Store) ResolveOperation(ctx context.Context, ownerID, runID int64, operationID, argsHash, output, evidence string) error {
	if !json.Valid([]byte(output)) || len(output) > 65536 || evidence == "" || len(evidence) > 2048 {
		return ErrInvalidInput
	}
	return s.transaction(ctx, func(tx *gorm.DB) error {
		var run Run
		err := tx.Where("id=? AND owner_id=? AND definition IS NOT NULL", runID, ownerID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var op Operation
		if err := tx.Where("id=? AND run_id=?", operationID, runID).Take(&op).Error; err != nil {
			return ErrConflict
		}
		if op.ArgsHash != argsHash || op.Status != "unknown" || run.Status != NeedsAttention {
			return ErrConflict
		}
		if err := tx.Model(&op).Updates(map[string]any{"status": Succeeded, "output": datatypes.JSON(output), "error": "", "finished_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if err := tx.Model(&Run{}).Where("id=?", runID).Updates(map[string]any{"status": Queued, "error": "", "finished_at": nil, "available_at": gorm.Expr("NOW()"), "generation": gorm.Expr("generation+1")}).Error; err != nil {
			return err
		}
		return appendEvent(tx, runID, "operation_reconciled", map[string]any{"operation_id": op.ID, "args_hash": argsHash, "decided_by": ownerID, "evidence": evidence})
	})
}

func (s *Store) Operations(ctx context.Context, ownerID, runID int64) ([]Operation, error) {
	if _, err := s.Get(ctx, ownerID, runID); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var operations []Operation
	err := s.db.WithContext(ctx).Where("run_id=?", runID).Order("started_at,id").Limit(256).Find(&operations).Error
	return operations, err
}
func boundedError(message string) string {
	if len(message) <= 512 {
		return message
	}
	end := 512
	for end > 0 && !utf8.ValidString(message[:end]) {
		end--
	}
	return message[:end]
}
