package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func warningJSON(warnings []string) (datatypes.JSON, error) {
	if len(warnings) > 2000 {
		return nil, ErrInvalidInput
	}
	for _, warning := range warnings {
		if len(warning) > 512 {
			return nil, ErrInvalidInput
		}
	}
	if warnings == nil {
		warnings = []string{}
	}
	raw, err := json.Marshal(warnings)
	return datatypes.JSON(raw), err
}
func (s *Store) Stage(ctx context.Context, job *Job, status string, warnings []string) error {
	return s.WithLease(ctx, job, func(tx *gorm.DB) error {
		if job.Kind != "index" || (status != model.DocStatusParsing && status != model.DocStatusChunking && status != model.DocStatusEmbedding) {
			return ErrInvalidInput
		}
		updates := map[string]any{"status": status, "error": ""}
		if warnings != nil {
			raw, err := warningJSON(warnings)
			if err != nil {
				return err
			}
			updates["parse_warnings"] = raw
		}
		if err := tx.Model(&model.Document{}).Where("id=? AND index_version=?", job.DocumentID, job.IndexVersion).Updates(updates).Error; err != nil {
			return err
		}
		return notify(tx, job, "document."+status)
	})
}
func (s *Store) Complete(ctx context.Context, job *Job, stats Stats) error {
	return s.WithLease(ctx, job, func(tx *gorm.DB) error {
		if job.Kind != "index" || stats.ChunkCount < 1 || stats.ChunkCount > 10000 || stats.TokenCount < 0 {
			return ErrInvalidInput
		}
		warnings, err := warningJSON(stats.Warnings)
		if err != nil {
			return err
		}
		var actual struct{ Chunks, Tokens, First, Last int }
		if err = tx.Raw(`SELECT COUNT(*) AS chunks,COALESCE(SUM(token_count),0) AS tokens,COALESCE(MIN(seq),0) AS first,COALESCE(MAX(seq),-1) AS last FROM document_chunks WHERE document_id=? AND index_version=? AND embedding_version=? AND status=1`, job.DocumentID, job.IndexVersion, job.EmbeddingVersion).Scan(&actual).Error; err != nil {
			return err
		}
		if actual.Chunks != stats.ChunkCount || actual.Tokens != stats.TokenCount || actual.First != 0 || actual.Last != stats.ChunkCount-1 {
			return ErrConflict
		}
		if err = tx.Model(&model.Document{}).Where("id=? AND index_version=?", job.DocumentID, job.IndexVersion).Updates(map[string]any{"status": model.DocStatusReady, "ready_version": job.IndexVersion, "chunk_count": stats.ChunkCount, "token_count": stats.TokenCount, "parse_warnings": warnings, "error": ""}).Error; err != nil {
			return err
		}
		if err = tx.Model(job).Updates(map[string]any{"status": "done", "lease_owner": "", "lease_until": nil, "error": "", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		job.Status = "done"
		if err = updateKBCount(tx, job.KBID); err != nil {
			return err
		}
		return notify(tx, job, "document.ready")
	})
}
func (s *Store) CompleteDelete(ctx context.Context, job *Job) error {
	return s.WithLease(ctx, job, func(tx *gorm.DB) error {
		if job.Kind != "delete" {
			return ErrInvalidInput
		}
		if err := tx.Where("document_id=?", job.DocumentID).Delete(&model.DocumentChunk{}).Error; err != nil {
			return err
		}
		if err := tx.Model(job).Updates(map[string]any{"status": "done", "lease_owner": "", "lease_until": nil, "error": "", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		job.Status = "done"
		return notify(tx, job, "document.deleted")
	})
}
func (s *Store) Fail(ctx context.Context, job *Job, cause error) error {
	if cause == nil {
		return ErrInvalidInput
	}
	return s.WithLease(ctx, job, func(tx *gorm.DB) error {
		if job.EpochAttempts >= job.MaxAttempts || errors.Is(cause, ErrPermanent) {
			return retire(tx, job, "failed", cause.Error())
		}
		delay := time.Duration(1<<min(job.EpochAttempts, 5)) * time.Second
		if err := tx.Model(job).Updates(map[string]any{"status": "retry_wait", "available_at": gorm.Expr("LEAST(deadline_at,clock_timestamp()+(? * INTERVAL '1 millisecond'))", delay.Milliseconds()), "lease_owner": "", "lease_until": nil, "error": bounded(cause.Error()), "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if job.Kind == "index" {
			if err := tx.Model(&model.Document{}).Where("id=? AND index_version=?", job.DocumentID, job.IndexVersion).Updates(map[string]any{"status": "retry_wait", "error": bounded(cause.Error())}).Error; err != nil {
				return err
			}
		}
		job.Status = "retry_wait"
		return notify(tx, job, "document.retry_wait")
	})
}
func (s *Store) Retry(ctx context.Context, owner, docID int64) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		doc, err := ownedDoc(tx, owner, docID, false)
		if err != nil {
			return err
		}
		if err = LockKBMutation(tx, doc.KBID); err != nil {
			return err
		}
		var job Job
		if err = tx.Where("document_id=? AND index_version=? AND kind='index'", docID, doc.IndexVersion).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&job).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		if job.Status != "failed" || job.ProcessorVersion != ProcessorVersion {
			return ErrConflict
		}
		if err = source(tx, &job); err != nil {
			return err
		}
		if err = tx.Model(&job).Updates(map[string]any{"status": "queued", "generation": gorm.Expr("generation+1"), "epoch_attempts": 0, "retry_epoch": gorm.Expr("retry_epoch+1"), "available_at": gorm.Expr("NOW()"), "deadline_at": time.Now().Add(24 * time.Hour), "lease_owner": "", "lease_until": nil, "error": "", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if err = tx.Model(doc).Updates(map[string]any{"status": model.DocStatusPending, "error": ""}).Error; err != nil {
			return err
		}
		job.Status = "queued"
		job.Generation++
		job.RetryEpoch++
		return notify(tx, &job, "document.retry")
	})
}
func (s *Store) Cancel(ctx context.Context, owner, docID int64) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		initial, err := ownedDoc(tx, owner, docID, false)
		if err != nil {
			return err
		}
		if err = LockKBMutation(tx, initial.KBID); err != nil {
			return err
		}
		// Same lock order as stage/publication: jobs first, then document.
		var jobs []Job
		if err := tx.Where("document_id=?", docID).Order("id").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&jobs).Error; err != nil {
			return err
		}
		doc, err := ownedDoc(tx, owner, docID, true)
		if err != nil {
			return err
		}
		if err = tx.Model(&Job{}).Where("document_id=? AND status IN ('queued','awaiting_source','running','retry_wait')", docID).Updates(map[string]any{"status": "canceled", "generation": gorm.Expr("generation+1"), "lease_owner": "", "lease_until": nil, "error": "source archived", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if err = tx.Model(doc).Updates(map[string]any{"status": "deleted", "archived_at": gorm.Expr("NOW()"), "index_version": gorm.Expr("index_version+1")}).Error; err != nil {
			return err
		}
		if err = updateKBCount(tx, doc.KBID); err != nil {
			return err
		}
		cleanup := &Job{DocumentID: docID, OwnerID: owner, KBID: doc.KBID, IndexVersion: doc.IndexVersion + 1, EmbeddingVersion: doc.EmbeddingVersion, ProcessorVersion: ProcessorVersion, Kind: "delete", Status: "queued", MaxAttempts: 3, AvailableAt: time.Now(), DeadlineAt: time.Now().Add(24 * time.Hour)}
		if err = tx.Create(cleanup).Error; err != nil {
			return err
		}
		return notify(tx, cleanup, "document.delete")
	})
}
