package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct{ db *gorm.DB }

func NewStore(db *gorm.DB) *Store { return &Store{db: db} }

// LockKBMutation serializes uploads/archive/reindex without holding a KB row
// while waiting on an executor's job lock (job -> document -> KB row order).
func LockKBMutation(tx *gorm.DB, kbID int64) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':kb-mutation:'||?::text,0))", kbID).Error
}
func (s *Store) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(fn)
}
func ownedDoc(tx *gorm.DB, owner, id int64, lock bool) (*model.Document, error) {
	query := tx.Model(&model.Document{}).Joins("JOIN knowledge_bases kb ON kb.id=documents.kb_id").Where("documents.id=? AND kb.user_id=? AND kb.archived_at IS NULL", id, owner)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: "documents"}})
	}
	var doc model.Document
	err := query.First(&doc).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &doc, err
}
func notify(tx *gorm.DB, job *Job, topic string) error {
	payload, err := json.Marshal(map[string]any{"job_id": job.ID, "document_id": job.DocumentID, "index_version": job.IndexVersion, "generation": job.Generation, "attempts": job.Attempts, "retry_epoch": job.RetryEpoch, "status": job.Status})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("doc:%d:%d:%s", job.ID, job.Generation, topic)
	return tx.Exec(`INSERT INTO execution_outbox(topic,payload,dedup_key) VALUES(?,?::jsonb,?) ON CONFLICT(dedup_key) DO NOTHING`, topic, string(payload), key).Error
}
func (s *Store) Enqueue(ctx context.Context, owner, docID int64) (*Job, error) {
	return s.enqueue(ctx, owner, docID, false, 3)
}
func (s *Store) Reserve(ctx context.Context, owner, docID int64) (*Job, error) {
	return s.enqueue(ctx, owner, docID, true, 3)
}
func (s *Store) ReserveWithBudget(ctx context.Context, owner, docID int64, maxAttempts int) (*Job, error) {
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	return s.enqueue(ctx, owner, docID, true, maxAttempts)
}
func (s *Store) enqueue(ctx context.Context, owner, docID int64, waiting bool, maxAttempts int) (*Job, error) {
	if maxAttempts < 1 || maxAttempts > 10 {
		return nil, ErrInvalidInput
	}
	var result *Job
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		initial, err := ownedDoc(tx, owner, docID, false)
		if err != nil {
			return err
		}
		if err = LockKBMutation(tx, initial.KBID); err != nil {
			return err
		}
		doc, err := ownedDoc(tx, owner, docID, true)
		if err != nil {
			return err
		}
		var kb model.KnowledgeBase
		if err = tx.First(&kb, doc.KBID).Error; err != nil {
			return err
		}
		if doc.ObjectKey == "" || len(doc.ContentHash) != 64 || doc.IndexVersion < 1 || len(doc.EmbeddingVersion) != 64 || doc.EmbeddingVersion != kb.EmbeddingVersion || kb.EmbeddingDimension < 1 || kb.Status != "active" {
			return ErrInvalidInput
		}
		var existing Job
		err = tx.Where("document_id=? AND index_version=? AND kind='index'", docID, doc.IndexVersion).Take(&existing).Error
		if err == nil {
			if !waiting && existing.Status == "awaiting_source" {
				changed := tx.Model(&Job{}).Where("id=? AND status='awaiting_source'", existing.ID).Updates(map[string]any{"status": "queued", "available_at": gorm.Expr("NOW()"), "updated_at": gorm.Expr("NOW()")})
				if changed.Error != nil {
					return changed.Error
				}
				if changed.RowsAffected == 0 {
					if err = tx.First(&existing, existing.ID).Error; err != nil {
						return err
					}
				} else {
					existing.Status = "queued"
					existing.AvailableAt = time.Now()
					if err = tx.Model(doc).Updates(map[string]any{"status": model.DocStatusPending, "error": ""}).Error; err != nil {
						return err
					}
					if err = notify(tx, &existing, "document.index"); err != nil {
						return err
					}
				}
			}
			result = &existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		job := &Job{DocumentID: docID, OwnerID: owner, KBID: doc.KBID, IndexVersion: doc.IndexVersion, EmbeddingVersion: doc.EmbeddingVersion, ProcessorVersion: ProcessorVersion, Kind: "index", Status: "queued", MaxAttempts: maxAttempts, AvailableAt: time.Now(), DeadlineAt: time.Now().Add(24 * time.Hour)}
		status, topic := model.DocStatusPending, "document.index"
		if waiting {
			job.Status = "awaiting_source"
			job.AvailableAt = time.Now().Add(30 * time.Second)
			status = "uploading"
			topic = "document.uploading"
		}
		if err = tx.Create(job).Error; err != nil {
			return err
		}
		if err = tx.Model(doc).Updates(map[string]any{"status": status, "error": ""}).Error; err != nil {
			return err
		}
		if err = updateKBCount(tx, job.KBID); err != nil {
			return err
		}
		if err = notify(tx, job, topic); err != nil {
			return err
		}
		result = job
		return nil
	})
	return result, err
}
func (s *Store) Claim(ctx context.Context, owner string, lease time.Duration) (*Job, error) {
	if owner == "" || len(owner) > 128 || lease < time.Second || lease > time.Minute {
		return nil, ErrInvalidInput
	}
	var result *Job
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		var job Job
		err := tx.Where(`(available_at<=clock_timestamp() AND (status IN ('queued','awaiting_source','retry_wait') OR (status='running' AND lease_until<clock_timestamp()))) OR (deadline_at<=clock_timestamp() AND status IN ('queued','awaiting_source','running','retry_wait'))`).Order("available_at,id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var live bool
		if err = tx.Raw(`SELECT deadline_at>clock_timestamp() FROM document_jobs WHERE id=?`, job.ID).Scan(&live).Error; err != nil {
			return err
		}
		if !live {
			return retire(tx, &job, "failed", "document job deadline exceeded")
		}
		if job.EpochAttempts >= job.MaxAttempts {
			return retire(tx, &job, "failed", "document executor exhausted retry budget")
		}
		if job.ProcessorVersion != ProcessorVersion {
			return retire(tx, &job, "failed", "document processor version unsupported; reindex required")
		}
		if err = source(tx, &job); err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
				return retire(tx, &job, "canceled", "document source or vector space changed")
			}
			return err
		}
		if err = tx.Raw(`UPDATE document_jobs SET status='running',generation=generation+1,attempts=attempts+1,epoch_attempts=epoch_attempts+1,lease_owner=?,lease_until=clock_timestamp()+(? * INTERVAL '1 millisecond'),updated_at=NOW() WHERE id=? RETURNING *`, owner, lease.Milliseconds(), job.ID).Scan(&job).Error; err != nil {
			return err
		}
		if err = notify(tx, &job, "document.running"); err != nil {
			return err
		}
		result = &job
		return nil
	})
	return result, err
}
func source(tx *gorm.DB, job *Job) error {
	var doc model.Document
	if err := tx.Unscoped().First(&doc, job.DocumentID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		return err
	}
	var kb model.KnowledgeBase
	if err := tx.Unscoped().First(&kb, job.KBID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		return err
	}
	if doc.KBID != job.KBID || kb.UserID != job.OwnerID || doc.IndexVersion != job.IndexVersion {
		return ErrConflict
	}
	if job.Kind == "delete" {
		if !doc.ArchivedAt.Valid {
			return ErrConflict
		}
		return nil
	}
	if doc.ArchivedAt.Valid || kb.ArchivedAt.Valid || kb.Status != "active" || doc.EmbeddingVersion != job.EmbeddingVersion || kb.EmbeddingVersion != job.EmbeddingVersion {
		return ErrConflict
	}
	return nil
}
func lockLease(tx *gorm.DB, job *Job) error {
	var current Job
	err := tx.Where("id=? AND status='running' AND generation=? AND lease_owner=? AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()", job.ID, job.Generation, job.LeaseOwner).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if current.DocumentID != job.DocumentID || current.IndexVersion != job.IndexVersion || current.EmbeddingVersion != job.EmbeddingVersion || current.ProcessorVersion != job.ProcessorVersion || current.Kind != job.Kind {
		return ErrConflict
	}
	return source(tx, job)
}

// WithLease is also used by chunk writes; no stale worker can mutate authority.
func (s *Store) WithLease(ctx context.Context, job *Job, fn func(*gorm.DB) error) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, job); err != nil {
			return err
		}
		return fn(tx)
	})
}
func (s *Store) Source(ctx context.Context, job *Job) (*model.Document, *model.KnowledgeBase, error) {
	var doc model.Document
	var kb model.KnowledgeBase
	err := s.WithLease(ctx, job, func(tx *gorm.DB) error {
		if err := tx.Unscoped().First(&doc, job.DocumentID).Error; err != nil {
			return err
		}
		return tx.Unscoped().First(&kb, job.KBID).Error
	})
	return &doc, &kb, err
}
func (s *Store) Renew(ctx context.Context, job *Job, lease time.Duration) error {
	if lease < time.Second || lease > time.Minute {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result := s.db.WithContext(ctx).Model(&Job{}).Where("id=? AND status='running' AND generation=? AND lease_owner=? AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()", job.ID, job.Generation, job.LeaseOwner).Updates(map[string]any{"lease_until": gorm.Expr("clock_timestamp()+(? * INTERVAL '1 millisecond')", lease.Milliseconds()), "updated_at": gorm.Expr("NOW()")})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrLeaseLost
	}
	return nil
}
func bounded(message string) string {
	if len(message) <= 512 {
		return message
	}
	end := 512
	for end > 0 && !utf8.ValidString(message[:end]) {
		end--
	}
	return message[:end]
}

func updateKBCount(tx *gorm.DB, kbID int64) error {
	// Serialize count publication before taking the next statement snapshot.
	// Concurrent completed documents must not overwrite each other's counts.
	var kb model.KnowledgeBase
	if err := tx.Where("id=?", kbID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&kb).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	return tx.Exec(`UPDATE knowledge_bases SET doc_count=(SELECT COUNT(*) FROM documents WHERE kb_id=? AND archived_at IS NULL AND status='ready' AND ready_version>0 AND embedding_version=?) WHERE id=?`, kbID, kb.EmbeddingVersion, kbID).Error
}
func retire(tx *gorm.DB, job *Job, status, message string) error {
	if err := tx.Model(job).Updates(map[string]any{"status": status, "generation": gorm.Expr("generation+1"), "lease_owner": "", "lease_until": nil, "error": bounded(message), "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
		return err
	}
	if job.Kind == "index" {
		if err := tx.Model(&model.Document{}).Where("id=? AND index_version=?", job.DocumentID, job.IndexVersion).Updates(map[string]any{"status": model.DocStatusFailed, "error": bounded(message)}).Error; err != nil {
			return err
		}
	}
	job.Status = status
	job.Generation++
	return notify(tx, job, "document."+status)
}
