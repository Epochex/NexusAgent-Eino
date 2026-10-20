package long_term

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *Store) Fail(ctx context.Context, job *Job, cause error) error {
	if cause == nil {
		return ErrMemoryInvalid
	}
	return s.withLeaseSource(ctx, job, false, func(tx *gorm.DB, current *Job) error {
		if current.EpochAttempts >= current.MaxAttempts || errors.Is(cause, ErrMemoryPermanent) || errors.Is(cause, ErrMemoryInvalid) || errors.Is(cause, ErrMemoryConflict) {
			return memoryRetire(tx, current, "failed", cause.Error())
		}
		delay := time.Duration(1<<min(current.EpochAttempts, 5)) * time.Second
		if err := tx.Model(&Job{}).Where("id=?", current.ID).Updates(map[string]any{"status": "retry_wait", "available_at": gorm.Expr("LEAST(deadline_at,clock_timestamp()+(? * INTERVAL '1 millisecond'))", delay.Milliseconds()), "lease_owner": "", "lease_until": nil, "error": boundedMemory(cause.Error()), "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if current.Kind == "vector" && current.MemoryID != nil {
			if err := tx.Model(&model.Memory{}).Where("id=? AND user_id=? AND revision=?", *current.MemoryID, current.OwnerID, current.MemoryRevision).Update("index_status", "retry_wait").Error; err != nil {
				return err
			}
		}
		current.Status = "retry_wait"
		return memoryNotify(tx, current, "memory.retry_wait")
	})
}
func (s *Store) Retry(ctx context.Context, owner, jobID int64) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		var job Job
		if err := tx.Where("id=? AND owner_id=?", jobID, owner).Clauses(clause.Locking{Strength: "UPDATE"}).First(&job).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryNotFound
		} else if err != nil {
			return err
		}
		if job.Status != "failed" || job.ProcessorVersion != MemoryProcessorVersion {
			return ErrMemoryConflict
		}
		if err := memorySource(tx, &job); err != nil {
			return err
		}
		if job.Kind == "vector" {
			updated := tx.Model(&model.Memory{}).Where("id=? AND user_id=? AND revision=? AND embedding_version=?", *job.MemoryID, owner, job.MemoryRevision, job.EmbeddingVersion).Update("index_status", "pending")
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrMemoryConflict
			}
		}
		updates := map[string]any{"status": "queued", "generation": gorm.Expr("generation+1"), "epoch_attempts": 0, "retry_epoch": gorm.Expr("retry_epoch+1"), "lease_owner": "", "lease_until": nil, "available_at": gorm.Expr("NOW()"), "deadline_at": time.Now().Add(24 * time.Hour), "error": "", "updated_at": gorm.Expr("NOW()")}
		// Apply is atomic. A failed extraction has no committed operation rows, so an
		// explicit retry can reconsider its decision against current revisions.
		if job.Kind == "extract" {
			updates["facts"] = nil
			updates["decision"] = nil
			updates["decision_hash"] = ""
		}
		if err := tx.Model(&Job{}).Where("id=?", job.ID).Updates(updates).Error; err != nil {
			return err
		}
		job.Status = "queued"
		job.Generation++
		job.RetryEpoch++
		return memoryNotify(tx, &job, "memory.retry")
	})
}
func (s *Store) Archive(ctx context.Context, owner, memoryID int64, deleted bool) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := memoryOwnerLock(tx, owner); err != nil {
			return err
		}
		var memory model.Memory
		if err := tx.Where("id=? AND user_id=?", memoryID, owner).Clauses(clause.Locking{Strength: "UPDATE"}).First(&memory).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryNotFound
		} else if err != nil {
			return err
		}
		status := int16(2)
		if deleted {
			status = 3
		}
		if memory.Status == status {
			return nil
		}
		if memory.Status == 3 {
			return ErrMemoryNotFound
		}
		if err := tx.Model(&model.Memory{}).Where("id=?", memory.ID).Updates(map[string]any{"status": status, "revision": memory.Revision + 1, "index_status": "pending"}).Error; err != nil {
			return err
		}
		memory.Revision++
		memory.Status = status
		memory.IndexStatus = "pending"
		return queueMemoryVector(tx, nil, &memory, "delete")
	})
}
func (s *Store) Source(ctx context.Context, job *Job) (*Source, *Decision, error) {
	var source Source
	var decision *Decision
	err := s.withLease(ctx, job, func(_ *gorm.DB, current *Job) error {
		if current.Kind != "extract" || json.Unmarshal(current.Payload, &source) != nil {
			return PermanentMemory(ErrMemoryInvalid)
		}
		if len(current.Decision) > 0 {
			var saved Decision
			if json.Unmarshal(current.Decision, &saved) != nil {
				return PermanentMemory(ErrMemoryInvalid)
			}
			decision = &saved
		}
		if len(current.Facts) > 0 {
			if json.Unmarshal(current.Facts, &source.Facts) != nil {
				return PermanentMemory(ErrMemoryInvalid)
			}
		}
		return nil
	})
	return &source, decision, err
}
func (s *Store) VectorSource(ctx context.Context, job *Job) (*model.Memory, *Profile, bool, error) {
	var memory model.Memory
	var profile *Profile
	live := false
	err := s.withLease(ctx, job, func(tx *gorm.DB, current *Job) error {
		if current.Kind != "vector" || current.MemoryID == nil {
			return ErrMemoryInvalid
		}
		if err := tx.Where("id=? AND user_id=?", *current.MemoryID, current.OwnerID).First(&memory).Error; err != nil {
			return err
		}
		if memory.Revision != current.MemoryRevision || memory.EmbeddingVersion != current.EmbeddingVersion {
			return nil
		}
		if current.VectorAction == "delete" {
			live = memory.Status != 1
			return nil
		}
		if current.VectorAction != "upsert" {
			return PermanentMemory(ErrMemoryInvalid)
		}
		if memory.Status != 1 {
			return nil
		}
		var pinned Profile
		if err := tx.Where("user_id=? AND embedding_version=?", memory.UserID, memory.EmbeddingVersion).First(&pinned).Error; err != nil {
			return PermanentMemory(err)
		}
		profile = &pinned
		live = true
		return nil
	})
	return &memory, profile, live, err
}
func normalizedMemoryAgent(agent *int64) (*int64, error) {
	if agent == nil {
		return nil, nil
	}
	if *agent < 0 {
		return nil, ErrMemoryInvalid
	}
	if *agent == 0 {
		return nil, nil
	}
	return agent, nil
}
func (s *Store) ReadyScope(ctx context.Context, owner int64, agent *int64, profile *Profile) (vector.Scope, error) {
	scope := vector.Scope{}
	if profile == nil || profile.UserID != owner || !validMemorySpace(profile.EmbeddingVersion) {
		return scope, ErrMemoryInvalid
	}
	agent, err := normalizedMemoryAgent(agent)
	if err != nil {
		return scope, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var rows []model.Memory
	if err = memoryTargetScope(s.db.WithContext(ctx), owner, agent).Select("id,revision").Where("status=1 AND index_status='ready' AND indexed_revision=revision AND embedding_version=? AND EXISTS(SELECT 1 FROM memory_profiles p WHERE p.user_id=memories.user_id AND p.embedding_version=memories.embedding_version AND p.active)", profile.EmbeddingVersion).Order("id").Limit(vector.MaxScopeRecords + 1).Find(&rows).Error; err != nil {
		return scope, err
	}
	if len(rows) > vector.MaxScopeRecords {
		return scope, ErrMemoryInvalid
	}
	scope.UserID = owner
	scope.AgentID = agent
	scope.EmbeddingVersion = profile.EmbeddingVersion
	scope.MemoryRevisions = map[int64]int64{}
	for _, row := range rows {
		scope.MemoryRevisions[row.ID] = row.Revision
	}
	return scope, nil
}
func (s *Store) ReadyByIDs(ctx context.Context, owner int64, agent *int64, profile *Profile, ids []int64) ([]model.Memory, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 256 || profile == nil || profile.UserID != owner {
		return nil, ErrMemoryInvalid
	}
	agent, err := normalizedMemoryAgent(agent)
	if err != nil {
		return nil, err
	}
	var rows []model.Memory
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = memoryTargetScope(s.db.WithContext(ctx), owner, agent).Where("id IN ? AND status=1 AND index_status='ready' AND indexed_revision=revision AND embedding_version=? AND EXISTS(SELECT 1 FROM memory_profiles p WHERE p.user_id=memories.user_id AND p.embedding_version=memories.embedding_version AND p.active)", ids, profile.EmbeddingVersion).Find(&rows).Error
	return rows, err
}
func (s *Store) ListJobs(ctx context.Context, owner int64, offset, limit int) ([]Job, int64, error) {
	if offset < 0 || limit < 1 || limit > 100 {
		return nil, 0, ErrMemoryInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	query := s.db.WithContext(ctx).Model(&Job{}).Where("owner_id=?", owner)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []Job
	err := query.Order("id DESC").Offset(offset).Limit(limit).Find(&rows).Error
	return rows, total, err
}
