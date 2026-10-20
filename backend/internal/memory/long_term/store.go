package long_term

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Store struct{ db *gorm.DB }

func NewStore(db *gorm.DB) *Store { return &Store{db: db} }
func (s *Store) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.db.WithContext(ctx).Transaction(fn)
}
func memoryOwnerLock(tx *gorm.DB, owner int64) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':memory-owner:'||?::bigint::text,0))", owner).Error
}
func validMemorySpace(version string) bool {
	raw, err := hex.DecodeString(version)
	return err == nil && len(raw) == 32
}
func (s *Store) EnsureProfile(ctx context.Context, profile *Profile) (*Profile, error) {
	if profile == nil || profile.UserID < 1 || profile.EmbeddingConfigID < 1 || !validMemorySpace(profile.EmbeddingVersion) || profile.EmbeddingDimension < 1 || profile.EmbeddingDimension > 16384 || !objectJSON(profile.EmbeddingSnapshot) {
		return nil, ErrMemoryInvalid
	}
	var result Profile
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		if err := memoryOwnerLock(tx, profile.UserID); err != nil {
			return err
		}
		err := tx.Where("user_id=? AND active", profile.UserID).Take(&result).Error
		if err == nil {
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		candidate := *profile
		candidate.Active = true
		if err = tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}, {Name: "embedding_version"}}, DoUpdates: clause.Assignments(map[string]any{"active": true})}).Create(&candidate).Error; err != nil {
			return err
		}
		return tx.Where("user_id=? AND active", profile.UserID).Take(&result).Error
	})
	return &result, err
}
func (s *Store) Profile(ctx context.Context, owner int64) (*Profile, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var profile Profile
	err := s.db.WithContext(ctx).Where("user_id=? AND active", owner).Take(&profile).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = ErrMemoryNotFound
	}
	return &profile, err
}
func memoryNotify(tx *gorm.DB, job *Job, topic string) error {
	payload, err := memoryNoticePayload(job)
	if err != nil {
		return err
	}
	return tx.Exec("INSERT INTO execution_outbox(topic,payload,dedup_key) VALUES(?,?::jsonb,?) ON CONFLICT(dedup_key) DO NOTHING", topic, string(payload), noticeKey(job, topic)).Error
}
func (s *Store) Enqueue(ctx context.Context, input EnqueueInput) (*Job, bool, error) {
	if input.OwnerID < 1 || input.SessionID == "" || input.Every < 1 || input.Every > 20 || !objectJSON(input.Extractor) {
		return nil, false, ErrMemoryInvalid
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = 3
	}
	canonical, err := canonicalMemoryModel(input.Extractor)
	if err != nil {
		return nil, false, err
	}
	input.Extractor = canonical
	if input.MaxAttempts < 1 || input.MaxAttempts > 10 {
		return nil, false, ErrMemoryInvalid
	}
	var result *Job
	created := false
	err = s.transaction(ctx, func(tx *gorm.DB) error {
		var session model.Session
		if err := tx.Where("id=? AND user_id=?", input.SessionID, input.OwnerID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&session).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryNotFound
		} else if err != nil {
			return err
		}
		if session.AgentID != nil && *session.AgentID <= 0 {
			session.AgentID = nil
		}
		if session.AgentID != nil {
			var allowed int64
			if err := tx.Model(&model.Agent{}).Where("id=? AND status=1 AND (owner_id IS NULL OR owner_id=?)", *session.AgentID, input.OwnerID).Count(&allowed).Error; err != nil {
				return err
			}
			if allowed != 1 {
				return ErrMemoryNotFound
			}
		}
		if err := tx.Exec("INSERT INTO memory_cursors(session_id) VALUES(?) ON CONFLICT DO NOTHING", session.ID).Error; err != nil {
			return err
		}
		var cursor struct{ LastEnqueuedMessageID int64 }
		if err := tx.Table("memory_cursors").Where("session_id=?", session.ID).Take(&cursor).Error; err != nil {
			return err
		}
		var profile Profile
		if err := tx.Where("user_id=? AND active", input.OwnerID).Take(&profile).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryNotFound
		} else if err != nil {
			return err
		}
		extractorHash := sha256.Sum256(input.Extractor)
		extractorVersion := hex.EncodeToString(extractorHash[:])
		var messages []model.Message
		sourceQuery := func() *gorm.DB {
			return tx.Select("id,role,content").Where("session_id=? AND role IN ? AND status=1", session.ID, []string{"user", "assistant"}).Order("id DESC").Limit(21)
		}
		if err := sourceQuery().Where("id>?", cursor.LastEnqueuedMessageID).Find(&messages).Error; err != nil {
			return err
		}
		if len(messages) == 0 {
			if !input.Force {
				return nil
			}
			if cursor.LastEnqueuedMessageID == 0 {
				return ErrMemoryInvalid
			}
			var existing Job
			err := tx.Where("session_id=? AND through_message_id=? AND embedding_version=? AND extractor_version=? AND kind='extract'", session.ID, cursor.LastEnqueuedMessageID, profile.EmbeddingVersion, extractorVersion).Take(&existing).Error
			if err == nil {
				result = &existing
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			// Explicit re-extraction under a new pinned model/space is a new request.
			if err = sourceQuery().Where("id<=?", cursor.LastEnqueuedMessageID).Find(&messages).Error; err != nil {
				return err
			}
			if len(messages) == 0 {
				return ErrMemoryInvalid
			}
		}
		if !input.Force && len(messages) < input.Every {
			return nil
		}
		warnings := []string{}
		if len(messages) > 20 {
			messages = messages[:20]
			warnings = append(warnings, "source_window_limited")
		}
		through := messages[0].ID
		source := Source{Extractor: append(json.RawMessage(nil), input.Extractor...)}
		total := 0
		for i := len(messages) - 1; i >= 0; i-- {
			message := messages[i]
			if len(message.Content) > 16384 {
				return ErrMemoryInvalid
			}
			total += len(message.Content)
			if total > 128<<10 {
				return ErrMemoryInvalid
			}
			source.Messages = append(source.Messages, SourceMessage{ID: message.ID, Role: message.Role, Content: message.Content})
		}
		payload, err := json.Marshal(source)
		if err != nil {
			return err
		}
		warningJSON, _ := json.Marshal(warnings)
		job := newMemoryJob("extract", input.OwnerID, profile.EmbeddingVersion)
		job.SessionID = &session.ID
		job.AgentID = session.AgentID
		job.ThroughMessageID = through
		job.ExtractorVersion = extractorVersion
		job.Payload = payload
		job.Warnings = warningJSON
		job.MaxAttempts = input.MaxAttempts
		inserted := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(job)
		if inserted.Error != nil {
			return inserted.Error
		}
		if inserted.RowsAffected == 0 {
			if err := tx.Where("session_id=? AND through_message_id=? AND embedding_version=? AND extractor_version=? AND kind='extract'", session.ID, through, profile.EmbeddingVersion, extractorVersion).Take(job).Error; err != nil {
				return err
			}
		} else {
			created = true
			if err := memoryNotify(tx, job, "memory.queued"); err != nil {
				return err
			}
		}
		if err := tx.Table("memory_cursors").Where("session_id=?", session.ID).Update("last_enqueued_message_id", through).Error; err != nil {
			return err
		}
		result = job
		return nil
	})
	return result, created, err
}
func newMemoryJob(kind string, owner int64, version string) *Job {
	return &Job{Kind: kind, OwnerID: owner, EmbeddingVersion: version, ProcessorVersion: MemoryProcessorVersion, Status: "queued", MaxAttempts: 3, AvailableAt: time.Now(), DeadlineAt: time.Now().Add(24 * time.Hour), Payload: datatypes.JSON(`{}`), Warnings: datatypes.JSON(`[]`)}
}
func (s *Store) Claim(ctx context.Context, owner string, lease time.Duration) (*Job, error) {
	if owner == "" || len(owner) > 128 || lease < time.Second || lease > time.Minute {
		return nil, ErrMemoryInvalid
	}
	var result *Job
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		var job Job
		err := tx.Where("(available_at<=clock_timestamp() AND (status IN ('queued','retry_wait') OR (status='running' AND lease_until<clock_timestamp()))) OR (deadline_at<=clock_timestamp() AND status IN ('queued','running','retry_wait'))").Order("available_at,id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var live bool
		if err = tx.Raw("SELECT deadline_at>clock_timestamp() FROM memory_jobs WHERE id=?", job.ID).Scan(&live).Error; err != nil {
			return err
		}
		switch {
		case !live:
			return memoryRetire(tx, &job, "failed", "memory job deadline exceeded")
		case job.EpochAttempts >= job.MaxAttempts:
			return memoryRetire(tx, &job, "failed", "memory executor exhausted retry budget")
		case job.ProcessorVersion != MemoryProcessorVersion:
			return memoryRetire(tx, &job, "failed", "memory processor version requires migration")
		}
		if err = memorySource(tx, &job); err != nil {
			if errors.Is(err, ErrMemoryNotFound) || errors.Is(err, ErrMemoryConflict) {
				return memoryRetire(tx, &job, "canceled", "memory source or scope changed")
			}
			return err
		}
		if err = tx.Raw("UPDATE memory_jobs SET status='running',generation=generation+1,attempts=attempts+1,epoch_attempts=epoch_attempts+1,lease_owner=?,lease_until=clock_timestamp()+(? * INTERVAL '1 millisecond'),updated_at=NOW() WHERE id=? RETURNING *", owner, lease.Milliseconds(), job.ID).Scan(&job).Error; err != nil {
			return err
		}
		if err = memoryNotify(tx, &job, "memory.running"); err != nil {
			return err
		}
		result = &job
		return nil
	})
	return result, err
}
func sameMemoryAgent(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
func memorySource(tx *gorm.DB, job *Job) error {
	if job.Kind == "vector" {
		if job.MemoryID == nil {
			return ErrMemoryConflict
		}
		var memory model.Memory
		if err := tx.Where("id=? AND user_id=?", *job.MemoryID, job.OwnerID).First(&memory).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryNotFound
		} else {
			return err
		}
	}
	if job.Kind != "extract" || job.SessionID == nil {
		return ErrMemoryConflict
	}
	var session model.Session
	if err := tx.Where("id=? AND user_id=?", *job.SessionID, job.OwnerID).First(&session).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrMemoryNotFound
	} else if err != nil {
		return err
	}
	if session.AgentID != nil && *session.AgentID <= 0 {
		session.AgentID = nil
	}
	if !sameMemoryAgent(session.AgentID, job.AgentID) {
		return ErrMemoryConflict
	}
	if session.AgentID != nil {
		var allowed int64
		if err := tx.Model(&model.Agent{}).Where("id=? AND status=1 AND (owner_id IS NULL OR owner_id=?)", *session.AgentID, job.OwnerID).Count(&allowed).Error; err != nil {
			return err
		}
		if allowed != 1 {
			return ErrMemoryConflict
		}
	}
	var count int64
	if err := tx.Model(&Profile{}).Where("user_id=? AND embedding_version=? AND active", job.OwnerID, job.EmbeddingVersion).Count(&count).Error; err != nil {
		return err
	}
	if count != 1 {
		return ErrMemoryConflict
	}
	return nil
}
func (s *Store) withLease(ctx context.Context, job *Job, fn func(*gorm.DB, *Job) error) error {
	return s.withLeaseSource(ctx, job, true, fn)
}
func (s *Store) withLeaseSource(ctx context.Context, job *Job, requireSource bool, fn func(*gorm.DB, *Job) error) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		var current Job
		err := tx.Where("id=? AND generation=? AND lease_owner=? AND status='running' AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()", job.ID, job.Generation, job.LeaseOwner).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&current).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrMemoryLeaseLost
		}
		if err != nil {
			return err
		}
		if current.OwnerID != job.OwnerID || current.Kind != job.Kind || current.MemoryRevision != job.MemoryRevision || current.EmbeddingVersion != job.EmbeddingVersion || current.ProcessorVersion != MemoryProcessorVersion {
			return ErrMemoryConflict
		}
		if requireSource {
			if err = memorySource(tx, &current); err != nil {
				return err
			}
		}
		return fn(tx, &current)
	})
}
func (s *Store) Renew(ctx context.Context, job *Job, lease time.Duration) error {
	if lease < time.Second || lease > time.Minute {
		return ErrMemoryInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result := s.db.WithContext(ctx).Model(&Job{}).Where("id=? AND generation=? AND lease_owner=? AND status='running' AND lease_until>clock_timestamp() AND deadline_at>clock_timestamp()", job.ID, job.Generation, job.LeaseOwner).Updates(map[string]any{"lease_until": gorm.Expr("clock_timestamp()+(? * INTERVAL '1 millisecond')", lease.Milliseconds()), "updated_at": gorm.Expr("NOW()")})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrMemoryLeaseLost
	}
	return nil
}
func memoryFinish(tx *gorm.DB, job *Job, status string) error {
	if err := tx.Model(&Job{}).Where("id=?", job.ID).Updates(map[string]any{"status": status, "lease_owner": "", "lease_until": nil, "error": "", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
		return err
	}
	job.Status = status
	return memoryNotify(tx, job, "memory."+status)
}
func memoryRetire(tx *gorm.DB, job *Job, status, message string) error {
	if err := tx.Model(&Job{}).Where("id=?", job.ID).Updates(map[string]any{"status": status, "generation": gorm.Expr("generation+1"), "lease_owner": "", "lease_until": nil, "error": boundedMemory(message), "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
		return err
	}
	if job.Kind == "vector" && job.MemoryID != nil {
		if err := tx.Model(&model.Memory{}).Where("id=? AND user_id=? AND revision=?", *job.MemoryID, job.OwnerID, job.MemoryRevision).Update("index_status", "failed").Error; err != nil {
			return err
		}
	}
	job.Status = status
	job.Generation++
	return memoryNotify(tx, job, "memory."+status)
}
func memoryTargetScope(query *gorm.DB, owner int64, agent *int64) *gorm.DB {
	query = query.Where("user_id=?", owner)
	if agent == nil {
		return query.Where("agent_id IS NULL")
	}
	return query.Where("(agent_id IS NULL OR agent_id=?)", *agent)
}
func cleanMemoryContent(content string) (string, error) {
	content = strings.TrimSpace(content)
	if content == "" || len(content) > 1024 || !utf8.ValidString(content) {
		return "", fmt.Errorf("%w: memory content must contain 1..1024 bytes", ErrMemoryInvalid)
	}
	return content, nil
}
