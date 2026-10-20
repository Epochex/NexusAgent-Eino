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

type RebuildResult struct {
	ID               int64     `gorm:"primaryKey" json:"id"`
	OwnerID          int64     `json:"-"`
	RequestKey       string    `json:"-"`
	EmbeddingVersion string    `json:"embedding_version"`
	Reindexed        int       `json:"reindexed"`
	Merged           int       `json:"merged"`
	CreatedAt        time.Time `json:"created_at"`
}

func (RebuildResult) TableName() string { return "memory_profile_rebuilds" }

// RebuildProfile atomically switches the active profile and hides/requeues all
// active facts. Remote cleanup/upsert remains in independently fenced jobs.
func (s *Store) RebuildProfile(ctx context.Context, profile *Profile, requestKey string) (*RebuildResult, error) {
	if profile == nil || profile.UserID < 1 || profile.EmbeddingConfigID < 1 || !validMemorySpace(profile.EmbeddingVersion) || profile.EmbeddingDimension < 1 || profile.EmbeddingDimension > 16384 || !objectJSON(profile.EmbeddingSnapshot) || len(requestKey) < 1 || len(requestKey) > 64 {
		return nil, ErrMemoryInvalid
	}
	for _, ch := range requestKey {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
			return nil, ErrMemoryInvalid
		}
	}
	result := &RebuildResult{OwnerID: profile.UserID, RequestKey: requestKey, EmbeddingVersion: profile.EmbeddingVersion}
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		if err := memoryOwnerLock(tx, profile.UserID); err != nil {
			return err
		}
		var existing RebuildResult
		err := tx.Where("owner_id=? AND request_key=?", profile.UserID, requestKey).Take(&existing).Error
		if err == nil {
			if existing.EmbeddingVersion != profile.EmbeddingVersion {
				return ErrMemoryConflict
			}
			*result = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var rows []model.Memory
		if err = tx.Where("user_id=? AND status=1", profile.UserID).Order("id").Limit(1001).Clauses(clause.Locking{Strength: "UPDATE"}).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) > 1000 {
			return PermanentMemory(fmt.Errorf("active memory limit exceeded; archive before rebuilding"))
		}
		type update struct {
			id, revision int64
			hash         string
		}
		var canonical []update
		var duplicates []int64
		var jobs []*Job
		seen := map[string]bool{}
		for _, row := range rows {
			if !utf8.ValidString(row.Content) || row.Content == "" || len(row.Content) > 4096 {
				return PermanentMemory(ErrMemoryInvalid)
			}
			sum := sha256.Sum256([]byte(row.Content))
			hash := hex.EncodeToString(sum[:])
			agent := int64(0)
			if row.AgentID != nil {
				agent = *row.AgentID
			}
			identity := fmt.Sprintf("%d:%s", agent, hash)
			job := newMemoryJob("vector", profile.UserID, row.EmbeddingVersion)
			memoryID := row.ID
			job.MemoryID = &memoryID
			job.MemoryRevision = row.Revision + 1
			job.AgentID = row.AgentID
			if seen[identity] {
				duplicates = append(duplicates, row.ID)
				job.VectorAction = "delete"
				result.Merged++
			} else {
				seen[identity] = true
				canonical = append(canonical, update{row.ID, row.Revision + 1, hash})
				job.VectorAction = "upsert"
				job.EmbeddingVersion = profile.EmbeddingVersion
				result.Reindexed++
				if row.EmbeddingVersion != profile.EmbeddingVersion {
					payload, _ := json.Marshal(map[string]any{"cleanup_previous": true, "previous_space": row.EmbeddingVersion})
					job.Payload = datatypes.JSON(payload)
				}
			}
			jobs = append(jobs, job)
		}
		if err = tx.Model(&Profile{}).Where("user_id=? AND active", profile.UserID).Update("active", false).Error; err != nil {
			return err
		}
		candidate := *profile
		candidate.Active = true
		if err = tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}, {Name: "embedding_version"}}, DoUpdates: clause.Assignments(map[string]any{"active": true})}).Create(&candidate).Error; err != nil {
			return err
		}
		if err = tx.Create(result).Error; err != nil {
			return err
		}
		if len(duplicates) > 0 {
			if err = tx.Model(&model.Memory{}).Where("id IN ? AND user_id=? AND status=1", duplicates, profile.UserID).Updates(map[string]any{"status": 3, "revision": gorm.Expr("revision+1"), "index_status": "pending"}).Error; err != nil {
				return err
			}
		}
		for start := 0; start < len(canonical); start += 200 {
			batch := canonical[start:min(start+200, len(canonical))]
			tuples := make([]string, 0, len(batch))
			args := make([]any, 0, len(batch)*3+2)
			for _, row := range batch {
				tuples = append(tuples, "(?::bigint,?::bigint,?::text)")
				args = append(args, row.id, row.revision, row.hash)
			}
			args = append(args, profile.EmbeddingVersion, profile.UserID)
			statement := "UPDATE memories AS memory SET revision=updates.revision,content_hash=updates.hash,embedding_version=?,index_status='pending',updated_at=NOW() FROM (VALUES " + strings.Join(tuples, ",") + ") AS updates(id,revision,hash) WHERE memory.id=updates.id AND memory.user_id=? AND memory.status=1"
			// Placeholders follow the SET expression before the VALUES block.
			ordered := append([]any{profile.EmbeddingVersion}, args[:len(args)-2]...)
			ordered = append(ordered, profile.UserID)
			if err = tx.Exec(statement, ordered...).Error; err != nil {
				return err
			}
		}
		for _, job := range jobs {
			id := result.ID
			job.RebuildID = &id
		}
		if len(jobs) > 0 {
			if err = tx.CreateInBatches(jobs, 200).Error; err != nil {
				return err
			}
			if err = tx.Exec(`INSERT INTO execution_outbox(topic,payload,dedup_key)
    SELECT 'memory.vector',jsonb_build_object('job_id',id,'kind',kind,'generation',generation,'status',status,'memory_id',memory_id,'memory_revision',memory_revision,'rebuild_id',rebuild_id),
    'memory:'||id||':'||generation||':memory.vector' FROM memory_jobs WHERE rebuild_id=? ON CONFLICT(dedup_key) DO NOTHING`, result.ID).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}
