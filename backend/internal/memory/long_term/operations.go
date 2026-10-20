package long_term

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func normalizeDecision(input Decision) (Decision, []byte, string, error) {
	result := Decision{Ops: append([]Operation(nil), input.Ops...), Candidates: map[int64]int64{}}
	if len(input.Ops) > 32 || len(input.Candidates) > 32 {
		return result, nil, "", ErrMemoryInvalid
	}
	for id, revision := range input.Candidates {
		if id < 1 || revision < 1 {
			return result, nil, "", ErrMemoryInvalid
		}
		result.Candidates[id] = revision
	}
	mutated := map[int64]bool{}
	for i := range result.Ops {
		op := &result.Ops[i]
		op.Op = strings.ToUpper(strings.TrimSpace(op.Op))
		op.Content = strings.TrimSpace(op.Content)
		if !utf8.ValidString(op.Content) || len(op.Content) > 1024 {
			return result, nil, "", ErrMemoryInvalid
		}
		switch op.Op {
		case "ADD":
			if op.ID != 0 || op.Content == "" {
				return result, nil, "", ErrMemoryInvalid
			}
		case "UPDATE", "DELETE":
			if op.ID < 1 || result.Candidates[op.ID] < 1 || mutated[op.ID] || op.Op == "UPDATE" && op.Content == "" {
				return result, nil, "", ErrMemoryInvalid
			}
			mutated[op.ID] = true
		case "NOOP":
			if op.ID != 0 && result.Candidates[op.ID] < 1 {
				return result, nil, "", ErrMemoryInvalid
			}
		default:
			return result, nil, "", ErrMemoryInvalid
		}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return result, nil, "", err
	}
	sum := sha256.Sum256(raw)
	return result, raw, hex.EncodeToString(sum[:]), nil
}
func validateCandidates(tx *gorm.DB, job *Job, decision Decision) (map[int64]model.Memory, error) {
	rows := map[int64]model.Memory{}
	for id, revision := range decision.Candidates {
		var memory model.Memory
		if err := memoryTargetScope(tx, job.OwnerID, job.AgentID).Where("id=? AND status=1 AND revision=? AND indexed_revision=revision AND index_status='ready' AND embedding_version=?", id, revision, job.EmbeddingVersion).Clauses(clause.Locking{Strength: "UPDATE"}).First(&memory).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil, ErrMemoryConflict
			}
			return nil, err
		}
		rows[id] = memory
	}
	return rows, nil
}
func (s *Store) SaveDecision(ctx context.Context, job *Job, input Decision) error {
	decision, raw, hash, err := normalizeDecision(input)
	if err != nil {
		return PermanentMemory(err)
	}
	return s.withLease(ctx, job, func(tx *gorm.DB, current *Job) error {
		if current.Kind != "extract" {
			return ErrMemoryInvalid
		}
		if len(current.Decision) > 0 {
			if current.DecisionHash != hash {
				return ErrMemoryConflict
			}
			return nil
		}
		if err := memoryOwnerLock(tx, current.OwnerID); err != nil {
			return err
		}
		if _, err := validateCandidates(tx, current, decision); err != nil {
			return err
		}
		return tx.Model(&Job{}).Where("id=?", current.ID).Updates(map[string]any{"decision": string(raw), "decision_hash": hash}).Error
	})
}
func (s *Store) Apply(ctx context.Context, job *Job) error {
	return s.withLease(ctx, job, func(tx *gorm.DB, current *Job) error {
		if current.Kind != "extract" || len(current.Decision) == 0 {
			return ErrMemoryInvalid
		}
		var saved Decision
		if json.Unmarshal(current.Decision, &saved) != nil {
			return PermanentMemory(ErrMemoryInvalid)
		}
		decision, _, hash, err := normalizeDecision(saved)
		if err != nil || hash != current.DecisionHash {
			return PermanentMemory(ErrMemoryConflict)
		}
		if err = memoryOwnerLock(tx, current.OwnerID); err != nil {
			return err
		}
		if err = memorySource(tx, current); err != nil {
			return err
		}
		targets, err := validateCandidates(tx, current, decision)
		if err != nil {
			return err
		}
		var active int64
		if err = tx.Model(&model.Memory{}).Where("user_id=? AND status=1", current.OwnerID).Count(&active).Error; err != nil {
			return err
		}
		for index, op := range decision.Ops {
			encoded, _ := json.Marshal(op)
			sum := sha256.Sum256(encoded)
			applied := &AppliedOperation{JobID: current.ID, OpIndex: index, PayloadHash: hex.EncodeToString(sum[:]), Op: op.Op}
			switch op.Op {
			case "ADD":
				content, err := cleanMemoryContent(op.Content)
				if err != nil {
					return PermanentMemory(err)
				}
				contentSum := sha256.Sum256([]byte(content))
				contentHash := hex.EncodeToString(contentSum[:])
				keySum := sha256.Sum256([]byte(fmt.Sprintf("memory:%d:%d", current.ID, index)))
				key := hex.EncodeToString(keySum[:])
				memory := model.Memory{UserID: current.OwnerID, AgentID: current.AgentID, Content: content, Scope: "user", VectorID: "", Status: 1, Revision: 1, IndexStatus: "pending", EmbeddingVersion: current.EmbeddingVersion, ContentHash: contentHash, SourceOperationKey: &key}
				if current.AgentID != nil {
					memory.Scope = "agent"
				}
				// Same-content concurrent ADDs converge under the user mutation lock.
				existing := tx.Where("user_id=? AND content_hash=? AND status=1", current.OwnerID, contentHash)
				if current.AgentID == nil {
					existing = existing.Where("agent_id IS NULL")
				} else {
					existing = existing.Where("agent_id=?", *current.AgentID)
				}
				var duplicate model.Memory
				err = existing.Take(&duplicate).Error
				if err == nil {
					memory = duplicate
				} else if err == gorm.ErrRecordNotFound {
					if active >= 1000 {
						return PermanentMemory(fmt.Errorf("active memory limit reached"))
					}
					if err = tx.Create(&memory).Error; err != nil {
						return err
					}
					active++
					if err = queueMemoryVector(tx, current, &memory, "upsert"); err != nil {
						return err
					}
				} else {
					return err
				}
				id := memory.ID
				applied.MemoryID = &id
				applied.ResultRevision = memory.Revision
			case "UPDATE", "DELETE":
				memory := targets[op.ID]
				changed := op.Op == "DELETE" || memory.Content != op.Content
				if changed {
					updates := map[string]any{"revision": memory.Revision + 1, "index_status": "pending"}
					action := "upsert"
					if op.Op == "DELETE" {
						updates["status"] = 2
						action = "delete"
						active--
					} else {
						content, err := cleanMemoryContent(op.Content)
						if err != nil {
							return PermanentMemory(err)
						}
						sum := sha256.Sum256([]byte(content))
						updates["content"] = content
						updates["content_hash"] = hex.EncodeToString(sum[:])
						memory.Content = content
					}
					updated := tx.Model(&model.Memory{}).Where("id=? AND user_id=? AND revision=? AND status=1", memory.ID, current.OwnerID, memory.Revision).Updates(updates)
					if updated.Error != nil {
						return updated.Error
					}
					if updated.RowsAffected != 1 {
						return ErrMemoryConflict
					}
					memory.Revision++
					memory.IndexStatus = "pending"
					if action == "delete" {
						memory.Status = 2
					}
					if err = queueMemoryVector(tx, current, &memory, action); err != nil {
						return err
					}
				}
				id := memory.ID
				applied.MemoryID = &id
				applied.ResultRevision = memory.Revision
			case "NOOP":
				if op.ID > 0 {
					id := op.ID
					applied.MemoryID = &id
					applied.ResultRevision = decision.Candidates[op.ID]
				}
			}
			if err = tx.Create(applied).Error; err != nil {
				return err
			}
		}
		return memoryFinish(tx, current, "done")
	})
}
func queueMemoryVector(tx *gorm.DB, parent *Job, memory *model.Memory, action string) error {
	job := newMemoryJob("vector", memory.UserID, memory.EmbeddingVersion)
	id := memory.ID
	job.MemoryID = &id
	job.MemoryRevision = memory.Revision
	job.AgentID = memory.AgentID
	job.VectorAction = action
	if parent != nil {
		parentID := parent.ID
		job.ParentJobID = &parentID
		job.MaxAttempts = parent.MaxAttempts
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(job)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}
	return memoryNotify(tx, job, "memory.vector")
}
func (s *Store) CompleteVector(ctx context.Context, job *Job) error {
	return s.withLease(ctx, job, func(tx *gorm.DB, current *Job) error {
		if current.Kind != "vector" || current.MemoryID == nil {
			return ErrMemoryInvalid
		}
		var memory model.Memory
		if err := tx.Where("id=? AND user_id=?", *current.MemoryID, current.OwnerID).Clauses(clause.Locking{Strength: "UPDATE"}).First(&memory).Error; err != nil {
			return err
		}
		if memory.Revision != current.MemoryRevision || memory.EmbeddingVersion != current.EmbeddingVersion {
			return memoryFinish(tx, current, "superseded")
		}
		status := "ready"
		if current.VectorAction == "delete" {
			if memory.Status == 1 {
				return memoryFinish(tx, current, "superseded")
			}
			status = "removed"
		} else if current.VectorAction != "upsert" || memory.Status != 1 {
			return memoryFinish(tx, current, "superseded")
		}
		if err := tx.Model(&model.Memory{}).Where("id=? AND revision=?", memory.ID, current.MemoryRevision).Updates(map[string]any{"indexed_revision": current.MemoryRevision, "index_status": status}).Error; err != nil {
			return err
		}
		return memoryFinish(tx, current, "done")
	})
}
