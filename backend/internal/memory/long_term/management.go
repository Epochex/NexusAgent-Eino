package long_term

import (
	"context"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
	"strings"
	"time"
)

type ProjectionProgress struct {
	VectorPending    int64 `json:"vector_pending"`
	VectorFailed     int64 `json:"vector_failed"`
	VectorDone       int64 `json:"vector_done"`
	VectorSuperseded int64 `json:"vector_superseded"`
}
type JobProgress struct {
	Job
	ProjectionProgress
}
type RebuildProgress struct {
	RebuildResult
	ProjectionProgress
}

func (s *Store) GetJob(ctx context.Context, owner, id int64) (*Job, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var job Job
	err := s.db.WithContext(ctx).Where("id=? AND owner_id=?", id, owner).First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrMemoryNotFound
	}
	return &job, err
}
func projectionCounts(query *gorm.DB) (ProjectionProgress, error) {
	var result ProjectionProgress
	err := query.Select(`COUNT(*) FILTER(WHERE status IN ('queued','running','retry_wait')) AS vector_pending,
 COUNT(*) FILTER(WHERE status='failed') AS vector_failed,
 COUNT(*) FILTER(WHERE status='done') AS vector_done,
 COUNT(*) FILTER(WHERE status IN ('superseded','canceled')) AS vector_superseded`).Scan(&result).Error
	return result, err
}

// Extraction completion means facts and vector intents committed. These counters
// separately describe projection readiness; a completed extraction can still fail indexing.
func (s *Store) JobStatus(ctx context.Context, owner, id int64) (*JobProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	job, err := s.GetJob(ctx, owner, id)
	if err != nil {
		return nil, err
	}
	counts, err := projectionCounts(s.db.WithContext(ctx).Model(&Job{}).Where("owner_id=? AND parent_job_id=? AND kind='vector'", owner, id))
	return &JobProgress{Job: *job, ProjectionProgress: counts}, err
}
func (s *Store) RebuildStatus(ctx context.Context, owner, id int64) (*RebuildProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var rebuild RebuildResult
	err := s.db.WithContext(ctx).Where("id=? AND owner_id=?", id, owner).First(&rebuild).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrMemoryNotFound
	}
	if err != nil {
		return nil, err
	}
	counts, err := projectionCounts(s.db.WithContext(ctx).Model(&Job{}).Where("owner_id=? AND rebuild_id=? AND kind='vector'", owner, id))
	return &RebuildProgress{RebuildResult: rebuild, ProjectionProgress: counts}, err
}

// Hits are conditional on the exact recalled revisions and active space. A
// concurrent archive/update must not credit a stale vector result.
func (s *Store) RecordHits(ctx context.Context, owner int64, agent *int64, profile *Profile, revisions map[int64]int64) error {
	if len(revisions) == 0 {
		return nil
	}
	if len(revisions) > 256 || profile == nil || profile.UserID != owner {
		return ErrMemoryInvalid
	}
	agent, err := normalizedMemoryAgent(agent)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	tuples := make([]string, 0, len(revisions))
	args := make([]any, 0, len(revisions)*2)
	for id, revision := range revisions {
		if id < 1 || revision < 1 {
			return ErrMemoryInvalid
		}
		tuples = append(tuples, "(?,?)")
		args = append(args, id, revision)
	}
	return memoryTargetScope(s.db.WithContext(ctx).Model(&model.Memory{}), owner, agent).
		Where("(id,revision) IN ("+strings.Join(tuples, ",")+")", args...).
		Where("status=1 AND index_status='ready' AND indexed_revision=revision AND embedding_version=? AND EXISTS(SELECT 1 FROM memory_profiles p WHERE p.user_id=memories.user_id AND p.embedding_version=memories.embedding_version AND p.active)", profile.EmbeddingVersion).
		Updates(map[string]any{"hit_count": gorm.Expr("hit_count+1"), "last_hit_at": gorm.Expr("clock_timestamp()")}).Error
}
