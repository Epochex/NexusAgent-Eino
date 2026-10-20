// Package long_term owns scoped memory extraction and its durable projections.
package long_term

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"gorm.io/gorm"
)

type Config struct {
	RecallTopK        int
	MinimumSimilarity float32
}

func DefaultConfig() Config { return Config{RecallTopK: 3, MinimumSimilarity: 0.25} }

type Manager struct {
	gateway *llm.Gateway
	models  ModelResolver
	repo    *Store
	vectors vector.Store
	cfg     Config
}

func NewManager(gateway *llm.Gateway, models ModelResolver, store *Store, vectors vector.Store) *Manager {
	return &Manager{gateway: gateway, models: models, repo: store, vectors: vectors, cfg: DefaultConfig()}
}
func (m *Manager) valid() bool {
	return m != nil && m.gateway != nil && m.models != nil && m.repo != nil && m.repo.db != nil && m.vectors != nil
}
func (m *Manager) EnqueueSession(ctx context.Context, owner int64, sessionID string, force bool, every, maxAttempts int) (*Job, bool, error) {
	if !m.valid() || owner < 1 || sessionID == "" || every < 1 || every > 20 {
		return nil, false, ErrMemoryInvalid
	}
	if actor, ok := security.UserIDFromContext(ctx); ok && actor != owner {
		return nil, false, ErrMemoryNotFound
	}
	ctx, cancel := context.WithTimeout(security.WithUserID(ctx, owner), 2*time.Second)
	defer cancel()
	var session model.Session
	if err := m.repo.db.WithContext(ctx).Select("id").Where("id=? AND user_id=?", sessionID, owner).Take(&session).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, ErrMemoryNotFound
	} else if err != nil {
		return nil, false, err
	}
	if !force {
		var count int64
		err := m.repo.db.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (SELECT id FROM messages WHERE session_id=? AND role IN ('user','assistant') AND status=1 AND id>COALESCE((SELECT last_enqueued_message_id FROM memory_cursors WHERE session_id=?),0) LIMIT ?) pending`, sessionID, sessionID, every).Scan(&count).Error
		if err != nil {
			return nil, false, err
		}
		if count < int64(every) {
			return nil, false, nil
		}
	}
	if _, err := m.models.EnsureMemoryProfile(ctx, owner); err != nil {
		return nil, false, err
	}
	extractor, err := m.models.CaptureMemoryExtractor(ctx)
	if err != nil {
		return nil, false, err
	}
	return m.repo.Enqueue(ctx, EnqueueInput{OwnerID: owner, SessionID: sessionID, Force: force, Every: every, MaxAttempts: maxAttempts, Extractor: extractor})
}
func (m *Manager) Recall(ctx context.Context, owner int64, agent *int64, query string) ([]model.Memory, error) {
	if !m.valid() || owner < 1 || strings.TrimSpace(query) == "" || len(query) > 4096 {
		return nil, ErrMemoryInvalid
	}
	if actor, ok := security.UserIDFromContext(ctx); ok && actor != owner {
		return nil, ErrMemoryNotFound
	}
	ctx, cancel := context.WithTimeout(security.WithUserID(ctx, owner), 2*time.Second)
	defer cancel()
	agent, err := normalizedMemoryAgent(agent)
	if err != nil {
		return nil, err
	}
	profile, err := m.repo.Profile(ctx, owner)
	if errors.Is(err, ErrMemoryNotFound) {
		var legacy int64
		err = memoryTargetScope(m.repo.db.WithContext(ctx), owner, agent).Model(&model.Memory{}).Where("status=1 AND (embedding_version='' OR index_status='needs_reindex')").Count(&legacy).Error
		if err != nil {
			return nil, err
		}
		if legacy > 0 {
			return nil, fmt.Errorf("%w: legacy memory needs reindex", ErrMemoryConflict)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	scope, err := m.repo.ReadyScope(ctx, owner, agent, profile)
	if err != nil {
		return nil, err
	}
	if len(scope.MemoryRevisions) == 0 {
		return nil, nil
	}
	descriptor, err := m.models.ResolveMemoryEmbedding(ctx, profile)
	if err != nil {
		return nil, err
	}
	values, err := m.gateway.Embed(ctx, descriptor, []string{query})
	if err != nil {
		return nil, err
	}
	if len(values) != 1 || len(values[0]) != profile.EmbeddingDimension {
		return nil, fmt.Errorf("memory query embedding dimension mismatch")
	}
	scoped, ok := m.vectors.(vector.ScopedStore)
	if !ok {
		return nil, fmt.Errorf("scoped vector search required")
	}
	hits, err := scoped.SearchScoped(ctx, MemoryCollection(profile.EmbeddingVersion), values[0], m.cfg.RecallTopK, scope)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(hits))
	for _, hit := range hits {
		if hit.Score >= m.cfg.MinimumSimilarity {
			ids = append(ids, hit.ID)
		}
	}
	rows, err := m.repo.ReadyByIDs(ctx, owner, agent, profile, ids)
	if err != nil {
		return nil, err
	}
	byID := map[int64]model.Memory{}
	for _, row := range rows {
		if scope.MemoryRevisions[row.ID] == row.Revision {
			byID[row.ID] = row
		}
	}
	result := make([]model.Memory, 0, len(rows))
	seen := map[int64]bool{}
	for _, hit := range hits {
		if hit.Score < m.cfg.MinimumSimilarity || seen[hit.ID] {
			continue
		}
		if row, ok := byID[hit.ID]; ok {
			seen[hit.ID] = true
			result = append(result, row)
		}
	}
	revisions := make(map[int64]int64, len(result))
	for _, row := range result {
		revisions[row.ID] = row.Revision
	}
	if err := m.repo.RecordHits(ctx, owner, agent, profile, revisions); err != nil {
		slog.Warn("memory recall statistics unavailable", "owner_id", owner)
	}
	return result, nil
}
