// 记忆服务：长期记忆的管理面（列表/删除/手动抽取）（td.md §7.6，Ep 11）。
//
// 分层说明：抽取与召回的"算法"在 memory/long_term.Manager（网关+向量库协作），
// 本服务只做管理面编排：归属校验、级联清理向量、按需触发抽取。
package service

import (
	"context"
	"errors"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"gorm.io/datatypes"
)

// MemoryService 记忆管理业务。
type MemoryService struct {
	svcs    *Services
	manager *long_term.Manager // main 注入，可 nil（未配置时功能降级）
}

// NewMemoryService 构造。
func NewMemoryService(svcs *Services) *MemoryService {
	return &MemoryService{svcs: svcs}
}

// SetManager 注入长期记忆管理器。
func (s *MemoryService) SetManager(m *long_term.Manager) { s.manager = m }

// List 用户记忆列表（管理面）。
func (s *MemoryService) List(ctx context.Context, userID int64, q pagination.Query) ([]model.Memory, int64, error) {
	list, total, err := s.svcs.Deps.Repos.Memory.ListByUser(ctx, userID, q.Offset(), q.Limit())
	if err != nil {
		return nil, 0, errcode.ErrInternal.WithCause(err)
	}
	return list, total, nil
}

// Delete immediately revokes recall and durably schedules remote cleanup.
func (s *MemoryService) Delete(ctx context.Context, userID, id int64) error {
	return memoryError(long_term.NewStore(s.svcs.Repos.DB).Archive(ctx, userID, id, true))
}
func (s *MemoryService) Archive(ctx context.Context, userID, id int64) error {
	return memoryError(long_term.NewStore(s.svcs.Repos.DB).Archive(ctx, userID, id, false))
}

// ExtractInput 手动抽取请求。
type ExtractInput struct {
	SessionID string `json:"session_id"`
}

// ExtractNow accepts a durable job. Fact and vector completion are separate states.
func (s *MemoryService) ExtractNow(ctx context.Context, userID int64, in *ExtractInput) (*long_term.Job, error) {
	if s.manager == nil {
		return nil, errcode.ErrNotImplement.WithMsg("长期记忆执行器未配置")
	}
	if in == nil || in.SessionID == "" {
		return nil, errcode.ErrInvalidParam.WithMsg("session_id不能为空")
	}
	job, _, err := s.manager.EnqueueSession(ctx, userID, in.SessionID, true, 6, s.svcs.Cfg.Worker.MaxRetry)
	return job, memoryError(err)
}

func (s *MemoryService) Jobs(ctx context.Context, owner int64, q pagination.Query) ([]long_term.Job, int64, error) {
	rows, total, err := long_term.NewStore(s.svcs.Repos.DB).ListJobs(ctx, owner, q.Offset(), q.Limit())
	return rows, total, memoryError(err)
}
func (s *MemoryService) Job(ctx context.Context, owner, id int64) (*long_term.JobProgress, error) {
	result, err := long_term.NewStore(s.svcs.Repos.DB).JobStatus(ctx, owner, id)
	return result, memoryError(err)
}
func (s *MemoryService) Retry(ctx context.Context, owner, id int64) error {
	return memoryError(long_term.NewStore(s.svcs.Repos.DB).Retry(ctx, owner, id))
}
func (s *MemoryService) Profile(ctx context.Context, owner int64) (*long_term.Profile, error) {
	profile, err := long_term.NewStore(s.svcs.Repos.DB).Profile(ctx, owner)
	if errors.Is(err, long_term.ErrMemoryNotFound) {
		return nil, nil
	}
	return profile, memoryError(err)
}

type MemoryRebuildInput struct {
	EmbeddingConfigID int64  `json:"embedding_config_id"`
	RequestKey        string `json:"request_key"`
}
type MemoryEmbeddingOption struct {
	ID        int64  `json:"id"`
	Alias     string `json:"alias"`
	Dimension int    `json:"dimension"`
}

func (s *MemoryService) EmbeddingOptions(ctx context.Context) ([]MemoryEmbeddingOption, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := s.svcs.Repos.LLM.ListModels(ctx, model.ModelTypeEmbed)
	if err != nil {
		return nil, err
	}
	options := make([]MemoryEmbeddingOption, 0, len(rows))
	for _, row := range rows {
		if row.Status != 1 || row.Provider == nil || row.Provider.Status != 1 {
			continue
		}
		dim := row.EmbeddingDimension
		if dim == 0 {
			dim = s.svcs.Cfg.Milvus.Dim
		}
		if dim < 1 || dim > 16384 {
			continue
		}
		options = append(options, MemoryEmbeddingOption{row.ID, row.Alias, dim})
	}
	return options, nil
}

func (s *MemoryService) Rebuild(ctx context.Context, owner int64, in *MemoryRebuildInput) (*long_term.RebuildResult, error) {
	if s.manager == nil {
		return nil, errcode.ErrNotImplement.WithMsg("长期记忆执行器未配置")
	}
	if in == nil || in.EmbeddingConfigID < 1 {
		return nil, errcode.ErrInvalidParam
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, version, dimension, err := s.svcs.freezeEmbeddingSpace(ctx, in.EmbeddingConfigID, long_term.MemoryVectorSchemaVersion)
	if err != nil {
		return nil, err
	}
	result, err := long_term.NewStore(s.svcs.Repos.DB).RebuildProfile(ctx, &long_term.Profile{UserID: owner, EmbeddingConfigID: in.EmbeddingConfigID, EmbeddingVersion: version, EmbeddingDimension: dimension, EmbeddingSnapshot: datatypes.JSON(raw)}, in.RequestKey)
	return result, memoryError(err)
}
func (s *MemoryService) RebuildStatus(ctx context.Context, owner, id int64) (*long_term.RebuildProgress, error) {
	result, err := long_term.NewStore(s.svcs.Repos.DB).RebuildStatus(ctx, owner, id)
	return result, memoryError(err)
}
func memoryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, long_term.ErrMemoryNotFound):
		return errcode.ErrMemoryNotFound
	case errors.Is(err, long_term.ErrMemoryInvalid):
		return errcode.ErrInvalidParam.WithMsg("记忆请求超出允许范围，请缩短内容或减少事实条数").WithCause(err)
	case errors.Is(err, long_term.ErrMemoryConflict), errors.Is(err, long_term.ErrMemoryLeaseLost):
		return errcode.ErrRunConflict.WithMsg("记忆或模型空间已变化，请重新提交").WithCause(err)
	default:
		return errcode.From(err)
	}
}
