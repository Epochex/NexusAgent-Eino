// Workflow definition management. Execution lives in workflow_execution.go.
package service

import (
	"context"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
	"gorm.io/gorm"
	"time"
)

type WorkflowService struct {
	svcs  *Services
	store *execution.Store
}

func NewWorkflowService(svcs *Services) *WorkflowService {
	service := &WorkflowService{svcs: svcs}
	if svcs.Repos != nil && svcs.Repos.DB != nil {
		service.store = execution.NewStore(svcs.Repos.DB)
	}
	return service
}
func (s *WorkflowService) List(ctx context.Context, userID int64, q pagination.Query) ([]model.Workflow, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.svcs.Repos.Workflow.ListWorkflowsForCreator(ctx, userID, q.Offset(), q.Limit())
}
func (s *WorkflowService) Get(ctx context.Context, userID, id int64) (*model.Workflow, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	w, err := s.svcs.Repos.Workflow.GetWorkflowForCreator(ctx, id, userID)
	if err != nil {
		return nil, errcode.ErrRunNotFound.WithMsg("工作流不存在").WithCause(err)
	}
	return w, nil
}
func (s *WorkflowService) Create(ctx context.Context, w *model.Workflow) error {
	d, err := workflow.ParseDSL(w.DSL)
	if err != nil {
		return err
	}
	if w.Name == "" {
		w.Name = d.Name
	}
	if w.Description == "" {
		w.Description = d.Desc
	}
	w.Version = 1
	return s.svcs.Repos.Workflow.CreateWorkflow(ctx, w)
}

type WorkflowUpdateInput struct {
	Version     *int
	Name        *string
	Description *string
	DSL         *string
	IsActive    *bool
	Status      *int16
}

func (s *WorkflowService) Update(ctx context.Context, userID, id int64, in *WorkflowUpdateInput) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if in == nil {
		return errcode.ErrInvalidParam
	}
	cur, err := s.Get(ctx, userID, id)
	if err != nil {
		return err
	}
	expected := cur.Version
	if in.Version != nil && *in.Version != expected {
		return errcode.ErrRunConflict.WithMsg("工作流已更新，请重新加载后编辑")
	}
	if in.DSL != nil {
		d, err := workflow.ParseDSL(*in.DSL)
		if err != nil {
			return err
		}
		cur.DSL = *in.DSL
		if in.Name == nil {
			cur.Name = d.Name
		}
		if in.Description == nil {
			cur.Description = d.Desc
		}
	}
	if in.Name != nil {
		cur.Name = *in.Name
	}
	if in.Description != nil {
		cur.Description = *in.Description
	}
	if in.IsActive != nil {
		cur.IsActive = *in.IsActive
	}
	if in.Status != nil {
		cur.Status = *in.Status
	}
	cur.Version = expected + 1
	err = s.svcs.Repos.Workflow.UpdateWorkflowForCreator(ctx, cur, userID, expected)
	if errors.Is(err, repo.ErrWorkflowVersionConflict) {
		return errcode.ErrRunConflict.WithCause(err)
	}
	return err
}
func (s *WorkflowService) Delete(ctx context.Context, userID, id int64) error {
	err := s.svcs.Repos.Workflow.DeleteWorkflowForCreator(ctx, id, userID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errcode.ErrRunNotFound.WithMsg("工作流不存在")
	}
	return err
}
func (s *WorkflowService) Validate(text string) error { _, err := workflow.ParseDSL(text); return err }
