package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
	"gorm.io/gorm"
)

type RunInput struct {
	WorkflowID int64          `json:"workflow_id"`
	Input      map[string]any `json:"input"`
	RequestKey string         `json:"request_key,omitempty"`
}

func (s *WorkflowService) Run(ctx context.Context, userID int64, in *RunInput) (*model.WorkflowRun, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if in == nil || userID <= 0 || in.WorkflowID <= 0 {
		return nil, errcode.ErrInvalidParam
	}
	if s.store == nil {
		return nil, errcode.ErrInternal
	}
	input, err := json.Marshal(in.Input)
	if err != nil {
		return nil, errcode.ErrInvalidParam.WithCause(err)
	}
	if in.Input == nil {
		input = []byte(`{}`)
	}
	if existing, err := s.store.FindRequest(ctx, userID, in.WorkflowID, in.RequestKey, input); err != nil {
		return nil, workflowError(err)
	} else if existing != nil {
		return s.GetWorkflowRun(ctx, userID, existing.ID)
	}
	w, err := s.Get(ctx, userID, in.WorkflowID)
	if err != nil {
		return nil, err
	}
	definition, err := s.freeze(ctx, userID, w)
	if err != nil {
		return nil, err
	}
	dsl, err := workflow.ParseDSL(definition.DSL)
	if err != nil {
		return nil, err
	}
	for _, key := range dsl.Input {
		if _, exists := in.Input[key]; !exists {
			return nil, errcode.ErrInvalidParam.WithMsg("缺少工作流入参 %q", key)
		}
	}
	deps, err := s.frozenDeps(ctx, &execution.Run{OwnerID: userID}, definition)
	if err != nil {
		return nil, err
	}
	deps.LookupWorkflow = frozenLookup(definition)
	if _, err = workflow.Compile(ctx, dsl, deps, s.store.Checkpoints(&execution.Run{})); err != nil {
		return nil, errcode.ErrWorkflowDSL.WithCause(err)
	}
	raw, err := json.Marshal(definition)
	if err != nil {
		return nil, errcode.ErrInvalidParam.WithCause(err)
	}
	run, err := s.store.EnqueueRequest(ctx, userID, w.ID, raw, input, in.RequestKey, time.Now().Add(24*time.Hour))
	if err != nil {
		return nil, workflowError(err)
	}
	return s.GetWorkflowRun(ctx, userID, run.ID)
}
func (s *WorkflowService) Driver() *execution.Driver {
	driver := execution.NewDriver(s.store, workflow.Deps{})
	driver.DepsFactory = s.frozenDeps
	return driver
}
func (s *WorkflowService) Approve(ctx context.Context, userID, runID int64, approved bool) (*model.WorkflowRun, error) {
	return s.ApproveDecision(ctx, userID, runID, "", "", approved, "")
}
func (s *WorkflowService) ApproveDecision(ctx context.Context, userID, runID int64, interruptID, argsHash string, approved bool, comment string) (*model.WorkflowRun, error) {
	if len(comment) > 2048 {
		return nil, errcode.ErrInvalidParam.WithMsg("审批意见过长")
	}
	if err := s.store.DecideWithComment(ctx, userID, runID, interruptID, argsHash, approved, comment); err != nil {
		return nil, workflowError(err)
	}
	return s.GetWorkflowRun(ctx, userID, runID)
}
func (s *WorkflowService) GetWorkflowRun(ctx context.Context, userID, runID int64) (*model.WorkflowRun, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	record, err := s.svcs.Repos.Workflow.GetRunForCreator(ctx, runID, userID)
	if err != nil {
		return nil, workflowError(err)
	}
	if record.DefinitionHash == "" {
		return record, nil
	}
	durable, err := s.store.Get(ctx, userID, runID)
	if err != nil {
		return nil, workflowError(err)
	}
	record.Status = durable.Status
	record.Output = durable.Output
	record.Error = durable.Error
	record.StateVersion = durable.StateVersion
	record.LastSeq = durable.NextSeq
	record.StartedAt = durable.StartedAt
	record.FinishedAt = durable.FinishedAt
	history, truncated, err := s.store.ApprovalHistory(ctx, userID, runID)
	if err != nil {
		return nil, workflowError(err)
	}
	record.ApprovalHistoryTruncated = truncated
	for _, a := range history {
		record.ApprovalHistory = append(record.ApprovalHistory, model.WorkflowApproval{ID: a.ID, InterruptID: a.InterruptID, Path: a.Path, ArgsHash: a.ArgsHash, Prompt: a.Prompt, Decision: a.Decision, Comment: a.Comment, ExpiresAt: a.ExpiresAt, DecidedAt: a.DecidedAt, DecidedBy: a.DecidedBy})
	}
	for _, a := range durable.Approvals {
		record.Approvals = append(record.Approvals, model.WorkflowApproval{ID: a.ID, InterruptID: a.InterruptID, Path: a.Path, ArgsHash: a.ArgsHash, Prompt: a.Prompt, Decision: a.Decision, Comment: a.Comment, ExpiresAt: a.ExpiresAt})
	}
	operations, err := s.store.Operations(ctx, userID, runID)
	if err != nil {
		return nil, workflowError(err)
	}
	for i, op := range operations {
		record.Steps = append(record.Steps, model.WorkflowStepRun{ID: int64(i + 1), RunID: runID, NodeKey: op.Path, NodeType: op.Kind, Status: op.Status, Output: op.Output, Error: op.Error, StartedAt: &operations[i].StartedAt, FinishedAt: op.FinishedAt})
	}
	return record, nil
}
func (s *WorkflowService) ListRuns(ctx context.Context, userID, workflowID int64, q pagination.Query) ([]model.WorkflowRun, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return s.svcs.Repos.Workflow.ListRunsForCreator(ctx, userID, workflowID, q.Offset(), q.Limit())
}
func (s *WorkflowService) Cancel(ctx context.Context, userID, runID int64) (*model.WorkflowRun, error) {
	if err := s.store.Cancel(ctx, userID, runID); err != nil {
		return nil, workflowError(err)
	}
	return s.GetWorkflowRun(ctx, userID, runID)
}
func (s *WorkflowService) Operations(ctx context.Context, userID, runID int64) ([]execution.Operation, error) {
	ops, err := s.store.Operations(ctx, userID, runID)
	return ops, workflowError(err)
}
func (s *WorkflowService) Reconcile(ctx context.Context, userID, runID int64, operationID, argsHash, output, evidence string) (*model.WorkflowRun, error) {
	if err := s.store.ResolveOperation(ctx, userID, runID, operationID, argsHash, output, evidence); err != nil {
		return nil, workflowError(err)
	}
	return s.GetWorkflowRun(ctx, userID, runID)
}
func (s *WorkflowService) Events(ctx context.Context, userID, runID, after int64) ([]execution.Event, error) {
	events, err := s.store.Events(ctx, userID, runID, after, 128)
	return events, workflowError(err)
}
func workflowError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, execution.ErrNotFound) || errors.Is(err, gorm.ErrRecordNotFound):
		return errcode.ErrRunNotFound
	case errors.Is(err, execution.ErrConflict) || errors.Is(err, execution.ErrVersionMismatch) || errors.Is(err, execution.ErrLeaseLost):
		return errcode.ErrRunConflict.WithCause(err)
	case errors.Is(err, execution.ErrInvalidInput):
		return errcode.ErrInvalidParam.WithCause(err)
	case errors.Is(err, capacity.ErrBusy):
		return errcode.ErrRateLimited.WithMsg("共享待处理任务已达容量上限")
	case errors.Is(err, execution.ErrCursorExpired):
		return errcode.ErrEventCursorExpired
	default:
		return errcode.From(err)
	}
}
