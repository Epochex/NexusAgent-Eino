package service

import (
	"context"
	"errors"

	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

func (s *Services) CapacitySnapshot(ctx context.Context) (*capacity.Snapshot, error) {
	if _, err := s.activeAdmin(ctx); err != nil {
		return nil, err
	}
	if s.Capacity == nil {
		return nil, errcode.ErrInternal
	}
	return s.Capacity.Snapshot(ctx)
}
func (s *Services) UpdateCapacity(ctx context.Context, scope string, p capacity.Policy) (*capacity.Policy, error) {
	if _, err := s.activeAdmin(ctx); err != nil {
		return nil, err
	}
	if s.Capacity == nil {
		return nil, errcode.ErrInternal
	}
	result, err := s.Capacity.UpdatePolicy(ctx, scope, p)
	if errors.Is(err, capacity.ErrInvalidPolicy) {
		return nil, errcode.ErrInvalidParam.WithCause(err)
	}
	if errors.Is(err, capacity.ErrPolicyConflict) {
		return nil, errcode.ErrRunConflict.WithMsg("额度配置已变化，请刷新后重试")
	}
	return result, err
}
