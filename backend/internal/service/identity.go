package service

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"time"
)

func (s *Services) activeUser(ctx context.Context) (*model.User, error) {
	id, ok := security.UserIDFromContext(ctx)
	if !ok || id < 1 {
		return nil, errcode.ErrUnauthorized
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	actor, err := s.Repos.User.GetByID(ctx, id)
	if err != nil || actor.Status != 1 {
		return nil, errcode.ErrForbidden
	}
	return actor, nil
}
func (s *Services) activeAdmin(ctx context.Context) (*model.User, error) {
	actor, err := s.activeUser(ctx)
	if err != nil {
		return nil, err
	}
	if actor.Role != "admin" {
		return nil, errcode.ErrForbidden
	}
	return actor, nil
}
