//go:build integration

package service

import (
	"context"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

func TestA2AManagementRequiresFreshActiveAdministrator(t *testing.T) {
	_, base, owner := workflowServiceFixture(t)
	svcs, err := New(base.Deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := security.WithUserID(context.Background(), owner)
	if _, err = svcs.A2A.List(ctx); err == nil {
		t.Fatal("ordinary user accessed A2A management")
	}
	if err = svcs.Repos.DB.Model(&model.User{}).Where("id=?", owner).Update("role", "admin").Error; err != nil {
		t.Fatal(err)
	}
	if _, err = svcs.A2A.List(ctx); err != nil {
		t.Fatal("active admin denied", err)
	}
	if err = svcs.Repos.DB.Model(&model.User{}).Where("id=?", owner).Update("role", "user").Error; err != nil {
		t.Fatal(err)
	}
	if _, err = svcs.A2A.List(ctx); err == nil {
		t.Fatal("revoked administrator retained A2A management")
	}
}
