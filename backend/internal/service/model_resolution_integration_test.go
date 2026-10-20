//go:build integration

package service

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"testing"
	"time"
)

func TestModelResolutionRespectsCancellationAndRevocation(t *testing.T) {
	_, services, _ := workflowServiceFixture(t)
	provider := &model.ModelProvider{Code: "model", Name: "model", BaseURL: "http://127.0.0.1:1", Status: 1}
	services.Repos.DB.Create(provider)
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "chat", ModelName: "chat", Type: model.ModelTypeChat, Status: 1}
	services.Repos.DB.Create(configured)
	services.Cfg.LLM.DefaultChat = "chat"
	canceled, stop := context.WithCancel(context.Background())
	stop()
	started := time.Now()
	if _, _, err := services.ResolveModel(canceled, 0, 0); err == nil {
		t.Error("canceled default-model lookup ignored context")
	}
	if time.Since(started) > time.Second {
		t.Error("canceled resolution exceeded budget")
	}
	services.Repos.DB.Model(configured).Update("status", 0)
	if _, _, err := services.ResolveModel(context.Background(), 0, 0); err == nil {
		t.Error("disabled default model revived from cache")
	}
	if _, _, err := services.ResolveModel(context.Background(), configured.ID, 0); err == nil {
		t.Error("disabled explicitly selected model accepted")
	}
	services.Repos.DB.Model(configured).Update("status", 1)
	services.Repos.DB.Model(provider).Update("status", 0)
	if _, _, err := services.ResolveModel(context.Background(), configured.ID, 0); err == nil {
		t.Error("disabled provider accepted")
	}
	if _, err := New(services.Deps); err != nil {
		t.Fatal("disabled optional model prevented management startup", err)
	}
}
