//go:build integration

package service

import (
	"context"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"testing"
)

type unavailableVector struct{ vector.Store }

func (unavailableVector) EnsureCollection(context.Context, string, int) error {
	return errors.New("injected collection outage")
}
func TestKBInitializationFailureCannotReportActive(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	services.Cfg.RAG = config.RAG{ChunkSize: 64, ChunkOverlap: 8}
	services.Cfg.Milvus.Dim = 3
	provider := &model.ModelProvider{Code: "embed", Name: "embed", BaseURL: "http://127.0.0.1:1", Status: 1}
	services.Repos.DB.Create(provider)
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "embed", ModelName: "embed", Type: model.ModelTypeEmbed, Status: 1}
	services.Repos.DB.Create(configured)
	service := NewKBService(services)
	kb := &model.KnowledgeBase{Name: "unavailable", EmbeddingConfigID: configured.ID}
	if err := service.CreateKB(context.Background(), owner, kb); err == nil {
		t.Error("missing RAG accepted active KB")
	}
	var count int64
	services.Repos.DB.Model(&model.KnowledgeBase{}).Count(&count)
	if count != 0 {
		t.Error("missing dependency created unusable KB")
	}
	services.SetRAGEngine(engine.New(nil, unavailableVector{}, nil, &services.Cfg.RAG, 3))
	kb = &model.KnowledgeBase{Name: "failed", EmbeddingConfigID: configured.ID}
	if err := service.CreateKB(context.Background(), owner, kb); err == nil {
		t.Error("collection failure accepted")
	}
	var got model.KnowledgeBase
	if err := services.Repos.DB.First(&got, kb.ID).Error; err != nil || got.Status != "failed" {
		t.Fatalf("failed collection reported active: %+v %v", got, err)
	}
}
