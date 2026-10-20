//go:build integration

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
)

func TestKBFreezesSelectedEmbeddingAndRotatesCredentials(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	services.Cfg.RAG = config.RAG{ChunkSize: 64, ChunkOverlap: 8}
	services.Cfg.Milvus.Dim = 7
	services.Cfg.LLM.DefaultEmbedding = "unrelated-default"
	services.KB = NewKBService(services)
	cipher, err := services.Encryptor.Encrypt("old-test-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "embed", Name: "embed", BaseURL: "http://127.0.0.1:1", APIKey: cipher, Status: 1}
	if err = services.Repos.DB.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	selected := &model.ModelConfig{ProviderID: provider.ID, Alias: "chosen", ModelName: "chosen-v1", Type: model.ModelTypeEmbed, ContextWindow: 128, EmbeddingDimension: 3, Status: 1}
	if err = services.Repos.DB.Create(selected).Error; err != nil {
		t.Fatal(err)
	}
	kb := &model.KnowledgeBase{Name: "pinned", EmbeddingConfigID: selected.ID, ChunkSize: 64, ChunkOverlap: 8}
	kb.UserID = owner
	if err = services.freezeKBEmbedding(context.Background(), kb); err != nil {
		t.Fatal(err)
	}
	if err = services.Repos.DB.Create(kb).Error; err != nil {
		t.Fatal(err)
	}
	kb.MilvusCollection = infra.KBCollection(kb.ID, kb.EmbeddingVersion)
	if err = services.Repos.DB.Model(kb).Update("milvus_collection", kb.MilvusCollection).Error; err != nil {
		t.Fatal(err)
	}
	if len(kb.EmbeddingVersion) != 64 || kb.EmbeddingDimension != 3 || len(kb.EmbeddingSnapshot) == 0 || strings.Contains(string(kb.EmbeddingSnapshot), "old-test-key") {
		t.Fatalf("embedding identity not pinned: %+v", kb)
	}
	originalVersion := kb.EmbeddingVersion
	ranker := &model.ModelConfig{ProviderID: provider.ID, Alias: "bge-reranker", ModelName: "rank-v1", Type: model.ModelTypeRerank, Status: 1}
	if err = services.Repos.DB.Create(ranker).Error; err != nil {
		t.Fatal(err)
	}
	rotated, err := services.Encryptor.Encrypt("rotated-test-key")
	if err != nil {
		t.Fatal(err)
	}
	services.Repos.DB.Model(provider).Update("api_key", rotated)
	services.Repos.DB.Model(selected).Update("model_name", "edited-v2")
	descriptor, dimension, err := services.ResolveKBEmbedding(context.Background(), kb)
	if err != nil || descriptor.ModelName != "chosen-v1" || descriptor.APIKey != "rotated-test-key" || dimension != 3 || kb.EmbeddingVersion != originalVersion {
		t.Fatalf("edited model mixed vector space: %+v %d %v", descriptor, dimension, err)
	}
	flow := &model.Workflow{ID: 1234, Version: 1, DSL: fmt.Sprintf("name: pinned\nnodes: [{key: search,type: kb,kb_id: %d,query: query}]", kb.ID)}
	definition, err := services.Workflow.freeze(context.Background(), owner, flow)
	if err != nil {
		t.Fatal(err)
	}
	var frozen frozenModel
	var frozenRanker frozenModel
	if err = json.Unmarshal(definition.Models["\x00kb-rerank"], &frozenRanker); err != nil || frozenRanker.Descriptor.ModelName != "rank-v1" {
		t.Fatal("KB reranker settings not frozen", err)
	}
	if err = json.Unmarshal(definition.Models[fmt.Sprintf("kb-embedding:%d", kb.ID)], &frozen); err != nil {
		t.Fatal(err)
	}
	if frozen.Descriptor.ModelName != "chosen-v1" || frozen.EncryptedKey != rotated {
		t.Fatal("workflow froze edited model rather than KB vector-space settings")
	}
	services.Repos.DB.Model(provider).Update("base_url", "http://127.0.0.1:2")
	if _, _, err = services.ResolveKBEmbedding(context.Background(), kb); err == nil {
		t.Fatal("fresh credential could be sent to stale provider endpoint")
	}
}
