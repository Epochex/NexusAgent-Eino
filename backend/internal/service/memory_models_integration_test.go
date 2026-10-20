//go:build integration

package service

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"strings"
	"testing"
)

func TestMemoryModelsPinSettingsAndRotateOnlySameOriginCredentials(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	services.Cfg.Milvus.Dim = 7
	services.Cfg.LLM.DefaultEmbedding = "memory-embed"
	services.Cfg.LLM.DefaultChat = "memory-chat"
	oldKey := "public-old-fixture-key"
	cipher, err := services.Encryptor.Encrypt(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	provider := &model.ModelProvider{Code: "memory", Name: "memory", BaseURL: "http://127.0.0.1:1", APIKey: cipher, Status: 1}
	services.Repos.DB.Create(provider)
	embedding := &model.ModelConfig{ProviderID: provider.ID, Alias: "memory-embed", ModelName: "memory-v1", Type: model.ModelTypeEmbed, EmbeddingDimension: 3, Status: 1}
	services.Repos.DB.Create(embedding)
	extractor := &model.ModelConfig{ProviderID: provider.ID, Alias: "memory-chat", ModelName: "extract-v1", Type: model.ModelTypeChat, Status: 1}
	services.Repos.DB.Create(extractor)
	profile, err := services.EnsureMemoryProfile(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := services.CaptureMemoryExtractor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if profile.EmbeddingDimension != 3 || len(profile.EmbeddingVersion) != 64 || strings.Contains(string(profile.EmbeddingSnapshot), oldKey) || strings.Contains(string(profile.EmbeddingSnapshot), cipher) || strings.Contains(string(captured), cipher) {
		t.Fatal("profile dimension/key policy violated")
	}
	_, kbVersion, _, err := services.freezeEmbeddingSpace(context.Background(), embedding.ID, "kb-v2")
	if err != nil || kbVersion == profile.EmbeddingVersion {
		t.Fatal("memory and KB schema identities not separated", err)
	}
	version := profile.EmbeddingVersion
	newKey := "public-new-fixture-key"
	rotated, err := services.Encryptor.Encrypt(newKey)
	if err != nil {
		t.Fatal(err)
	}
	services.Repos.DB.Model(provider).Update("api_key", rotated)
	services.Repos.DB.Model(embedding).Updates(map[string]any{"model_name": "memory-v2", "embedding_dimension": 4})
	services.Repos.DB.Model(extractor).Update("model_name", "extract-v2")
	existing, err := services.EnsureMemoryProfile(context.Background(), owner)
	if err != nil || existing.EmbeddingVersion != version || existing.EmbeddingDimension != 3 {
		t.Fatal("profile silently repinned", err)
	}
	descriptor, err := services.ResolveMemoryEmbedding(context.Background(), profile)
	if err != nil || descriptor.ModelName != "memory-v1" || descriptor.APIKey != newKey {
		t.Fatal("embedding settings drifted", err)
	}
	chat, err := services.ResolveMemoryExtractor(context.Background(), captured)
	if err != nil || chat.ModelName != "extract-v1" || chat.APIKey != newKey {
		t.Fatal("extractor settings drifted", err)
	}
	services.Repos.DB.Model(provider).Update("base_url", "http://127.0.0.1:2")
	if _, err = services.ResolveMemoryEmbedding(context.Background(), profile); err == nil {
		t.Fatal("fresh key could be sent to stale embedding origin")
	}
	if _, err = services.ResolveMemoryExtractor(context.Background(), captured); err == nil {
		t.Fatal("fresh key could be sent to stale extractor origin")
	}
}

var _ long_term.ModelResolver = (*Services)(nil)
