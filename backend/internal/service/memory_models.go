package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"gorm.io/datatypes"
)

func (s *Services) ConfigureMemory() *long_term.Manager {
	if s.Gateway == nil || s.Vector == nil || s.Repos == nil {
		return nil
	}
	manager := long_term.NewManager(s.Gateway, s, long_term.NewStore(s.Repos.DB), s.Vector)
	s.Chat.SetLongTermMemory(manager)
	s.Memory.SetManager(manager)
	return manager
}

func (s *Services) EnsureMemoryProfile(ctx context.Context, owner int64) (*long_term.Profile, error) {
	store := long_term.NewStore(s.Repos.DB)
	profile, err := store.Profile(ctx, owner)
	if err == nil {
		return profile, nil
	}
	if !errors.Is(err, long_term.ErrMemoryNotFound) {
		return nil, err
	}
	configured, err := s.Repos.LLM.DefaultModel(ctx, s.Cfg.LLM.DefaultEmbedding, model.ModelTypeEmbed)
	if err != nil {
		return nil, errcode.ErrProviderNotFound.WithCause(err)
	}
	raw, version, dimension, err := s.freezeEmbeddingSpace(ctx, configured.ID, long_term.MemoryVectorSchemaVersion)
	if err != nil {
		return nil, err
	}
	return store.EnsureProfile(ctx, &long_term.Profile{UserID: owner, EmbeddingConfigID: configured.ID, EmbeddingVersion: version, EmbeddingDimension: dimension, EmbeddingSnapshot: datatypes.JSON(raw)})
}
func (s *Services) CaptureMemoryExtractor(ctx context.Context) (json.RawMessage, error) {
	configured, err := s.Repos.LLM.DefaultModel(ctx, s.Cfg.LLM.DefaultChat, model.ModelTypeChat)
	if err != nil {
		return nil, errcode.ErrProviderNotFound.WithCause(err)
	}
	raw, err := s.captureModel(configured)
	if err != nil {
		return nil, err
	}
	var snapshot frozenModel
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		return nil, err
	}
	snapshot.EncryptedKey = ""
	snapshot.Descriptor.APIKey = ""
	return json.Marshal(snapshot)
}
func (s *Services) ResolveMemoryEmbedding(ctx context.Context, profile *long_term.Profile) (*llm.ModelDescriptor, error) {
	if profile == nil || profile.EmbeddingDimension < 1 {
		return nil, long_term.ErrMemoryConflict
	}
	snapshot, err := s.capturePinnedModel(ctx, profile.EmbeddingSnapshot, profile.EmbeddingConfigID, model.ModelTypeEmbed)
	if err != nil {
		return nil, err
	}
	key, err := s.Encryptor.Decrypt(snapshot.EncryptedKey)
	if err != nil {
		return nil, err
	}
	descriptor := snapshot.Descriptor
	descriptor.APIKey = key
	return &descriptor, nil
}
func (s *Services) ResolveMemoryExtractor(ctx context.Context, raw json.RawMessage) (*llm.ModelDescriptor, error) {
	var pinned frozenModel
	if json.Unmarshal(raw, &pinned) != nil {
		return nil, long_term.ErrMemoryConflict
	}
	snapshot, err := s.capturePinnedModel(ctx, raw, pinned.ConfigID, model.ModelTypeChat)
	if err != nil {
		return nil, err
	}
	key, err := s.Encryptor.Decrypt(snapshot.EncryptedKey)
	if err != nil {
		return nil, err
	}
	descriptor := snapshot.Descriptor
	descriptor.APIKey = key
	return &descriptor, nil
}
