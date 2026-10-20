package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"strings"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// A vector-space fingerprint includes endpoint/model settings/dimension, not
// a secret. Credential rotation must not require rebuilding every document.
func (s *Services) freezeKBEmbedding(ctx context.Context, kb *model.KnowledgeBase) error {
	configured, err := s.Repos.LLM.GetModel(ctx, kb.EmbeddingConfigID)
	if err != nil || configured.Type != model.ModelTypeEmbed {
		return errcode.ErrProviderNotFound.WithMsg("请选择可用的向量化模型")
	}
	raw, err := s.captureModel(configured)
	if err != nil {
		return err
	}
	var snapshot frozenModel
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}
	dimension := configured.EmbeddingDimension
	if dimension == 0 {
		dimension = s.Cfg.Milvus.Dim
	}
	if dimension < 1 || dimension > 16384 {
		return errcode.ErrInvalidParam.WithMsg("向量维度必须为 1..16384")
	}
	snapshot.EncryptedKey = ""
	pinned, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	identity, err := json.Marshal(struct {
		Model     frozenModel
		Dimension int
		Schema    string
	}{snapshot, dimension, infra.KBVectorSchemaVersion})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(identity)
	kb.EmbeddingSnapshot = pinned
	kb.EmbeddingVersion = hex.EncodeToString(sum[:])
	kb.EmbeddingDimension = dimension
	return nil
}
func (s *Services) captureKBModel(ctx context.Context, kb *model.KnowledgeBase) (frozenModel, error) {
	var snapshot frozenModel
	if kb.EmbeddingVersion == "" || kb.EmbeddingDimension < 1 || json.Unmarshal(kb.EmbeddingSnapshot, &snapshot) != nil || snapshot.ConfigID != kb.EmbeddingConfigID {
		return snapshot, errcode.ErrRunConflict.WithMsg("知识库缺少向量空间版本，请重新建立索引")
	}
	current, err := s.Repos.LLM.GetModel(ctx, snapshot.ConfigID)
	if err != nil || current.Type != model.ModelTypeEmbed || current.Status != 1 || current.Provider == nil || current.Provider.Status != 1 || current.ProviderID != snapshot.ProviderID {
		return snapshot, errcode.ErrProviderNotFound.WithMsg("向量化模型已撤销或不可用")
	}
	// Never send a rotated credential to an endpoint the administrator replaced.
	if strings.TrimRight(current.Provider.BaseURL, "/") != strings.TrimRight(snapshot.Descriptor.BaseURL, "/") {
		return snapshot, errcode.ErrRunConflict.WithMsg("向量化服务地址已变更，需要重新建立知识库索引")
	}
	snapshot.EncryptedKey = current.Provider.APIKey
	return snapshot, nil
}
func (s *Services) ResolveKBEmbedding(ctx context.Context, kb *model.KnowledgeBase) (*llm.ModelDescriptor, int, error) {
	snapshot, err := s.captureKBModel(ctx, kb)
	if err != nil {
		return nil, 0, err
	}
	key, err := s.Encryptor.Decrypt(snapshot.EncryptedKey)
	if err != nil {
		return nil, 0, err
	}
	descriptor := snapshot.Descriptor
	descriptor.APIKey = key
	return &descriptor, kb.EmbeddingDimension, nil
}
