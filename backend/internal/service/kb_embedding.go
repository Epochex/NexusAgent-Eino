package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"gorm.io/datatypes"
	"strings"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

// A vector-space fingerprint includes endpoint/model settings/dimension, not
// a secret. Credential rotation must not require rebuilding every document.
func (s *Services) freezeKBEmbedding(ctx context.Context, kb *model.KnowledgeBase) error {
	raw, version, dimension, err := s.freezeEmbeddingSpace(ctx, kb.EmbeddingConfigID, infra.KBVectorSchemaVersion)
	if err != nil {
		return err
	}
	kb.EmbeddingSnapshot = datatypes.JSON(raw)
	kb.EmbeddingVersion = version
	kb.EmbeddingDimension = dimension
	return nil
}
func (s *Services) freezeEmbeddingSpace(ctx context.Context, configID int64, schema string) (json.RawMessage, string, int, error) {
	configured, err := s.Repos.LLM.GetModel(ctx, configID)
	if err != nil || configured.Type != model.ModelTypeEmbed {
		return nil, "", 0, errcode.ErrProviderNotFound.WithMsg("请选择可用的向量化模型")
	}
	raw, err := s.captureModel(configured)
	if err != nil {
		return nil, "", 0, err
	}
	var snapshot frozenModel
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		return nil, "", 0, err
	}
	dimension := configured.EmbeddingDimension
	if dimension == 0 {
		dimension = s.Cfg.Milvus.Dim
	}
	if dimension < 1 || dimension > 16384 {
		return nil, "", 0, errcode.ErrInvalidParam.WithMsg("向量维度必须为1..16384")
	}
	snapshot.EncryptedKey = ""
	snapshot.Descriptor.APIKey = ""
	pinned, err := json.Marshal(snapshot)
	if err != nil {
		return nil, "", 0, err
	}
	identity, err := json.Marshal(struct {
		Model     frozenModel
		Dimension int
		Schema    string
	}{snapshot, dimension, schema})
	if err != nil {
		return nil, "", 0, err
	}
	sum := sha256.Sum256(identity)
	return pinned, hex.EncodeToString(sum[:]), dimension, nil
}
func (s *Services) captureKBModel(ctx context.Context, kb *model.KnowledgeBase) (frozenModel, error) {
	var snapshot frozenModel
	if kb.EmbeddingVersion == "" || kb.EmbeddingDimension < 1 || json.Unmarshal(kb.EmbeddingSnapshot, &snapshot) != nil || snapshot.ConfigID != kb.EmbeddingConfigID {
		return snapshot, errcode.ErrRunConflict.WithMsg("知识库缺少向量空间版本，请重新建立索引")
	}
	return s.capturePinnedModel(ctx, kb.EmbeddingSnapshot, kb.EmbeddingConfigID, model.ModelTypeEmbed)
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

func (s *Services) capturePinnedModel(ctx context.Context, raw []byte, configID int64, kind string) (frozenModel, error) {
	var snapshot frozenModel
	if json.Unmarshal(raw, &snapshot) != nil || snapshot.ConfigID != configID || configID < 1 {
		return snapshot, errcode.ErrRunConflict.WithMsg("固定模型快照无效，需要重新建立索引")
	}
	current, err := s.Repos.LLM.GetModel(ctx, snapshot.ConfigID)
	if err != nil || current.Type != kind || current.Status != 1 || current.Provider == nil || current.Provider.Status != 1 || current.ProviderID != snapshot.ProviderID {
		return snapshot, errcode.ErrProviderNotFound.WithMsg("固定模型或供应商已撤销")
	}
	if strings.TrimRight(current.Provider.BaseURL, "/") != strings.TrimRight(snapshot.Descriptor.BaseURL, "/") {
		return snapshot, errcode.ErrRunConflict.WithMsg("模型服务地址已变更，需要显式迁移向量空间")
	}
	snapshot.EncryptedKey = current.Provider.APIKey
	return snapshot, nil
}
