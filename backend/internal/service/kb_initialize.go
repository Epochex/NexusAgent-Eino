package service

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"gorm.io/gorm"
	"time"
)

// InitializeKB is an explicit retry/migration entry point for legacy metadata.
// It pins the selected model once; subsequent retries retain that vector space.
func (s *KBService) InitializeKB(ctx context.Context, owner, kbID int64) (*model.KnowledgeBase, error) {
	if _, err := s.GetKB(ctx, owner, kbID); err != nil {
		return nil, err
	}
	if s.svcs.ragEngine == nil {
		return nil, errcode.ErrStorageUnavailable.WithMsg("向量索引服务未装配")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var kb model.KnowledgeBase
	dbCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	err := s.svcs.Repos.DB.WithContext(dbCtx).Transaction(func(tx *gorm.DB) error {
		if err := ingestion.LockKBMutation(tx, kbID); err != nil {
			return err
		}
		if err := tx.Where("id=? AND user_id=?", kbID, owner).First(&kb).Error; err != nil {
			return err
		}
		if kb.EmbeddingVersion == "" {
			if err := s.svcs.freezeKBEmbedding(dbCtx, &kb); err != nil {
				return err
			}
			kb.MilvusCollection = infra.KBCollection(kb.ID, kb.EmbeddingVersion)
		}
		// Existing valid spaces are never silently renamed or repinned.
		if kb.MilvusCollection != infra.KBCollection(kb.ID, kb.EmbeddingVersion) {
			return errcode.ErrRunConflict.WithMsg("向量空间结构不兼容，请迁移到新的知识库")
		}
		kb.Status = "initializing"
		return tx.Model(&kb).Updates(map[string]any{"embedding_version": kb.EmbeddingVersion, "embedding_dimension": kb.EmbeddingDimension, "embedding_snapshot": kb.EmbeddingSnapshot, "milvus_collection": kb.MilvusCollection, "status": kb.Status}).Error
	})
	stop()
	if err != nil {
		return nil, errcode.From(err)
	}
	err = s.svcs.ragEngine.EnsureKnowledgeBaseVectorCollection(ctx, &kb)
	status := "active"
	if err != nil {
		status = "failed"
	}
	final, cancelFinal := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFinal()
	result := s.svcs.Repos.DB.WithContext(final).Model(&model.KnowledgeBase{}).Where("id=? AND user_id=? AND embedding_version=? AND status IN ('initializing','failed','active')", kb.ID, owner, kb.EmbeddingVersion).Update("status", status)
	if result.Error != nil {
		return nil, errcode.ErrInternal.WithCause(result.Error)
	}
	if result.RowsAffected != 1 {
		return nil, errcode.ErrRunConflict
	}
	kb.Status = status
	if err != nil {
		return nil, errcode.ErrStorageUnavailable.WithMsg("向量集合初始化失败，可重试初始化").WithCause(err)
	}
	return &kb, nil
}

func (s *KBService) RetryDoc(ctx context.Context, owner, kbID, docID int64) error {
	if _, err := s.GetKB(ctx, owner, kbID); err != nil {
		return err
	}
	doc, err := s.svcs.Repos.KB.GetDoc(ctx, docID)
	if err != nil || doc.KBID != kbID {
		return errcode.ErrDocNotFound
	}
	return ingestionError(ingestion.NewStore(s.svcs.Repos.DB).Retry(ctx, owner, docID))
}

// ReindexDoc verifies and reuploads the original bytes into the current durable
// pipeline. The replacement identity is returned; legacy chunks remain invisible.
func (s *KBService) ReindexDoc(ctx context.Context, owner, kbID, docID int64) (*model.Document, error) {
	kb, err := s.GetKB(ctx, owner, kbID)
	if err != nil {
		return nil, err
	}
	doc, err := s.svcs.Repos.KB.GetDoc(ctx, docID)
	if err != nil || doc.KBID != kbID {
		return nil, errcode.ErrDocNotFound
	}
	if doc.Status != "needs_reindex" {
		return nil, errcode.ErrRunConflict.WithMsg("仅旧版本文档需要迁移；处理失败请使用重试")
	}
	if kb.EmbeddingVersion == "" || kb.Status != "active" {
		kb, err = s.InitializeKB(ctx, owner, kbID)
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	data, err := s.GetObject(ctx, doc.ObjectKey)
	if err != nil {
		return nil, errcode.ErrStorageUnavailable.WithCause(err)
	}
	replacement, err := s.uploadDurable(ctx, owner, kbID, doc.Filename, data)
	if err != nil {
		return nil, err
	}
	if replacement.ID == doc.ID {
		return nil, errcode.ErrRunConflict.WithMsg("旧文档身份与新索引冲突，请重新上传原文件")
	}
	if err = ingestion.NewStore(s.svcs.Repos.DB).Cancel(ctx, owner, doc.ID); err != nil {
		return nil, ingestionError(err)
	}
	return replacement, nil
}
