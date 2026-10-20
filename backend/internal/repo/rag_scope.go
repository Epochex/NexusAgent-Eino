package repo

import (
	"context"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
)

func (r *KBRepo) PublishedScope(ctx context.Context, snapshot *model.KnowledgeBase) (vector.Scope, error) {
	scope := vector.Scope{}
	if snapshot == nil || snapshot.ID < 1 || snapshot.UserID < 1 || len(snapshot.EmbeddingVersion) != 64 {
		return scope, fmt.Errorf("valid published KB identity required")
	}
	if owner, ok := security.UserIDFromContext(ctx); ok && owner != snapshot.UserID {
		return scope, fmt.Errorf("published scope owner mismatch")
	}
	var current model.KnowledgeBase
	if err := r.db.WithContext(ctx).Where("id=? AND user_id=? AND status='active' AND embedding_version=? AND milvus_collection=?", snapshot.ID, snapshot.UserID, snapshot.EmbeddingVersion, snapshot.MilvusCollection).Take(&current).Error; err != nil {
		return scope, err
	}
	var docs []model.Document
	if err := r.db.WithContext(ctx).Select("id,ready_version").Where("kb_id=? AND status='ready' AND ready_version=index_version AND ready_version>0 AND embedding_version=?", current.ID, current.EmbeddingVersion).Order("id").Limit(vector.MaxScopeRecords + 1).Find(&docs).Error; err != nil {
		return scope, err
	}
	if len(docs) > vector.MaxScopeRecords {
		return scope, fmt.Errorf("knowledge base exceeds published document search limit %d", vector.MaxScopeRecords)
	}
	scope.UserID = current.UserID
	scope.KBID = current.ID
	scope.EmbeddingVersion = current.EmbeddingVersion
	scope.DocumentVersions = map[int64]int64{}
	for _, doc := range docs {
		scope.DocumentVersions[doc.ID] = doc.ReadyVersion
	}
	return scope, nil
}
