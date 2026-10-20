package ingestion

import (
	"context"
	"errors"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ArchiveKB atomically hides the KB and sources, fences all executors, and
// schedules immutable per-source remote cleanup. No network IO is in this tx.
func (s *Store) ArchiveKB(ctx context.Context, owner, kbID int64) error {
	return s.transaction(ctx, func(tx *gorm.DB) error {
		if err := LockKBMutation(tx, kbID); err != nil {
			return err
		}
		var kb model.KnowledgeBase
		if err := tx.Where("id=? AND user_id=?", kbID, owner).Take(&kb).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		// Acquire every job before touching documents or the KB counter.
		var jobs []Job
		if err := tx.Where("kb_id=?", kbID).Order("id").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&jobs).Error; err != nil {
			return err
		}
		if err := tx.Model(&Job{}).Where("kb_id=? AND status IN ('queued','awaiting_source','running','retry_wait')", kbID).Updates(map[string]any{"status": "canceled", "generation": gorm.Expr("generation+1"), "lease_owner": "", "lease_until": nil, "error": "knowledge base archived", "updated_at": gorm.Expr("NOW()")}).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.Document{}).Where("kb_id=?", kbID).Updates(map[string]any{"status": "deleted", "archived_at": gorm.Expr("NOW()"), "index_version": gorm.Expr("index_version+1")}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO document_jobs(document_id,owner_id,kb_id,index_version,embedding_version,processor_version,kind,status,max_attempts,available_at,deadline_at)
   SELECT id,?,kb_id,index_version,embedding_version,?,'delete','queued',3,NOW(),NOW()+INTERVAL '24 hours' FROM documents
   WHERE kb_id=? AND archived_at IS NOT NULL
   ON CONFLICT(document_id,index_version,kind) DO NOTHING`, owner, ProcessorVersion, kbID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO execution_outbox(topic,payload,dedup_key)
   SELECT 'document.delete',jsonb_build_object('job_id',id,'document_id',document_id,'index_version',index_version,'generation',generation,'status',status),
   'doc:'||id||':'||generation||':document.delete' FROM document_jobs
   WHERE kb_id=? AND kind='delete' AND status='queued' ON CONFLICT(dedup_key) DO NOTHING`, kbID).Error; err != nil {
			return err
		}
		return tx.Model(&kb).Updates(map[string]any{"archived_at": gorm.Expr("NOW()"), "status": "archived", "doc_count": 0}).Error
	})
}
