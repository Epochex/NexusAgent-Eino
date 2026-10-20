package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *KBRepo) writeChunks(ctx context.Context, chunks []model.DocumentChunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) > 10000 {
		return ingestion.ErrInvalidInput
	}
	job, ok := ingestion.JobFromContext(ctx)
	if !ok {
		return ingestion.ErrLeaseLost
	}
	return ingestion.NewStore(r.db).WithLease(ctx, job, func(tx *gorm.DB) error {
		rows := append([]model.DocumentChunk(nil), chunks...)
		for i := range rows {
			row := &rows[i]
			if row.DocumentID != job.DocumentID || row.KBID != job.KBID || row.IndexVersion != job.IndexVersion || row.EmbeddingVersion != job.EmbeddingVersion || row.Seq != i || row.Content == "" || len(row.Content) > 65536 || !utf8.ValidString(row.Content) || row.TokenCount < 0 {
				return ingestion.ErrConflict
			}
			meta, err := canonicalChunkMeta(row.Meta)
			if err != nil {
				return err
			}
			row.Meta = meta
			row.ID = 0
			row.Status = 1
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, 200).Error; err != nil {
			return err
		}
		var saved []model.DocumentChunk
		if err := tx.Where("document_id=? AND index_version=? AND embedding_version=?", job.DocumentID, job.IndexVersion, job.EmbeddingVersion).Order("seq").Limit(10001).Find(&saved).Error; err != nil {
			return err
		}
		if len(saved) != len(rows) {
			return ingestion.ErrConflict
		}
		for i, row := range saved {
			meta, err := canonicalChunkMeta(row.Meta)
			if err != nil {
				return err
			}
			if row.Seq != i || row.KBID != job.KBID || row.Content != rows[i].Content || row.TokenCount != rows[i].TokenCount || !bytes.Equal(meta, rows[i].Meta) {
				return ingestion.ErrConflict
			}
			chunks[i] = row
		}
		return nil
	})
}
func canonicalChunkMeta(raw []byte) (datatypes.JSON, error) {
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	if len(raw) > 4096 {
		return nil, ingestion.ErrInvalidInput
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || value == nil || decoder.Decode(new(any)) != io.EOF {
		return nil, ingestion.ErrInvalidInput
	}
	normalized, err := json.Marshal(value)
	return datatypes.JSON(normalized), err
}
func visibleChunks(query *gorm.DB) *gorm.DB {
	return query.Model(&model.DocumentChunk{}).Select("document_chunks.*,ready_doc.filename AS source_filename").
		Joins("JOIN documents ready_doc ON ready_doc.id=document_chunks.document_id").
		Joins("JOIN knowledge_bases ready_kb ON ready_kb.id=document_chunks.kb_id").
		Where("document_chunks.status=1 AND ready_doc.status='ready' AND ready_doc.archived_at IS NULL AND ready_kb.archived_at IS NULL AND ready_kb.status='active' AND ready_doc.ready_version=document_chunks.index_version AND ready_doc.embedding_version=document_chunks.embedding_version AND ready_kb.embedding_version=document_chunks.embedding_version AND document_chunks.embedding_version<>''")
}
