package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/minio/minio-go/v7"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *KBService) uploadDurable(ctx context.Context, owner, kbID int64, filename string, data []byte) (*model.Document, error) {
	if len(data) > MaxDocSize {
		return nil, errcode.ErrFileTooLarge
	}
	if len(data) == 0 {
		return nil, errcode.ErrInvalidParam.WithMsg("文件内容为空")
	}
	filename = path.Base(strings.ReplaceAll(filename, "\\", "/"))
	if filename == "." || filename == "" || len(filename) > 256 || !utf8.ValidString(filename) || strings.ContainsRune(filename, 0) {
		return nil, errcode.ErrInvalidParam.WithMsg("文件名无效或过长")
	}
	if !allowedDocTypes[extOf(filename)] {
		return nil, errcode.ErrFileType
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	kb, err := s.GetKB(ctx, owner, kbID)
	if err != nil {
		return nil, err
	}
	if extOf(filename) == ".pdf" && s.svcs.PDFParser == nil {
		return nil, errcode.ErrUnavailable.WithMsg("PDF 文档解析暂不可用，请联系管理员或上传 TXT/DOCX 文件")
	}
	if s.minio == nil || s.bucket == "" {
		return nil, errcode.ErrStorageUnavailable
	}
	if kb.Status != "active" || len(kb.EmbeddingVersion) != 64 || kb.EmbeddingDimension < 1 {
		return nil, errcode.ErrRunConflict.WithMsg("知识库尚未准备好，需要建立向量空间索引")
	}
	digest := sha256.Sum256(data)
	contentHash := hex.EncodeToString(digest[:])
	var doc model.Document
	var job *ingestion.Job
	// Reserve source+job+outbox before PUT, without holding a DB transaction on IO.
	dbCtx, stopDB := context.WithTimeout(ctx, 3*time.Second)
	err = s.svcs.Repos.DB.WithContext(dbCtx).Transaction(func(tx *gorm.DB) error {
		if err := ingestion.LockKBMutation(tx, kbID); err != nil {
			return err
		}
		var current model.KnowledgeBase
		if err := tx.Where("id=? AND user_id=? AND status='active' AND embedding_version=?", kbID, owner, kb.EmbeddingVersion).First(&current).Error; err != nil {
			return err
		}
		doc = model.Document{KBID: kbID, Filename: filename, FileType: strings.TrimPrefix(extOf(filename), "."), FileSize: int64(len(data)), ContentHash: contentHash, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: "uploading"}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&doc)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			if err := tx.Where("kb_id=? AND filename=? AND content_hash=?", kbID, filename, contentHash).First(&doc).Error; err != nil {
				return err
			}
		}
		if doc.ObjectKey == "" {
			doc.ObjectKey = fmt.Sprintf("docs/%d/%d/%s%s", kbID, doc.ID, contentHash, extOf(filename))
			if err := tx.Model(&doc).Update("object_key", doc.ObjectKey).Error; err != nil {
				return err
			}
		}
		var err error
		job, err = ingestion.NewStore(tx).ReserveWithBudget(dbCtx, owner, doc.ID, s.svcs.Cfg.Worker.MaxRetry)
		return err
	})
	stopDB()
	if err != nil {
		return nil, ingestionError(err)
	}
	if job.Status != "awaiting_source" && job.Status != "failed" {
		return &doc, nil
	}
	unlock, err := ingestion.LockSource(ctx, s.svcs.Repos.DB, doc.ID)
	if err != nil {
		return nil, ingestionError(err)
	}
	defer unlock()
	// Archive can race with the reservation. Cleanup uses this same remote-IO
	// lock, so a late PUT cannot recreate an object after cleanup finishes.
	if _, err = s.svcs.Repos.KB.GetDoc(ctx, doc.ID); err != nil {
		return nil, ingestionError(err)
	}
	if _, err = s.minio.PutObject(ctx, s.bucket, doc.ObjectKey, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: mimeOf(filename), UserMetadata: map[string]string{"sha256": contentHash, "source-document": strconv.FormatInt(doc.ID, 10)}}); err != nil {
		return nil, errcode.ErrStorageUnavailable.WithCause(err)
	}
	jobs := ingestion.NewStore(s.svcs.Repos.DB)
	if job.Status == "failed" {
		if err = jobs.Retry(ctx, owner, doc.ID); err != nil {
			return nil, ingestionError(err)
		}
	} else if _, err = jobs.Enqueue(ctx, owner, doc.ID); err != nil {
		return nil, ingestionError(err)
	}
	return s.svcs.Repos.KB.GetDoc(ctx, doc.ID)
}
func ingestionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ingestion.ErrNotFound) || errors.Is(err, gorm.ErrRecordNotFound):
		return errcode.ErrDocNotFound
	case errors.Is(err, ingestion.ErrConflict) || errors.Is(err, ingestion.ErrLeaseLost):
		return errcode.ErrRunConflict.WithCause(err)
	case errors.Is(err, ingestion.ErrInvalidInput):
		return errcode.ErrInvalidParam.WithCause(err)
	default:
		return errcode.From(err)
	}
}
