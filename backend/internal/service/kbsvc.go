// 知识库服务：KB CRUD / 文档上传与 ETL 调度 / 检索测试台。
package service

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/pagination"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

// MaxDocSize 文档上传大小上限（教学版 20MB）。
const MaxDocSize = 20 << 20

// allowedDocTypes 允许的文档类型。
var allowedDocTypes = map[string]bool{
	".pdf": true, ".docx": true, ".md": true, ".markdown": true,
	".txt": true, ".html": true, ".htm": true,
}

// KBService 知识库业务。
type KBService struct {
	svcs   *Services
	cfg    *config.RAG
	minio  *minio.Client
	bucket string
}

// NewKBService 构造。
func NewKBService(svcs *Services) *KBService {
	return &KBService{svcs: svcs, cfg: &svcs.Deps.Cfg.RAG}
}

// SetMinIO 注入对象存储（main 装配）。
func (s *KBService) SetMinIO(cli *minio.Client, bucket string) {
	s.minio = cli
	s.bucket = bucket
}

// CreateKB 创建知识库（同步创建 Milvus 集合）。
func (s *KBService) CreateKB(ctx context.Context, userID int64, kb *model.KnowledgeBase) error {
	if s.svcs.ragEngine == nil {
		return errcode.ErrStorageUnavailable.WithMsg("向量索引服务未装配")
	}
	kb.Name = strings.TrimSpace(kb.Name)
	if kb.Name == "" || len(kb.Name) > 128 || len(kb.Description) > 8192 {
		return errcode.ErrInvalidParam.WithMsg("name 不能为空")
	}
	kb.UserID = userID
	kb.ID = 0
	kb.DocCount = 0
	kb.Status = "initializing"
	// embedding_config_id 有非空外键：未显式指定时自动绑定默认向量化模型
	if kb.EmbeddingConfigID <= 0 {
		m, err := s.svcs.Deps.Repos.LLM.DefaultModel(ctx, s.svcs.Deps.Cfg.LLM.DefaultEmbedding, modelTypeEmbed)
		if err != nil {
			return errcode.ErrInvalidParam.WithMsg("未找到可用的向量化模型配置，请先在模型管理中启用").WithCause(err)
		}
		kb.EmbeddingConfigID = m.ID
	}
	if err := s.svcs.freezeKBEmbedding(ctx, kb); err != nil {
		return err
	}
	if kb.ChunkSize <= 0 {
		kb.ChunkSize = s.cfg.ChunkSize
	}
	if kb.ChunkOverlap <= 0 {
		kb.ChunkOverlap = s.cfg.ChunkOverlap
	}
	if kb.ChunkSize < 64 || kb.ChunkSize > 4096 || kb.ChunkOverlap < 0 || kb.ChunkOverlap > kb.ChunkSize/2 {
		return errcode.ErrInvalidParam.WithMsg("分块大小须为 64..4096，重叠不能超过一半")
	}
	if err := s.svcs.Deps.Repos.KB.CreateKB(ctx, kb); err != nil {
		return errcode.ErrInternal.WithCause(err)
	}
	// 集合名 = kb_{id}（依赖自增 ID，先落库再建集合）
	kb.MilvusCollection = infra.KBCollection(kb.ID, kb.EmbeddingVersion)
	if err := s.svcs.Deps.Repos.KB.UpdateKB(ctx, kb); err != nil {
		return errcode.ErrInternal.WithCause(err)
	}
	initialized, err := s.InitializeKB(ctx, userID, kb.ID)
	if err != nil {
		return err
	}
	*kb = *initialized
	return nil
}

// ListKBs 知识库列表。
func (s *KBService) ListKBs(ctx context.Context, userID int64, q pagination.Query) ([]model.KnowledgeBase, int64, error) {
	return s.svcs.Deps.Repos.KB.ListKBs(ctx, userID, q.Offset(), q.Limit())
}

// GetKB 详情。
func (s *KBService) GetKB(ctx context.Context, userID, id int64) (*model.KnowledgeBase, error) {
	kb, err := s.svcs.Deps.Repos.KB.GetKBForUser(ctx, id, userID)
	if err != nil {
		return nil, errcode.ErrKBNotFound.WithCause(err)
	}
	return kb, nil
}

// DeleteKB 删除知识库（级联删文档/分块/向量集合）。
func (s *KBService) DeleteKB(ctx context.Context, userID, id int64) error {
	if _, err := s.GetKB(ctx, userID, id); err != nil {
		return err
	}
	return ingestionError(ingestion.NewStore(s.svcs.Repos.DB).ArchiveKB(ctx, userID, id))
}

// UploadDoc 上传文档：对象存储 + 建档 + 投递 ETL 任务。
func (s *KBService) UploadDoc(ctx context.Context, userID, kbID int64, filename string, data []byte) (*model.Document, error) {
	return s.uploadDurable(ctx, userID, kbID, filename, data)
}

// ListDocs 文档列表。
func (s *KBService) ListDocs(ctx context.Context, userID, kbID int64, q pagination.Query) ([]model.Document, int64, error) {
	if _, err := s.GetKB(ctx, userID, kbID); err != nil {
		return nil, 0, err
	}
	return s.svcs.Deps.Repos.KB.ListDocs(ctx, kbID, q.Offset(), q.Limit())
}

// DeleteDoc 删除文档（分块 + 向量一并清理）。
func (s *KBService) DeleteDoc(ctx context.Context, userID, kbID, docID int64) error {
	kb, err := s.GetKB(ctx, userID, kbID)
	if err != nil {
		return err
	}
	doc, err := s.svcs.Deps.Repos.KB.GetDoc(ctx, docID)
	if err != nil || doc.KBID != kb.ID {
		return errcode.ErrDocNotFound
	}
	if err = ingestion.NewStore(s.svcs.Repos.DB).Cancel(ctx, userID, docID); err != nil {
		return errcode.ErrInternal.WithCause(err)
	}
	return nil
}

// ListChunks 分块列表。
func (s *KBService) ListChunks(ctx context.Context, userID, docID int64, q pagination.Query) ([]model.DocumentChunk, int64, error) {
	doc, err := s.svcs.Deps.Repos.KB.GetDoc(ctx, docID)
	if err != nil {
		return nil, 0, errcode.ErrDocNotFound
	}
	if _, err := s.GetKB(ctx, userID, doc.KBID); err != nil {
		return nil, 0, err
	}
	return s.svcs.Deps.Repos.KB.ListChunks(ctx, docID, q.Offset(), q.Limit())
}

// TestRetrieve 检索测试台：只检索不回答（调参用，Ep 08 演示核心）。
func (s *KBService) InspectRetrieval(ctx context.Context, userID, kbID int64, query string) (engine.RetrievalResult, error) {
	kb, err := s.GetKB(ctx, userID, kbID)
	if err != nil {
		return engine.RetrievalResult{}, err
	}
	if s.svcs.ragEngine == nil {
		return engine.RetrievalResult{}, errcode.ErrNotImplement.WithMsg("rag engine 未装配")
	}
	return s.svcs.ragEngine.RetrieveDetailed(security.WithUserID(ctx, userID), kb, query)
}

func (s *KBService) TestRetrieve(ctx context.Context, userID, kbID int64, query string) ([]RetrievalRef, error) {
	kb, err := s.GetKB(ctx, userID, kbID)
	if err != nil {
		return nil, err
	}
	if s.svcs.ragEngine == nil {
		return nil, errcode.ErrNotImplement.WithMsg("rag engine 未装配")
	}
	refs, err := s.svcs.ragEngine.Retrieve(ctx, kb, query)
	if err != nil {
		return nil, errcode.From(err)
	}
	out := make([]RetrievalRef, 0, len(refs))
	for _, r := range refs {
		out = append(out, RetrievalRef{
			ChunkID: r.ChunkID, Filename: r.Filename, Page: r.Page,
			Content: r.Content, Score: r.Score,
		})
	}
	return out, nil
}

// RetrievalRef 检索测试台返回项。
type RetrievalRef struct {
	ChunkID  int64   `json:"chunk_id"`
	Filename string  `json:"filename"`
	Page     int     `json:"page"`
	Content  string  `json:"content"`
	Score    float32 `json:"score"`
}

// GetObject 读取文档原文（worker ETL 用）。
func (s *KBService) GetObject(ctx context.Context, objectKey string) ([]byte, error) {
	if s.minio == nil {
		return nil, fmt.Errorf("minio client not configured")
	}
	obj, err := s.minio.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	data, err := io.ReadAll(io.LimitReader(obj, MaxDocSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxDocSize {
		return nil, errcode.ErrFileTooLarge
	}
	return data, nil
}

// extOf 小写扩展名（含点）。
func extOf(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return lower(name[i:])
		}
	}
	return ""
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// mimeOf 按扩展名给 Content-Type（MinIO 下载友好）。
func mimeOf(name string) string {
	switch extOf(name) {
	case ".pdf":
		return "application/pdf"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".html", ".htm":
		return "text/html"
	default:
		return "text/plain"
	}
}
