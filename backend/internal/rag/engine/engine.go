// Package engine RAG 引擎（td.md §8.5，Ep 06-08）。
//
// 三段式管线，对应三个教学阶段：
//
//	Index（索引）：Load → Split → Chunk 落库(PG) → 批量 Embed → Milvus Upsert
//	Retrieve（检索）：稠密(Milvus HNSW) + 关键词(PG ILIKE/TF-IDF) → RRF 融合 → Rerank
//	Enhance（增强）：top-k 分块拼装上下文 + 引用信息（refs）
//
// 设计要点：检索是"双路召回"，稠密向量擅长语义、关键词擅长专有名词/编号，
// 两者互补后用 RRF（Reciprocal Rank Fusion）融合 —— 这是工业界标配方案。
package engine

import (
	"context"
	"fmt"
	"math"
	"sort"

	"gorm.io/datatypes"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/splitter"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
)

// ChunkRepo 分块数据访问（repo 层实现注入）。
type ChunkRepo interface {
	// BatchCreate 批量写入分块（事务）。
	BatchCreate(ctx context.Context, chunks []model.DocumentChunk) error
	// ListByDoc 拉取文档全部分块。
	ListByDoc(ctx context.Context, docID int64) ([]model.DocumentChunk, error)
	// DeleteByDoc 删除文档全部分块。
	DeleteByDoc(ctx context.Context, docID int64) error
	// KeywordSearch 关键词检索（PG 侧，ILIKE/TF-IDF 打分）。
	KeywordSearch(ctx context.Context, kbID int64, terms []string, limit int) ([]ScoredChunk, error)
	// ListByIDs 按 ID 批量回查（Milvus 只存向量+ID，稠密命中需回表取 content）。
	ListByIDs(ctx context.Context, ids []int64) ([]model.DocumentChunk, error)
	PublishedScope(context.Context, *model.KnowledgeBase) (vector.Scope, error)
}

// ScoredChunk 带分数的分块（关键词检索结果）。
type ScoredChunk struct {
	Chunk    model.DocumentChunk
	Filename string  // 所属文档名
	Score    float64 // ILIKE/TF-IDF 分数
}

// Engine RAG 引擎。
type Engine struct {
	gateway *llm.Gateway
	store   vector.Store
	repo    ChunkRepo
	cfg     *config.RAG
	dim     int

	embedMdl         func() *llm.ModelDescriptor // Embedding 模型取值函数（每次调用实时解析，配置变更即时生效）
	rerankMdl        func() *llm.ModelDescriptor // Rerank 模型取值函数（返回 nil 时跳过 rerank）
	resolveEmbedding func(context.Context, *model.KnowledgeBase) (*llm.ModelDescriptor, int, error)
	indexProgress    func(context.Context) error
	routes           *RetrievalRoutes
	resolveRerank    func(context.Context) (*llm.ModelDescriptor, error)
}

type RetrievalRoutes struct{ Dense, Keyword, Rerank bool }

func (e *Engine) WithRetrievalRoutes(routes RetrievalRoutes) *Engine {
	copy := *e
	copy.routes = &routes
	return &copy
}
func (e *Engine) SetRerankResolver(resolve func(context.Context) (*llm.ModelDescriptor, error)) {
	e.resolveRerank = resolve
}
func (e *Engine) WithRerankResolver(resolve func(context.Context) (*llm.ModelDescriptor, error)) *Engine {
	copy := *e
	copy.resolveRerank = resolve
	return &copy
}

func (e *Engine) WithIndexProgress(progress func(context.Context) error) *Engine {
	copy := *e
	copy.indexProgress = progress
	return &copy
}

// New 构造引擎。
func New(gateway *llm.Gateway, store vector.Store, repo ChunkRepo, cfg *config.RAG, dim int) *Engine {
	return &Engine{gateway: gateway, store: store, repo: repo, cfg: cfg, dim: dim}
}

// SetModels 注入模型取值函数（service 层的 Default*() 实时解析器）。
func (e *Engine) SetModels(embed, rerank func() *llm.ModelDescriptor) {
	e.embedMdl = embed
	e.rerankMdl = rerank
}

func (e *Engine) SetEmbeddingResolver(resolve func(context.Context, *model.KnowledgeBase) (*llm.ModelDescriptor, int, error)) {
	e.resolveEmbedding = resolve
}
func (e *Engine) WithEmbeddingResolver(resolve func(context.Context, *model.KnowledgeBase) (*llm.ModelDescriptor, int, error)) *Engine {
	copy := *e
	copy.resolveEmbedding = resolve
	return &copy
}
func (e *Engine) embeddingFor(ctx context.Context, kb *model.KnowledgeBase) (*llm.ModelDescriptor, int, error) {
	var descriptor *llm.ModelDescriptor
	dimension := e.dim
	if e.resolveEmbedding != nil {
		var err error
		descriptor, dimension, err = e.resolveEmbedding(ctx, kb)
		if err != nil {
			return nil, 0, err
		}
	} else if e.embedMdl != nil {
		descriptor = e.embedMdl()
		if kb.EmbeddingDimension > 0 {
			dimension = kb.EmbeddingDimension
		}
	}
	if descriptor == nil || dimension < 1 {
		return nil, 0, fmt.Errorf("embedding model/space is not configured")
	}
	copy := *descriptor
	return &copy, dimension, nil
}

// EnsureKBVectorCollection 确保知识库的 Milvus 集合存在。
func (e *Engine) EnsureKBVectorCollection(ctx context.Context, collection string) error {
	return e.store.EnsureCollection(ctx, collection, e.dim)
}
func (e *Engine) EnsureKnowledgeBaseVectorCollection(ctx context.Context, kb *model.KnowledgeBase) error {
	if kb.EmbeddingDimension < 1 {
		return fmt.Errorf("knowledge base dimension missing")
	}
	return e.store.EnsureCollection(ctx, kb.MilvusCollection, kb.EmbeddingDimension)
}

// IndexDocument 文档索引管线（worker 的 doc-etl 消费此方法）。
//
//	输入：已从对象存储下载并解析好的文档原文
//	状态机：chunking → embedding → ready / failed（由调用方推进）
func (e *Engine) IndexDocument(ctx context.Context, kb *model.KnowledgeBase, doc *model.Document, loaded *loader.LoadedDoc) error {
	if kb == nil || doc == nil || loaded == nil || e.repo == nil || e.store == nil || e.gateway == nil {
		return fmt.Errorf("index dependencies/document are incomplete")
	}
	if doc.KBID != kb.ID {
		return fmt.Errorf("document does not belong to knowledge base")
	}
	embedding, dimension, err := e.embeddingFor(ctx, kb)
	if err != nil {
		return err
	}
	if err = e.store.EnsureCollection(ctx, kb.MilvusCollection, dimension); err != nil {
		return err
	}
	// ① 分块：按知识库自定义参数（无则用全局配置）
	chunkSize, chunkOverlap := kb.ChunkSize, kb.ChunkOverlap
	if chunkSize <= 0 {
		chunkSize = e.cfg.ChunkSize
	}
	if chunkOverlap <= 0 {
		chunkOverlap = e.cfg.ChunkOverlap
	}
	if chunkSize < 64 || chunkSize > 4096 || chunkOverlap < 0 || chunkOverlap > chunkSize/2 {
		return fmt.Errorf("chunk size must be 64..4096 with overlap at most half")
	}
	sp := splitter.New(chunkSize, chunkOverlap)

	// ② 落库：分块先写入 PG（Milvus 主键 = chunk.ID，必须先有 ID）
	// 页码等定位信息存 Meta JSONB（{"page":3}），引用时回显"第几页"
	var chunks []model.DocumentChunk
	for _, page := range loaded.Pages {
		pageChunks, err := sp.SplitBounded(page.Content, page.Page, 10000-len(chunks))
		if err != nil {
			return err
		}
		for _, c := range pageChunks {
			chunks = append(chunks, model.DocumentChunk{
				IndexVersion: doc.IndexVersion, EmbeddingVersion: doc.EmbeddingVersion,
				KBID:       kb.ID,
				DocumentID: doc.ID,
				Seq:        len(chunks),
				Content:    c.Content,
				TokenCount: llm.EstimateTokens(c.Content),
				Meta:       datatypes.JSON(fmt.Sprintf(`{"page":%d}`, c.Page)),
			})
		}
	}
	if len(chunks) == 0 {
		return fmt.Errorf("document produced no chunks: %s", doc.Filename)
	}
	if len(chunks) > 10000 {
		return fmt.Errorf("document exceeds 10000 chunks")
	}
	if err := e.repo.BatchCreate(ctx, chunks); err != nil {
		return fmt.Errorf("save chunks: %w", err)
	}

	// ③ 向量化（分批，避免单次请求过大）
	if e.indexProgress != nil {
		if err = e.indexProgress(ctx); err != nil {
			return err
		}
	}
	if err := e.embedChunks(ctx, kb, embedding, dimension, chunks); err != nil {
		return fmt.Errorf("embed chunks: %w", err)
	}

	// ④ 统计回填
	doc.ChunkCount = len(chunks)
	totalTokens := 0
	for _, c := range chunks {
		totalTokens += c.TokenCount
	}
	doc.TokenCount = totalTokens
	return nil
}

// embedChunks 批量向量化并写入 Milvus。
func (e *Engine) embedChunks(ctx context.Context, kb *model.KnowledgeBase, embedding *llm.ModelDescriptor, dimension int, chunks []model.DocumentChunk) error {
	batch := e.cfg.BatchEmbed
	if batch <= 0 {
		batch = 64
	}
	if batch > 256 {
		batch = 256
	}
	for i := 0; i < len(chunks); i += batch {
		end := i + batch
		if end > len(chunks) {
			end = len(chunks)
		}
		batchChunks := chunks[i:end]

		texts := make([]string, 0, len(batchChunks))
		for _, c := range batchChunks {
			if embedding.ContextWindow > 0 && c.TokenCount > embedding.ContextWindow {
				return fmt.Errorf("chunk exceeds configured embedding context window")
			}
			texts = append(texts, c.Content)
		}
		vecs, err := e.gateway.Embed(ctx, embedding, texts)
		if err != nil {
			return err
		}
		if len(vecs) != len(batchChunks) {
			return fmt.Errorf("embedding count %d differs from input %d", len(vecs), len(batchChunks))
		}
		for _, values := range vecs {
			if len(values) != dimension {
				return fmt.Errorf("embedding dimension %d differs from collection %d", len(values), dimension)
			}
			for _, value := range values {
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					return fmt.Errorf("embedding contains non-finite values")
				}
			}
		}

		records := make([]vector.Record, 0, len(batchChunks))
		for j, c := range batchChunks {
			records = append(records, vector.Record{
				ID:     c.ID,
				Vector: vecs[j],
				Metadata: map[string]any{
					"kb_id": kb.ID, "user_id": kb.UserID, "index_version": c.IndexVersion, "embedding_version": c.EmbeddingVersion,
					"doc_id":  c.DocumentID,
					"seq":     int32(c.Seq),
					"page":    int32(chunkPage(c)),
					"content": c.Content,
				},
			})
		}
		if err := e.store.Upsert(ctx, kb.MilvusCollection, records); err != nil {
			return err
		}
	}
	return nil
}

// DeleteDocument 删除文档的全部分块与向量（文档删除时调用）。
// DeleteDocumentVectors removes all immutable spaces before authority is finalized.
func (e *Engine) DeleteDocumentVectors(ctx context.Context, kb *model.KnowledgeBase, docID int64) error {
	chunks, err := e.repo.ListByDoc(ctx, docID)
	if err != nil {
		return err
	}
	groups := map[string][]int64{}
	for _, chunk := range chunks {
		collection := infra.KBCollection(kb.ID, chunk.EmbeddingVersion)
		groups[collection] = append(groups[collection], chunk.ID)
	}
	for collection, ids := range groups {
		for i := 0; i < len(ids); i += 256 {
			if err = e.store.Delete(ctx, collection, ids[i:min(i+256, len(ids))]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) DeleteDocument(ctx context.Context, collection string, docID int64) error {
	chunks, err := e.repo.ListByDoc(ctx, docID)
	if err != nil {
		return err
	}
	ids := make([]int64, 0, len(chunks))
	for _, c := range chunks {
		ids = append(ids, c.ID)
	}
	if len(ids) > 0 {
		if err := e.store.Delete(ctx, collection, ids); err != nil {
			return fmt.Errorf("delete vectors: %w", err)
		}
	}
	return e.repo.DeleteByDoc(ctx, docID)
}

// Ref 检索引用（前端展示"参考来源"）。
type Ref struct {
	ChunkID    int64   `json:"chunk_id"`
	DocumentID int64   `json:"document_id"`
	Filename   string  `json:"filename"`
	Page       int     `json:"page"`
	Content    string  `json:"content"`
	Score      float32 `json:"score"`
}

// BuildContext 把检索引用拼装成注入 prompt 的上下文文本。
func BuildContext(refs []Ref) string {
	if len(refs) == 0 {
		return ""
	}
	out := "以下是知识库中检索到的参考资料（回答时请标注 [来源编号]）：\n"
	for i, r := range refs {
		out += fmt.Sprintf("\n[%d] %s 第%d页:\n%s\n", i+1, r.Filename, r.Page, r.Content)
	}
	return out
}

// rerank 用 LLM 网关的重排序能力对融合结果重排。
func (e *Engine) rerank(ctx context.Context, descriptor *llm.ModelDescriptor, fused []FusedChunk, query string) ([]FusedChunk, error) {
	docs := make([]string, 0, len(fused))
	for _, f := range fused {
		docs = append(docs, f.Chunk.Content)
	}
	results, err := e.gateway.Rerank(ctx, descriptor, query, docs, len(docs))
	if err != nil {
		return nil, err
	}
	out := make([]FusedChunk, 0, len(results))
	for _, r := range results {
		if r.Index < 0 || r.Index >= len(fused) {
			continue
		}
		f := fused[r.Index]
		f.Score = r.Score // rerank 分数替换融合分数
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].Chunk.ID < out[j].Chunk.ID
		}
		return out[i].Score > out[j].Score
	})
	return out, nil
}

// ---- 指标辅助 ----

func recordStart() (t observabilityTimer) { return nowTimer() }

func observeStage(stage string, t observabilityTimer) {
	if m := observability.MetricsInstance(); m != nil {
		m.RAGSearchSeconds.WithLabelValues(stage).Observe(elapsedSeconds(t))
	}
}
