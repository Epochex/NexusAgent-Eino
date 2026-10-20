package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"strings"
	"time"
	"unicode/utf8"
)

type RetrievalResult struct {
	Refs     []Ref    `json:"refs"`
	Mode     string   `json:"mode"`
	Degraded []string `json:"degraded"`
}

func (e *Engine) Retrieve(ctx context.Context, kb *model.KnowledgeBase, query string) ([]Ref, error) {
	result, err := e.RetrieveDetailed(ctx, kb, query)
	if len(result.Degraded) > 0 {
		observability.LogWarn("retrieval degraded", "kb_id", fmt.Sprint(kb.ID), "stages", strings.Join(result.Degraded, ","))
	}
	return result.Refs, err
}
func (e *Engine) RetrieveDetailed(ctx context.Context, kb *model.KnowledgeBase, query string) (RetrievalResult, error) {
	result := RetrievalResult{Refs: []Ref{}, Mode: "none", Degraded: []string{}}
	if kb == nil || e.gateway == nil || e.repo == nil || e.store == nil {
		return result, fmt.Errorf("retrieval dependencies incomplete")
	}
	if kb.ID < 1 || kb.UserID < 1 {
		return result, fmt.Errorf("authenticated knowledge base scope required")
	}
	if owner, ok := security.UserIDFromContext(ctx); ok && owner != kb.UserID {
		return result, fmt.Errorf("knowledge base owner mismatch")
	}
	query = strings.TrimSpace(query)
	if query == "" || len(query) > 4096 || !utf8.ValidString(query) {
		return result, fmt.Errorf("query must contain 1..4096 UTF-8 bytes")
	}
	routes := RetrievalRoutes{Dense: true, Keyword: true, Rerank: true}
	if e.routes != nil {
		routes = *e.routes
	}
	if !routes.Dense && !routes.Keyword {
		return result, fmt.Errorf("at least one retrieval route required")
	}
	budget := e.cfg.QueryTimeout
	if budget == 0 {
		budget = 8 * time.Second
	}
	if budget < time.Second || budget > 30*time.Second {
		return result, fmt.Errorf("retrieval timeout must be 1..30s")
	}
	ctx, cancel := context.WithTimeout(security.WithUserID(ctx, kb.UserID), budget)
	defer cancel()
	scopeCtx, stopScope := context.WithTimeout(ctx, 2*time.Second)
	scope, err := e.repo.PublishedScope(scopeCtx, kb)
	stopScope()
	if err != nil {
		return result, fmt.Errorf("published scope: %w", err)
	}
	if scope.UserID != kb.UserID || scope.KBID != kb.ID || scope.EmbeddingVersion != kb.EmbeddingVersion {
		return result, fmt.Errorf("published scope identity mismatch")
	}
	if len(scope.DocumentVersions) == 0 {
		return result, nil
	}
	scoped, ok := e.store.(vector.ScopedStore)
	if !ok {
		return result, fmt.Errorf("vector store does not enforce published scope")
	}
	top := func(value int32, fallback int) int {
		if value <= 0 {
			return fallback
		}
		return min(int(value), 256)
	}
	denseK, sparseK, fusedN, finalK := top(e.cfg.DenseTopK, 20), top(e.cfg.SparseTopK, 20), top(e.cfg.FusedTopN, 30), top(e.cfg.FinalTopK, 5)
	var dense []vector.SearchHit
	start := recordStart()
	var denseErr error
	if routes.Dense {
		denseCtx, stopDense := context.WithTimeout(ctx, 4*time.Second)
		descriptor, dimension, resolveErr := e.embeddingFor(denseCtx, kb)
		denseErr = resolveErr
		if denseErr == nil {
			var vectors [][]float32
			vectors, denseErr = e.gateway.Embed(denseCtx, descriptor, []string{query})
			if denseErr == nil && (len(vectors) != 1 || len(vectors[0]) != dimension) {
				denseErr = fmt.Errorf("query embedding has wrong dimension")
			}
			if denseErr == nil {
				dense, denseErr = scoped.SearchScoped(denseCtx, kb.MilvusCollection, vectors[0], denseK, scope)
				if denseErr == nil && e.cfg.DenseScoreThreshold > 0 {
					accepted := dense[:0]
					for _, hit := range dense {
						if hit.Score >= e.cfg.DenseScoreThreshold {
							accepted = append(accepted, hit)
						}
					}
					dense = accepted
				}
			}
		}
		stopDense()
		observeStage("dense", start)
	}
	if denseErr != nil {
		result.Degraded = append(result.Degraded, "dense_unavailable")
	}
	terms := extractTerms(query)
	if len(terms) > 64 {
		terms = terms[:64]
	}
	start = recordStart()
	keywordCtx, stopKeyword := context.WithTimeout(ctx, 2*time.Second)
	var keyword []ScoredChunk
	var keywordErr error
	if routes.Keyword {
		keyword, keywordErr = e.repo.KeywordSearch(keywordCtx, kb.ID, terms, sparseK)
	}
	stopKeyword()
	observeStage("keyword_tfidf", start)
	if keywordErr != nil {
		result.Degraded = append(result.Degraded, "keyword_unavailable")
		keyword = nil
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if (!routes.Dense || denseErr != nil) && (!routes.Keyword || keywordErr != nil) {
		return result, fmt.Errorf("all retrieval routes unavailable: %w", errors.Join(denseErr, keywordErr))
	}
	switch {
	case routes.Dense && denseErr == nil && routes.Keyword && keywordErr == nil:
		result.Mode = "hybrid"
	case routes.Dense && denseErr == nil:
		result.Mode = "dense_only"
	default:
		result.Mode = "keyword_only"
	}
	k := e.cfg.RRFK
	if k <= 0 {
		k = 60
	}
	fused := Fuse(dense, keyword, k, fusedN)
	ids := make([]int64, 0, len(fused))
	for _, candidate := range fused {
		ids = append(ids, candidate.Chunk.ID)
	}
	if len(ids) > 0 {
		rows, err := e.repo.ListByIDs(ctx, ids)
		if err != nil {
			return result, fmt.Errorf("published chunk lookup: %w", err)
		}
		byID := map[int64]model.DocumentChunk{}
		for _, row := range rows {
			if row.KBID == kb.ID && row.EmbeddingVersion == kb.EmbeddingVersion && row.Content != "" && scope.DocumentVersions[row.DocumentID] == row.IndexVersion {
				byID[row.ID] = row
			}
		}
		verified := fused[:0]
		for _, candidate := range fused {
			if row, ok := byID[candidate.Chunk.ID]; ok {
				candidate.Chunk = row
				candidate.Filename = row.SourceFilename
				verified = append(verified, candidate)
			}
		}
		fused = verified
	}
	var rerankModel *llm.ModelDescriptor
	if routes.Rerank && len(fused) > 0 {
		if e.resolveRerank != nil {
			resolveCtx, stop := context.WithTimeout(ctx, time.Second)
			var resolveErr error
			rerankModel, resolveErr = e.resolveRerank(resolveCtx)
			stop()
			if resolveErr != nil {
				result.Degraded = append(result.Degraded, "rerank_unavailable")
			}
		} else if e.rerankMdl != nil {
			rerankModel = e.rerankMdl()
		}
	}
	reranked := false
	if rerankModel != nil && len(fused) > 0 {
		start = recordStart()
		rerankCtx, stopRerank := context.WithTimeout(ctx, 2*time.Second)
		ordered, err := e.rerank(rerankCtx, rerankModel, fused, query)
		stopRerank()
		observeStage("rerank", start)
		if err != nil {
			result.Degraded = append(result.Degraded, "rerank_unavailable")
		} else {
			fused = ordered
			reranked = true
		}
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	for _, candidate := range fused {
		if reranked && e.cfg.ScoreThreshold > 0 && candidate.Score < e.cfg.ScoreThreshold {
			continue
		}
		if len(result.Refs) >= finalK {
			break
		}
		result.Refs = append(result.Refs, Ref{ChunkID: candidate.Chunk.ID, DocumentID: candidate.Chunk.DocumentID, Filename: candidate.Filename, Page: chunkPage(candidate.Chunk), Content: candidate.Chunk.Content, Score: candidate.Score})
	}
	return result, nil
}
