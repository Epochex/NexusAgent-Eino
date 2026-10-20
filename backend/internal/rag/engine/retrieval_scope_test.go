package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type scopedRepo struct {
	indexRepo
	scope        vector.Scope
	keywordError error
	keyword      []ScoredChunk
}

func (r *scopedRepo) KeywordSearch(context.Context, int64, []string, int) ([]ScoredChunk, error) {
	return r.keyword, r.keywordError
}

func (r *scopedRepo) PublishedScope(context.Context, *model.KnowledgeBase) (vector.Scope, error) {
	return r.scope, nil
}

type scopedVectors struct {
	indexVectors
	rawCalls, scopedCalls int
	rawID                 int64
	received              vector.Scope
	err                   error
}

func (s *scopedVectors) Search(context.Context, string, []float32, int) ([]vector.SearchHit, error) {
	s.rawCalls++
	id := s.rawID
	if id == 0 {
		id = 99
	}
	return []vector.SearchHit{{Record: vector.Record{ID: id}, Score: 1}}, nil
}
func (s *scopedVectors) SearchScoped(_ context.Context, _ string, _ []float32, _ int, scope vector.Scope) ([]vector.SearchHit, error) {
	s.scopedCalls++
	s.received = scope
	if s.err != nil {
		return nil, s.err
	}
	return []vector.SearchHit{{Record: vector.Record{ID: 1}, Score: 0.9}}, nil
}

func TestRetrievalDegradationIsExplicit(t *testing.T) {
	for _, route := range []string{"dense", "keyword", "both"} {
		t.Run(route, func(t *testing.T) {
			e, repository, vectors, kb := scopedFixture(t)
			repository.keyword = []ScoredChunk{{Chunk: repository.lookup[0], Score: 1}}
			if route == "dense" || route == "both" {
				vectors.err = errors.New("injected vector outage")
			}
			if route == "keyword" || route == "both" {
				repository.keywordError = errors.New("injected keyword outage")
			}
			result, err := e.RetrieveDetailed(context.Background(), kb, "query")
			if route == "both" {
				if err == nil {
					t.Fatal("all routes failed but returned success")
				}
				return
			}
			want := "dense_only"
			warning := "keyword_unavailable"
			if route == "dense" {
				want = "keyword_only"
				warning = "dense_unavailable"
			}
			if err != nil || len(result.Refs) != 1 || result.Mode != want || len(result.Degraded) != 1 || result.Degraded[0] != warning {
				t.Fatalf("degradation hidden %+v %v", result, err)
			}
		})
	}
}
func scopedFixture(t *testing.T) (*Engine, *scopedRepo, *scopedVectors, *model.KnowledgeBase) {
	e, _, _ := indexFixture(t, 3)
	space := strings.Repeat("a", 64)
	kb := &model.KnowledgeBase{ID: 1, UserID: 7, EmbeddingVersion: space, EmbeddingDimension: 3, MilvusCollection: "test"}
	repo := &scopedRepo{scope: vector.Scope{UserID: 7, KBID: 1, EmbeddingVersion: space, DocumentVersions: map[int64]int64{11: 2}}}
	repo.lookup = []model.DocumentChunk{{ID: 1, KBID: 1, DocumentID: 11, IndexVersion: 2, EmbeddingVersion: space, Content: "published answer", SourceFilename: "answer.txt"}}
	vectors := &scopedVectors{}
	e.repo = repo
	e.store = vectors
	e.cfg.DenseTopK = 1
	e.cfg.SparseTopK = 1
	e.cfg.FusedTopN = 4
	e.cfg.FinalTopK = 3
	e.cfg.RRFK = 60
	return e, repo, vectors, kb
}
func TestRetrievePrefiltersPublishedScopeBeforeTopK(t *testing.T) {
	e, _, vectors, kb := scopedFixture(t)
	refs, err := e.Retrieve(security.WithUserID(context.Background(), 7), kb, "query")
	if err != nil || len(refs) != 1 || refs[0].ChunkID != 1 {
		t.Fatalf("hidden candidate starved published result %+v %v", refs, err)
	}
	if vectors.rawCalls != 0 || vectors.scopedCalls != 1 || vectors.received.DocumentVersions[11] != 2 {
		t.Fatalf("authoritative prefilter missing %+v", vectors)
	}
	_, err = e.Retrieve(security.WithUserID(context.Background(), 8), kb, "query")
	if err == nil {
		t.Error("foreign owner accepted")
	}
}
func TestRerankThresholdAllowsNoAnswer(t *testing.T) {
	e, _, vectors, kb := scopedFixture(t)
	e.cfg.ScoreThreshold = 0.5
	vectors.rawID = 1
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[{"index":0,"relevance_score":0.1}]}`)
	}))
	defer peer.Close()
	e.rerankMdl = func() *llm.ModelDescriptor {
		return &llm.ModelDescriptor{ProviderCode: "rerank-peer", BaseURL: peer.URL, ModelName: "rerank"}
	}
	refs, err := e.Retrieve(security.WithUserID(context.Background(), 7), kb, "no answer")
	if err != nil || len(refs) != 0 {
		t.Fatalf("low relevance forced citations: %+v %v", refs, err)
	}
}
