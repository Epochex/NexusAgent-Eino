package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
)

type indexRepo struct{ chunks []model.DocumentChunk }

func (r *indexRepo) BatchCreate(_ context.Context, chunks []model.DocumentChunk) error {
	for i := range chunks {
		chunks[i].ID = int64(i + 1)
	}
	r.chunks = append([]model.DocumentChunk(nil), chunks...)
	return nil
}
func (r *indexRepo) ListByDoc(context.Context, int64) ([]model.DocumentChunk, error) {
	return r.chunks, nil
}
func (r *indexRepo) DeleteByDoc(context.Context, int64) error { return nil }
func (r *indexRepo) KeywordSearch(context.Context, int64, []string, int) ([]ScoredChunk, error) {
	return nil, nil
}
func (r *indexRepo) ListByIDs(context.Context, []int64) ([]model.DocumentChunk, error) {
	return nil, nil
}

type indexVectors struct {
	writes  int
	records []vector.Record
}

func (s *indexVectors) EnsureCollection(context.Context, string, int) error { return nil }
func (s *indexVectors) Upsert(_ context.Context, _ string, records []vector.Record) error {
	s.writes++
	s.records = append(s.records, records...)
	return nil
}
func (s *indexVectors) Search(context.Context, string, []float32, int) ([]vector.SearchHit, error) {
	return nil, nil
}
func (s *indexVectors) Delete(context.Context, string, []int64) error { return nil }
func (s *indexVectors) DropCollection(context.Context, string) error  { return nil }
func (s *indexVectors) Count(context.Context, string) (int64, error) {
	return int64(len(s.records)), nil
}
func indexFixture(t *testing.T, dimension int) (*Engine, *indexRepo, *indexVectors) {
	t.Helper()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		data := []any{}
		for i := range request.Input {
			data = append(data, map[string]any{"index": i, "embedding": make([]float32, dimension)})
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(peer.Close)
	repository, store := &indexRepo{}, &indexVectors{}
	engine := New(llm.NewGateway(&config.LLM{MaxConnsPerProvider: 2}), store, repository, &config.RAG{ChunkSize: 64, ChunkOverlap: 8, BatchEmbed: 64}, 3)
	engine.SetModels(func() *llm.ModelDescriptor {
		return &llm.ModelDescriptor{ProviderCode: "index-peer", BaseURL: peer.URL, ModelName: "index-peer"}
	}, func() *llm.ModelDescriptor { return nil })
	return engine, repository, store
}
func TestIndexGlobalSequenceAndStoredTokens(t *testing.T) {
	engine, repository, store := indexFixture(t, 3)
	doc := &model.Document{ID: 1, KBID: 1, Filename: "two-pages.pdf"}
	loaded := &loader.LoadedDoc{Pages: []loader.PageContent{{Page: 1, Content: "first page"}, {Page: 2, Content: "second page"}}}
	if err := engine.IndexDocument(context.Background(), &model.KnowledgeBase{ID: 1, MilvusCollection: "test"}, doc, loaded); err != nil {
		t.Fatal(err)
	}
	if len(repository.chunks) != 2 || repository.chunks[0].Seq != 0 || repository.chunks[1].Seq != 1 {
		t.Fatalf("page-local chunk sequence reused: %+v", repository.chunks)
	}
	if doc.TokenCount == 0 || repository.chunks[0].TokenCount == 0 || len(store.records) != 2 {
		t.Fatalf("token accounting not persisted: %+v %+v", doc, repository.chunks)
	}
}
func TestIndexRejectsWrongVectorDimension(t *testing.T) {
	engine, _, store := indexFixture(t, 2)
	err := engine.IndexDocument(context.Background(), &model.KnowledgeBase{ID: 1, MilvusCollection: "test"}, &model.Document{ID: 1, KBID: 1}, &loader.LoadedDoc{Pages: []loader.PageContent{{Page: 1, Content: "input"}}})
	if err == nil || store.writes != 0 {
		t.Fatalf("invalid dimensions published: writes=%d err=%v", store.writes, err)
	}
}

func TestIndexRejectsUnsafeChunkBudget(t *testing.T) {
	engine, _, store := indexFixture(t, 3)
	err := engine.IndexDocument(context.Background(), &model.KnowledgeBase{ID: 1, ChunkSize: 1, MilvusCollection: "test"}, &model.Document{ID: 1, KBID: 1}, &loader.LoadedDoc{Pages: []loader.PageContent{{Page: 1, Content: "input"}}})
	if err == nil || store.writes != 0 {
		t.Fatalf("unsafe tiny chunks accepted: %v", err)
	}
}
