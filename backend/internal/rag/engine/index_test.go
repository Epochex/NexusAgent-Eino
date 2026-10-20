package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
)

type indexRepo struct {
	chunks []model.DocumentChunk
	lookup []model.DocumentChunk
}

func (r *indexRepo) PublishedScope(_ context.Context, kb *model.KnowledgeBase) (vector.Scope, error) {
	scope := vector.Scope{UserID: kb.UserID, KBID: kb.ID, EmbeddingVersion: kb.EmbeddingVersion, DocumentVersions: map[int64]int64{}}
	for _, chunk := range r.lookup {
		scope.DocumentVersions[chunk.DocumentID] = max(chunk.IndexVersion, 1)
	}
	return scope, nil
}

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
	return r.lookup, nil
}

type indexVectors struct {
	hits    []vector.SearchHit
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
	return s.hits, nil
}
func (s *indexVectors) SearchScoped(ctx context.Context, collection string, query []float32, k int, _ vector.Scope) ([]vector.SearchHit, error) {
	return s.Search(ctx, collection, query, k)
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

func TestIndexPinsModelAndCarriesGeneration(t *testing.T) {
	engine, repository, store := indexFixture(t, 3)
	engine.cfg.BatchEmbed = 1
	initial := engine.embedMdl
	calls := 0
	engine.embedMdl = func() *llm.ModelDescriptor {
		calls++
		descriptor := *initial()
		if calls > 1 {
			descriptor.ModelName = "edited-v2"
		}
		return &descriptor
	}
	kb := &model.KnowledgeBase{ID: 1, UserID: 7, MilvusCollection: "test", EmbeddingVersion: "space-v1", EmbeddingDimension: 3}
	doc := &model.Document{ID: 1, KBID: 1, IndexVersion: 2, EmbeddingVersion: "space-v1"}
	loaded := &loader.LoadedDoc{Pages: []loader.PageContent{{Page: 1, Content: "one"}, {Page: 2, Content: "two"}}}
	if err := engine.IndexDocument(context.Background(), kb, doc, loaded); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("model re-resolved between batches: %d", calls)
	}
	if repository.chunks[0].IndexVersion != 2 || repository.chunks[0].EmbeddingVersion != "space-v1" || store.records[0].Metadata["index_version"] != int64(2) {
		t.Fatal("version identity not carried to chunks and vectors")
	}
}
func TestRetrieveRejectsUnpublishedAndForeignDenseMetadata(t *testing.T) {
	engine, repository, store := indexFixture(t, 3)
	engine.cfg.FinalTopK = 1
	engine.cfg.FusedTopN = 10
	engine.cfg.RRFK = 60
	engine.cfg.DenseTopK = 10
	store.hits = []vector.SearchHit{{Record: vector.Record{ID: 1, Metadata: map[string]any{"content": "unpublished or foreign data"}}, Score: 1}}
	kb := &model.KnowledgeBase{ID: 1, UserID: 7, EmbeddingVersion: strings.Repeat("a", 64), MilvusCollection: "test"}
	refs, err := engine.Retrieve(context.Background(), kb, "query")
	if err != nil || len(refs) != 0 {
		t.Fatalf("unpublished hit exposed: %+v %v", refs, err)
	}
	repository.lookup = []model.DocumentChunk{{ID: 1, KBID: 2, DocumentID: 20, Content: "foreign data"}}
	refs, err = engine.Retrieve(context.Background(), kb, "query")
	if err != nil || len(refs) != 0 {
		t.Fatalf("other KB exposed: %+v %v", refs, err)
	}
}
