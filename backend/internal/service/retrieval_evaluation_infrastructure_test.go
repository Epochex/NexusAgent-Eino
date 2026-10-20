//go:build integration && infrastructure

package service

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"gorm.io/datatypes"
)

type evaluationDocument struct {
	ID, Content string
	Features    []string
}
type evaluationQuery struct {
	ID, Question, KB, Category string
	Relevant                   []string
}
type retrievalMetrics struct {
	Strategy                                                            string `json:"strategy"`
	Queries, Answerable, NoAnswer, Permission                           int
	RecallAt3, MRR                                                      float64
	NoAnswerFalseCitations, PermissionLeaks, CitationMismatches, Errors int
	P95Milliseconds                                                     float64
}

func TestFixed100QueryRetrievalEvaluation(t *testing.T) {
	corpusBytes, err := os.ReadFile("testdata/retrieval/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	queryBytes, err := os.ReadFile("testdata/retrieval/queries.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var corpus []evaluationDocument
	if err = json.Unmarshal(corpusBytes, &corpus); err != nil {
		t.Fatal(err)
	}
	var queries []evaluationQuery
	scanner := bufio.NewScanner(strings.NewReader(string(queryBytes)))
	for scanner.Scan() {
		var query evaluationQuery
		if err = json.Unmarshal(scanner.Bytes(), &query); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, query)
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(queries) < 100 || len(corpus) != 20 {
		t.Fatal("fixed evaluation corpus incomplete")
	}
	// These controlled features exercise ranking and isolation only. They are not
	// a trained semantic embedding model and are explicitly reported as such.
	embed := func(text string) []float32 {
		values := make([]float32, 32)
		text = strings.ToLower(text)
		sum := float64(0)
		for i, doc := range corpus {
			for _, feature := range doc.Features {
				if strings.Contains(text, strings.ToLower(feature)) {
					values[i]++
				}
			}
			sum += float64(values[i]) * float64(values[i])
		}
		if sum == 0 {
			values[31] = 1
			return values
		}
		norm := float32(math.Sqrt(sum))
		for i := range values {
			values[i] /= norm
		}
		return values
	}
	similarity := func(a, b []float32) float32 {
		var score float32
		for i := range a {
			score += a[i] * b[i]
		}
		return score
	}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/embeddings" {
			var request struct{ Input []string }
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid", 400)
				return
			}
			data := make([]map[string]any, 0, len(request.Input))
			for i, text := range request.Input {
				data = append(data, map[string]any{"index": i, "embedding": embed(text)})
			}
			json.NewEncoder(w).Encode(map[string]any{"data": data})
			return
		}
		if r.URL.Path == "/rerank" {
			var request struct {
				Query     string
				Documents []string
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid", 400)
				return
			}
			results := make([]map[string]any, 0, len(request.Documents))
			for i, text := range request.Documents {
				results = append(results, map[string]any{"index": i, "relevance_score": similarity(embed(request.Query), embed(text))})
			}
			json.NewEncoder(w).Encode(map[string]any{"results": results})
			return
		}
		http.NotFound(w, r)
	}))
	defer peer.Close()
	_, services, owner := workflowServiceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	store := vector.NewMilvusStore(cli)
	services.Cfg.Milvus.Dim = 32
	services.Cfg.RAG = config.RAG{ChunkSize: 512, ChunkOverlap: 64, DenseTopK: 25, SparseTopK: 25, FusedTopN: 15, FinalTopK: 3, RRFK: 60, ScoreThreshold: 0.35, DenseScoreThreshold: 0.25}
	services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 4, CircuitErrorRate: 1})
	services.Vector = store
	rag := engine.New(services.Gateway, store, services.Repos.KB, &services.Cfg.RAG, 32)
	rag.SetEmbeddingResolver(services.ResolveKBEmbedding)
	rag.SetRerankResolver(services.ResolveRerank)
	services.SetRAGEngine(rag)
	services.KB = NewKBService(services)
	provider := &model.ModelProvider{Code: "controlled-eval", Name: "controlled", BaseURL: peer.URL, Status: 1}
	if err = services.Repos.DB.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "controlled", ModelName: "controlled-v1", Type: model.ModelTypeEmbed, EmbeddingDimension: 32, Status: 1}
	if err = services.Repos.DB.Create(configured).Error; err != nil {
		t.Fatal(err)
	}
	ranker := &model.ModelConfig{ProviderID: provider.ID, Alias: "bge-reranker", ModelName: "controlled-ranker", Type: model.ModelTypeRerank, Status: 1}
	if err = services.Repos.DB.Create(ranker).Error; err != nil {
		t.Fatal(err)
	}
	primary := &model.KnowledgeBase{Name: "fixed-eval", EmbeddingConfigID: configured.ID}
	if err = services.KB.CreateKB(ctx, owner, primary); err != nil {
		t.Fatal(err)
	}
	foreignOwner := &model.User{Username: "foreign-eval", Email: "foreign-eval@test.invalid", PasswordHash: "test-only"}
	if err = services.Repos.DB.Create(foreignOwner).Error; err != nil {
		t.Fatal(err)
	}
	foreign := &model.KnowledgeBase{Name: "private-eval", EmbeddingConfigID: configured.ID}
	if err = services.KB.CreateKB(ctx, foreignOwner.ID, foreign); err != nil {
		t.Fatal(err)
	}
	for _, kb := range []*model.KnowledgeBase{primary, foreign} {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if err := store.DropCollection(cleanup, kb.MilvusCollection); err != nil {
				t.Error(err)
			}
		})
	}
	byDocument := map[int64]string{}
	byChunk := map[int64]string{}
	createReady := func(kb *model.KnowledgeBase, doc evaluationDocument) {
		t.Helper()
		digest := sha256.Sum256([]byte(doc.Content))
		source := &model.Document{KBID: kb.ID, Filename: doc.ID + ".txt", ObjectKey: "eval/" + doc.ID, ContentHash: hex.EncodeToString(digest[:]), FileSize: int64(len(doc.Content)), IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Status: model.DocStatusPending}
		if err := services.Repos.DB.Create(source).Error; err != nil {
			t.Fatal(err)
		}
		authority := ingestion.NewStore(services.Repos.DB)
		if _, err := authority.Enqueue(ctx, kb.UserID, source.ID); err != nil {
			t.Fatal(err)
		}
		job, err := authority.Claim(ctx, "retrieval-eval", time.Minute)
		if err != nil || job == nil {
			t.Fatal("fixture could not claim index", err)
		}
		tokens := llm.EstimateTokens(doc.Content)
		chunks := []model.DocumentChunk{{KBID: kb.ID, DocumentID: source.ID, IndexVersion: 1, EmbeddingVersion: kb.EmbeddingVersion, Seq: 0, Content: doc.Content, TokenCount: tokens, Meta: datatypes.JSON(`{"page":1}`)}}
		if err = services.Repos.KB.BatchCreate(ingestion.WithJob(ctx, job), chunks); err != nil {
			t.Fatal(err)
		}
		record := vector.Record{ID: chunks[0].ID, Vector: embed(doc.Content), Metadata: map[string]any{"user_id": kb.UserID, "kb_id": kb.ID, "doc_id": source.ID, "index_version": int64(1), "embedding_version": kb.EmbeddingVersion, "seq": int32(0), "page": int32(1)}}
		if err = store.Upsert(ctx, kb.MilvusCollection, []vector.Record{record}); err != nil {
			t.Fatal(err)
		}
		if err = authority.Complete(ctx, job, ingestion.Stats{ChunkCount: 1, TokenCount: tokens}); err != nil {
			t.Fatal(err)
		}
		if kb.ID == primary.ID {
			byDocument[source.ID] = doc.ID
			byChunk[chunks[0].ID] = doc.Content
		}
	}
	// Retrieval fixtures use explicit ready authority and real vectors. Source
	// upload/parser behavior is covered separately by the real ingestion test.
	for _, doc := range corpus {
		createReady(primary, doc)
	}
	createReady(foreign, corpus[0])
	strategies := []struct {
		name   string
		routes engine.RetrievalRoutes
	}{
		{"dense", engine.RetrievalRoutes{Dense: true}},
		{"keyword_tfidf", engine.RetrievalRoutes{Keyword: true}},
		{"hybrid", engine.RetrievalRoutes{Dense: true, Keyword: true}},
		{"hybrid_rerank", engine.RetrievalRoutes{Dense: true, Keyword: true, Rerank: true}},
	}
	var all []retrievalMetrics
	for _, strategy := range strategies {
		selected := rag.WithRetrievalRoutes(strategy.routes)
		metric := retrievalMetrics{Strategy: strategy.name}
		durations := make([]float64, 0, len(queries))
		for _, query := range queries {
			metric.Queries++
			started := time.Now()
			var result engine.RetrievalResult
			var err error
			if query.Category == "permission" {
				metric.Permission++
				result, err = services.KB.InspectRetrieval(ctx, owner, foreign.ID, query.Question)
				if err == nil || len(result.Refs) > 0 {
					metric.PermissionLeaks++
				}
				continue
			}
			result, err = selected.RetrieveDetailed(security.WithUserID(ctx, owner), primary, query.Question)
			durations = append(durations, float64(time.Since(started).Microseconds())/1000)
			if err != nil {
				metric.Errors++
				t.Logf("%s %s retrieval error: %v", strategy.name, query.ID, err)
				continue
			}
			for _, ref := range result.Refs {
				if byChunk[ref.ChunkID] != ref.Content || ref.Filename != byDocument[ref.DocumentID]+".txt" {
					metric.CitationMismatches++
				}
			}
			if query.Category == "no_answer" {
				metric.NoAnswer++
				if len(result.Refs) > 0 {
					metric.NoAnswerFalseCitations++
				}
				continue
			}
			metric.Answerable++
			rank := 0
			for i, ref := range result.Refs {
				for _, key := range query.Relevant {
					if byDocument[ref.DocumentID] == key {
						rank = i + 1
						break
					}
				}
				if rank > 0 {
					break
				}
			}
			if rank > 0 {
				metric.RecallAt3++
				metric.MRR += 1 / float64(rank)
			}
		}
		if metric.Answerable > 0 {
			metric.RecallAt3 /= float64(metric.Answerable)
			metric.MRR /= float64(metric.Answerable)
		}
		sort.Float64s(durations)
		if len(durations) > 0 {
			metric.P95Milliseconds = durations[min(len(durations)-1, int(float64(len(durations))*0.95))]
		}
		if metric.PermissionLeaks != 0 || metric.CitationMismatches != 0 || metric.Errors != 0 {
			t.Errorf("isolation/citation/protocol regression %+v", metric)
		}
		if strategy.name == "hybrid_rerank" && (metric.RecallAt3 < 0.85 || metric.NoAnswerFalseCitations != 0) {
			t.Errorf("controlled retrieval quality gate failed %+v", metric)
		}
		t.Logf("fixed evaluation %+v", metric)
		all = append(all, metric)
	}
	digest := sha256.Sum256(append(append([]byte(nil), corpusBytes...), queryBytes...))
	report := map[string]any{"dataset_sha256": hex.EncodeToString(digest[:]), "documents": len(corpus), "queries": len(queries), "real_model": false, "embedding": "controlled vocabulary features, 32 dimensions", "reranker": "cosine of the same controlled features", "evidence": "application ranking/isolation/citations only; no trained semantic model or answer quality claim", "results": all}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("NX_EVAL_REPORT_PATH"); path != "" {
		if err = os.WriteFile(path, append(raw, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
