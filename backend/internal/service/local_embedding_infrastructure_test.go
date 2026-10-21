//go:build integration && infrastructure && localembedding

package service

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/chengpeng-cp/nexus-agent/internal/rag/splitter"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"gorm.io/datatypes"
)

type semanticCaseResult struct {
	ID       string   `json:"id"`
	Category string   `json:"category"`
	Relevant []string `json:"relevant"`
	Ranked   []string `json:"ranked_document_ids"`
	Mode     string   `json:"mode"`
	Degraded []string `json:"degraded,omitempty"`
	Recall   float64  `json:"recall_at_3"`
	MRR      float64  `json:"reciprocal_rank"`
	MS       float64  `json:"total_ms"`
	Error    string   `json:"error,omitempty"`
}

func semanticRank(gold, ranked []string) (hit, recall, mrr float64) {
	want, found := map[string]bool{}, map[string]bool{}
	for _, id := range gold {
		want[id] = true
	}
	for i, id := range ranked {
		if i >= 3 {
			break
		}
		if want[id] {
			found[id] = true
			if mrr == 0 {
				mrr = 1 / float64(i+1)
			}
		}
	}
	if len(found) > 0 {
		hit = 1
	}
	if len(want) > 0 {
		recall = float64(len(found)) / float64(len(want))
	}
	return
}

func TestSemanticMetricCountsDistinctRelevantDocuments(t *testing.T) {
	h, r, m := semanticRank([]string{"a", "b"}, []string{"a", "a", "x"})
	if h != 1 || r != .5 || m != 1 {
		t.Fatal("duplicate chunks inflated document recall")
	}
	h, r, m = semanticRank([]string{"a", "b"}, []string{"x", "b", "a"})
	if h != 1 || r != 1 || m != .5 {
		t.Fatal("multi-relevant ranking incorrect")
	}
}

func TestLocalEmbeddingSemanticRetrieval(t *testing.T) {
	endpoint := os.Getenv("NX_LOCAL_EMBEDDING_URL")
	if endpoint == "" {
		t.Skip("explicit local embedding endpoint required")
	}
	key, err := os.ReadFile(os.Getenv("NX_LOCAL_EMBEDDING_TOKEN_FILE"))
	if err != nil {
		t.Fatal("private local model token unavailable")
	}
	out := os.Getenv("NX_LOCAL_EMBEDDING_REPORT_DIR")
	if out == "" {
		t.Fatal("report directory required")
	}
	for _, dataset := range []struct{ name, path string }{{"chinese100", "testdata/retrieval"}, {"scifact500", os.Getenv("NX_SCIFACT_SUBSET_DIR")}} {
		t.Run(dataset.name, func(t *testing.T) {
			if dataset.path == "" {
				t.Fatal("dataset path required")
			}
			corpusBytes, err := os.ReadFile(filepath.Join(dataset.path, "corpus.json"))
			if err != nil {
				t.Fatal(err)
			}
			queryBytes, err := os.ReadFile(filepath.Join(dataset.path, "queries.jsonl"))
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
				var q evaluationQuery
				if err = json.Unmarshal(scanner.Bytes(), &q); err != nil {
					t.Fatal(err)
				}
				queries = append(queries, q)
			}
			if err = scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if len(corpus) == 0 || len(corpus) > 1000 || len(queries) == 0 {
				t.Fatal("invalid bounded evaluation corpus")
			}
			_, services, owner := workflowServiceFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
			defer cancel()
			cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			store := vector.NewMilvusStore(cli)
			services.Vector = store
			services.Cfg.Outbound.PrivateOrigins = []string{strings.TrimSuffix(endpoint, "/v1")}
			services.Cfg.Milvus.Dim = 1024
			services.Cfg.RAG = config.RAG{ChunkSize: 512, ChunkOverlap: 64, BatchEmbed: 4, DenseTopK: 25, SparseTopK: 25, FusedTopN: 15, FinalTopK: 3, RRFK: 60, ScoreThreshold: .35, DenseScoreThreshold: .25}
			services.Gateway = llm.NewGateway(&config.LLM{MaxConnsPerProvider: 1, CircuitErrorRate: 1})
			rag := engine.New(services.Gateway, store, services.Repos.KB, &services.Cfg.RAG, 1024)
			rag.SetEmbeddingResolver(services.ResolveKBEmbedding)
			services.SetRAGEngine(rag)
			services.KB = NewKBService(services)
			encrypted, err := services.Encryptor.Encrypt(strings.TrimSpace(string(key)))
			if err != nil {
				t.Fatal(err)
			}
			// Isolated PG schemas reuse IDs; provider identity must also isolate
			// their Milvus embedding-space fingerprints at a fixed local endpoint.
			provider := &model.ModelProvider{Code: "local-qwen-" + strings.ToLower(rand.Text()[:10]), Name: "Local trained embedding", BaseURL: endpoint, APIKey: encrypted, Status: 1}
			if err = services.Repos.DB.Create(provider).Error; err != nil {
				t.Fatal(err)
			}
			configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "local-qwen", ModelName: "Qwen/Qwen3-Embedding-0.6B", Type: model.ModelTypeEmbed, EmbeddingDimension: 1024, Status: 1}
			if err = services.Repos.DB.Create(configured).Error; err != nil {
				t.Fatal(err)
			}
			kb := &model.KnowledgeBase{Name: "semantic-" + dataset.name, EmbeddingConfigID: configured.ID}
			if err = services.KB.CreateKB(ctx, owner, kb); err != nil {
				t.Fatal(err)
			}
			foreignOwner := &model.User{Username: "semantic-other", Email: "semantic-other@test.invalid", PasswordHash: "test-only"}
			if err = services.Repos.DB.Create(foreignOwner).Error; err != nil {
				t.Fatal(err)
			}
			foreign := &model.KnowledgeBase{Name: "semantic-private", EmbeddingConfigID: configured.ID}
			if err = services.KB.CreateKB(ctx, foreignOwner.ID, foreign); err != nil {
				t.Fatal(err)
			}
			byDoc := map[int64]string{}
			byChunk := map[int64]string{}
			indexStarted := time.Now()
			totalChunks := 0
			desc, _, err := services.ResolveKBEmbedding(ctx, kb)
			if err != nil {
				t.Fatal(err)
			}
			createReady := func(target *model.KnowledgeBase, doc evaluationDocument) {
				t.Helper()
				digest := sha256.Sum256([]byte(doc.Content))
				source := &model.Document{KBID: target.ID, Filename: doc.ID + ".txt", ObjectKey: "semantic/" + doc.ID, ContentHash: hex.EncodeToString(digest[:]), FileSize: int64(len(doc.Content)), IndexVersion: 1, EmbeddingVersion: target.EmbeddingVersion, Status: model.DocStatusPending}
				if err = services.Repos.DB.Create(source).Error; err != nil {
					t.Fatal(err)
				}
				authority := ingestion.NewStore(services.Repos.DB)
				if _, err = authority.Enqueue(ctx, target.UserID, source.ID); err != nil {
					t.Fatal(err)
				}
				job, err := authority.Claim(ctx, "semantic-index", time.Minute)
				if err != nil || job == nil {
					t.Fatal("index claim failed", err)
				}
				parts := splitter.New(512, 64).Split(doc.Content, 1)
				chunks := make([]model.DocumentChunk, len(parts))
				for i, p := range parts {
					chunks[i] = model.DocumentChunk{KBID: target.ID, DocumentID: source.ID, IndexVersion: 1, EmbeddingVersion: target.EmbeddingVersion, Seq: p.Seq, Content: p.Content, Meta: datatypes.JSON(`{"page":1}`)}
				}
				if err = services.Repos.KB.BatchCreate(ingestion.WithJob(ctx, job), chunks); err != nil {
					t.Fatal(err)
				}
				var records []vector.Record
				for start := 0; start < len(chunks); start += 4 {
					end := min(start+4, len(chunks))
					texts := []string{}
					for _, c := range chunks[start:end] {
						texts = append(texts, c.Content)
					}
					vectors, embedErr := services.Gateway.Embed(ctx, desc, texts)
					if embedErr != nil {
						t.Fatal("actual local embedding", embedErr)
					}
					for i, c := range chunks[start:end] {
						records = append(records, vector.Record{ID: c.ID, Vector: vectors[i], Metadata: map[string]any{"user_id": target.UserID, "kb_id": target.ID, "doc_id": source.ID, "index_version": int64(1), "embedding_version": target.EmbeddingVersion, "seq": int32(c.Seq), "page": int32(1)}})
						if target.ID == kb.ID {
							byChunk[c.ID] = c.Content
						}
					}
				}
				if err = store.Upsert(ctx, target.MilvusCollection, records); err != nil {
					t.Fatal(err)
				}
				if err = authority.Complete(ctx, job, ingestion.Stats{ChunkCount: len(chunks)}); err != nil {
					t.Fatal(err)
				}
				if target.ID == kb.ID {
					byDoc[source.ID] = doc.ID
					totalChunks += len(chunks)
				}
			}
			for i, doc := range corpus {
				createReady(kb, doc)
				if i%50 == 0 {
					t.Logf("%s indexed %d/%d", dataset.name, i+1, len(corpus))
				}
			}
			createReady(foreign, corpus[0])
			indexMS := float64(time.Since(indexStarted).Milliseconds())
			results := []map[string]any{}
			for _, strategy := range []struct {
				name   string
				routes engine.RetrievalRoutes
			}{{"dense", engine.RetrievalRoutes{Dense: true}}, {"keyword_tfidf", engine.RetrievalRoutes{Keyword: true}}, {"hybrid", engine.RetrievalRoutes{Dense: true, Keyword: true}}} {
				cases := []semanticCaseResult{}
				var answerable, unknown, permissions, leaks, errors, citationMismatch, unknownCandidates int
				var hitSum, recallSum, mrrSum float64
				var durations []float64
				for _, q := range queries {
					start := time.Now()
					row := semanticCaseResult{ID: q.ID, Category: q.Category, Relevant: q.Relevant}
					if q.Category == "permission" {
						permissions++
						r, e := services.KB.InspectRetrieval(ctx, owner, foreign.ID, q.Question)
						if e == nil || len(r.Refs) > 0 {
							leaks++
						}
						cases = append(cases, row)
						continue
					}
					r, e := rag.WithRetrievalRoutes(strategy.routes).RetrieveDetailed(security.WithUserID(ctx, owner), kb, q.Question)
					row.MS = float64(time.Since(start).Microseconds()) / 1000
					row.Mode = r.Mode
					row.Degraded = r.Degraded
					durations = append(durations, row.MS)
					if e != nil {
						errors++
						row.Error = e.Error()
					}
					for _, ref := range r.Refs {
						row.Ranked = append(row.Ranked, byDoc[ref.DocumentID])
						if byChunk[ref.ChunkID] != ref.Content || ref.Filename != byDoc[ref.DocumentID]+".txt" {
							citationMismatch++
						}
					}
					if q.Category == "no_answer" {
						unknown++
						if len(r.Refs) > 0 {
							unknownCandidates++
						}
					} else {
						answerable++
						h, rec, m := semanticRank(q.Relevant, row.Ranked)
						hitSum += h
						recallSum += rec
						mrrSum += m
						row.Recall = rec
						row.MRR = m
					}
					cases = append(cases, row)
				}
				sort.Float64s(durations)
				p95 := 0.0
				if len(durations) > 0 {
					p95 = durations[min(len(durations)-1, (95*len(durations)+99)/100-1)]
				}
				denom := float64(max(answerable, 1))
				results = append(results, map[string]any{"strategy": strategy.name, "answerable": answerable, "no_answer": unknown, "permission": permissions, "hit_at_3": hitSum / denom, "recall_at_3": recallSum / denom, "mrr_at_3": mrrSum / denom, "no_answer_returned_candidates": unknownCandidates, "permission_leaks": leaks, "citation_mismatches": citationMismatch, "errors": errors, "p95_ms": p95, "cases": cases})
				if leaks != 0 || citationMismatch != 0 || errors != 0 {
					t.Errorf("protocol/isolation/citation regression: %s leaks%d mismatches%d errors%d", strategy.name, leaks, citationMismatch, errors)
				}
				t.Logf("%s %s hit@3=%.4f recall@3=%.4f mrr@3=%.4f noanswer-candidates=%d/%d p95=%.2fms", dataset.name, strategy.name, hitSum/denom, recallSum/denom, mrrSum/denom, unknownCandidates, unknown, p95)
			}
			digest := sha256.Sum256(append(append([]byte(nil), corpusBytes...), queryBytes...))
			report := map[string]any{"dataset": dataset.name, "documents": len(corpus), "chunks": totalChunks, "queries": len(queries), "dataset_sha256": hex.EncodeToString(digest[:]), "embedding": "real local Qwen/Qwen3-Embedding-0.6B; 1024 dimensions; no query instruction", "model_revision": "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3", "indexing": "explicit PG publication jobs and real split/embed/Milvus vectors; upload/async-worker path tested separately", "ranking": "top3 chunks mapped to documents; duplicate documents cannot inflate recall", "dense_threshold": .25, "reranker": "none", "index_ms": indexMS, "results": results}
			raw, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(out, fmt.Sprintf("%s-results.json", dataset.name)), raw, 0644); err != nil {
				t.Fatal(err)
			}
		})
	}
}
