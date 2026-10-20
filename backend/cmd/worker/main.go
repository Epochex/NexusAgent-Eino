// Worker owns durable Eino execution and optional document ETL consumers.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/memory/long_term"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"github.com/chengpeng-cp/nexus-agent/internal/observability/logger"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"github.com/chengpeng-cp/nexus-agent/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker exited: %v\n", err)
		os.Exit(1)
	}
}
func run() error {
	queues := flag.String("queues", "execution,doc-etl,memory", "execution/doc-etl/memory; execution alone needs PostgreSQL only; memory needs PostgreSQL+Milvus")
	concurrency := flag.Int("execution-concurrency", 4, "durable execution slots, 1..64")
	memoryConcurrency := flag.Int("memory-concurrency", 2, "memory slots, 1..8")
	flag.Parse()
	selected := map[string]bool{}
	for _, queue := range strings.Split(*queues, ",") {
		queue = strings.TrimSpace(queue)
		if queue != "execution" && queue != "doc-etl" && queue != "memory" {
			return fmt.Errorf("unsupported queue %q", queue)
		}
		selected[queue] = true
	}
	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	db, err := infra.NewPG(ctx, &cfg.PG)
	if err != nil {
		return fmt.Errorf("pg: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	encryptor, err := security.NewEncryptor(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("encryptor: %w", err)
	}
	gateway := llm.NewGateway(&cfg.LLM)
	repos := repo.New(db)
	reg := tool.NewRegistry()
	if selected["execution"] {
		tool.RegisterSimple(reg)
		tool.RegisterWeb(reg, tool.SearchConfig{SearxngURL: cfg.Search.SearxngURL})
		tool.RegisterFiles(reg, tool.FileOpsConfig{BaseDir: "./data/files", MaxSize: 10 << 20})
		tool.RegisterDBQuery(reg, db)
		if cfg.Sandbox.Enabled {
			sandbox, e := tool.NewSandbox(tool.SandboxConfig{Image: cfg.Sandbox.Image, Memory: cfg.Sandbox.Memory, CPUs: cfg.Sandbox.CPUs, Timeout: cfg.Sandbox.Timeout, PoolSize: cfg.Sandbox.PoolSize})
			if e != nil {
				return fmt.Errorf("configured sandbox unavailable: %w", e)
			}
			tool.RegisterCodeExec(reg, sandbox, db)
		}
	}
	executor := tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 30 * time.Second})
	var rdb *redis.Client
	var store vector.Store
	dataSelected := selected["doc-etl"] || selected["memory"]
	if dataSelected {
		// Notifications reconnect independently. PostgreSQL remains the work authority.
		rdb = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: -1, ContextTimeoutEnabled: true})
		defer rdb.Close()
		mcli, e := infra.NewMilvus(ctx, &cfg.Milvus)
		if e != nil {
			return fmt.Errorf("data worker milvus: %w", e)
		}
		defer mcli.Close()
		store = vector.NewMilvusStore(mcli)
	}
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repos, Gateway: gateway, Encryptor: encryptor, RDB: rdb, Vector: store, Executor: executor, Registry: reg})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	var etl *worker.Worker
	var rag *engine.Engine
	if dataSelected {
		rag = engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
		rag.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)
		rag.SetEmbeddingResolver(svcs.ResolveKBEmbedding)
		rag.SetRerankResolver(svcs.ResolveRerank)
		svcs.SetRAGEngine(rag)
		tool.RegisterKBSearch(reg, func(callCtx context.Context, kbID int64, query string, topK int) (string, error) {
			userID, ok := security.UserIDFromContext(callCtx)
			if !ok {
				return "", fmt.Errorf("authenticated user context required")
			}
			kb, e := repos.KB.GetKBForUser(callCtx, kbID, userID)
			if e != nil {
				return "", e
			}
			refs, e := rag.Retrieve(callCtx, kb, query)
			if e != nil {
				return "", e
			}
			var result strings.Builder
			for i, ref := range refs {
				if i >= topK {
					break
				}
				fmt.Fprintf(&result, "[%s p%d] %s\n", ref.Filename, ref.Page, ref.Content)
			}
			return result.String(), nil
		})
	}
	if selected["doc-etl"] {
		mc, e := infra.NewMinIO(ctx, &cfg.MinIO)
		if e != nil {
			return fmt.Errorf("doc-etl minio: %w", e)
		}
		etl = worker.New(rdb, repos, rag, mc, cfg.MinIO.Bucket, &cfg.Worker)
	}
	var memoryWorker *long_term.Worker
	if selected["memory"] {
		memoryWorker, err = long_term.NewWorker(svcs.ConfigureMemory(), long_term.WorkerConfig{Concurrency: *memoryConcurrency})
		if err != nil {
			return err
		}
	}
	var durable *execution.Worker
	if selected["execution"] {
		capabilities := []string{}
		for _, code := range reg.Codes() {
			capabilities = append(capabilities, "tool:"+code)
		}
		if dataSelected {
			capabilities = append(capabilities, "rag")
		}
		durable, err = execution.NewWorker(execution.NewStore(db), svcs.Workflow.Driver(), execution.WorkerConfig{Concurrency: *concurrency, Capabilities: capabilities})
		if err != nil {
			return err
		}
	}
	observability.LogInfo("nexus-agent worker started", "queues", *queues, "execution_slots", fmt.Sprint(*concurrency))
	results := make(chan error, 4)
	count := 0
	if durable != nil {
		count++
		go func() { results <- durable.Run(ctx) }()
	}
	if rdb != nil {
		count++
		go func() {
			results <- execution.NewStore(db).RunNotifications(ctx, rdb, func() {
				if durable != nil {
					durable.Notify()
				}
				if memoryWorker != nil {
					memoryWorker.Notify()
				}
				if etl != nil {
					etl.Notify()
				}
			})
		}()
	}
	if etl != nil {
		count++
		go func() { results <- etl.Run(ctx) }()
	}
	if memoryWorker != nil {
		count++
		go func() { results <- memoryWorker.Run(ctx) }()
	}
	var first error
	for range count {
		if e := <-results; e != nil && first == nil {
			first = e
		}
		cancel()
	}
	return first
}
