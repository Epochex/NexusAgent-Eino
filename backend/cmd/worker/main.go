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
	queues := flag.String("queues", "execution,doc-etl", "execution and/or doc-etl; execution alone requires PostgreSQL only and does not enable knowledge-base nodes")
	concurrency := flag.Int("execution-concurrency", 4, "durable execution slots, 1..64")
	flag.Parse()
	selected := map[string]bool{}
	for _, queue := range strings.Split(*queues, ",") {
		queue = strings.TrimSpace(queue)
		if queue != "execution" && queue != "doc-etl" {
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
	executor := tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 30 * time.Second})
	var rdb *redis.Client
	var store vector.Store
	if selected["doc-etl"] {
		rdb, err = infra.NewRedis(ctx, &cfg.Redis)
		if err != nil {
			return fmt.Errorf("doc-etl redis: %w", err)
		}
		defer rdb.Close()
		mcli, e := infra.NewMilvus(ctx, &cfg.Milvus)
		if e != nil {
			return fmt.Errorf("doc-etl milvus: %w", e)
		}
		defer mcli.Close()
		store = vector.NewMilvusStore(mcli)
	}
	svcs, err := service.New(service.Deps{Cfg: cfg, Repos: repos, Gateway: gateway, Encryptor: encryptor, RDB: rdb, Vector: store, Executor: executor, Registry: reg})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	var etl *worker.Worker
	if selected["doc-etl"] {
		rag := engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
		rag.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)
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
		mc, e := infra.NewMinIO(ctx, &cfg.MinIO)
		if e != nil {
			return fmt.Errorf("doc-etl minio: %w", e)
		}
		etl = worker.New(rdb, repos, rag, mc, cfg.MinIO.Bucket, &cfg.Worker)
	}
	var durable *execution.Worker
	if selected["execution"] {
		capabilities := []string{}
		for _, code := range reg.Codes() {
			capabilities = append(capabilities, "tool:"+code)
		}
		if selected["doc-etl"] {
			capabilities = append(capabilities, "rag")
		}
		durable, err = execution.NewWorker(execution.NewStore(db), svcs.Workflow.Driver(), execution.WorkerConfig{Concurrency: *concurrency, Capabilities: capabilities})
		if err != nil {
			return err
		}
	}
	observability.LogInfo("nexus-agent worker started", "queues", *queues, "execution_slots", fmt.Sprint(*concurrency))
	results := make(chan error, 3)
	count := 0
	if durable != nil {
		count++
		go func() { results <- durable.Run(ctx) }()
		if rdb != nil {
			count++
			go func() { results <- execution.NewStore(db).RunNotifications(ctx, rdb, durable.Notify) }()
		}
	}
	if etl != nil {
		count++
		go func() { results <- etl.Run(ctx) }()
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
