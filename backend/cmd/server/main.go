// NexusAgent API 服务入口：全项目依赖装配（main 是唯一知道所有具体实现的地方）。
//
// 装配顺序（依赖方向决定，教学 Ep 02 的"总装车间"）：
//
//	配置 → 日志 → 追踪 → 基础设施(PG/Redis/Milvus/MinIO)
//	  → 安全(JWT/加密) → 网关/向量库 → 工具系统 → service
//	  → RAG 引擎 → 工作流引擎 → 长期记忆 → kb_search 工具
//	  → Hertz 路由 → A2A 服务端 → 优雅关停
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/minio/minio-go/v7"

	"github.com/chengpeng-cp/nexus-agent/internal/api"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
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
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "server exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// ===== ① 配置与日志 =====
	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	logger.Init(cfg.Log.Level, cfg.Log.Format)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ===== ② 可观测性 =====
	if cfg.Observability.OtelEnabled && cfg.Observability.OtelEndpoint != "" {
		shutdown, err := observability.InitTracer(ctx, cfg.Observability.OtelEndpoint)
		if err != nil {
			observability.LogWarn("otel init failed, tracing disabled", "err", err.Error())
		} else {
			defer shutdown(context.Background())
		}
	}
	observability.InitMetrics()

	// ===== ③ 基础设施 =====
	db, err := infra.NewPG(ctx, &cfg.PG)
	if err != nil {
		return fmt.Errorf("pg: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	rdb, err := infra.NewRedis(ctx, &cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rdb.Close()
	mcli, err := infra.NewMilvus(ctx, &cfg.Milvus)
	if err != nil {
		return fmt.Errorf("milvus: %w", err)
	}
	defer mcli.Close()
	mio, err := infra.NewMinIO(ctx, &cfg.MinIO)
	if err != nil {
		return fmt.Errorf("minio: %w", err)
	}
	// 对象桶不存在则创建（已存在时忽略）
	if err := mio.MakeBucket(ctx, cfg.MinIO.Bucket, minio.MakeBucketOptions{}); err != nil {
		if exists, _ := mio.BucketExists(ctx, cfg.MinIO.Bucket); !exists {
			return fmt.Errorf("minio bucket: %w", err)
		}
	}

	// ===== ④ 安全组件 =====
	encryptor, err := security.NewEncryptor(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("encryptor: %w", err)
	}
	jwtMgr := security.NewJWTManager(cfg.JWT.Secret, cfg.JWT.AccessTTL, cfg.JWT.RefreshTTL)

	// ===== ⑤ LLM 网关与向量库 =====
	gateway := llm.NewGateway(&cfg.LLM)
	store := vector.NewMilvusStore(mcli)

	// ===== ⑥ 数据访问与工具系统 =====
	repos := repo.New(db)
	reg := tool.NewRegistry()
	tool.RegisterSimple(reg)
	closeWeb, err := tool.RegisterWeb(reg, tool.SearchConfig{SearxngURL: cfg.Search.SearxngURL, DomainWhitelist: cfg.Search.HTTPDomains, HTTPPrivateOrigins: cfg.Search.HTTPPrivateOrigins})
	if err != nil {
		return fmt.Errorf("web tools: %w", err)
	}
	defer closeWeb()
	tool.RegisterFiles(reg, tool.FileOpsConfig{BaseDir: "./data/files", MaxSize: 10 << 20})
	tool.RegisterDBQuery(reg, db)
	if cfg.Sandbox.Enabled {
		sandbox, err := tool.NewSandbox(tool.SandboxConfig{
			Image: cfg.Sandbox.Image, Memory: cfg.Sandbox.Memory,
			CPUs: cfg.Sandbox.CPUs, Timeout: cfg.Sandbox.Timeout, PoolSize: cfg.Sandbox.PoolSize,
			RequireRemote: cfg.Env == "prod" || cfg.Env == "production",
		})
		if err != nil {
			return fmt.Errorf("configured sandbox unavailable: %w", err)
		}
		defer sandbox.Close()
		tool.RegisterCodeExec(reg, sandbox, db)
	}
	executor := tool.NewExecutor(repos.Tool, reg, tool.ExecutorConfig{DefaultTimeout: 30 * time.Second})

	// ===== ⑦ service 层 =====
	svcs, err := service.New(service.Deps{
		Root: ctx,
		Cfg:  cfg, Repos: repos, Gateway: gateway,
		Encryptor: encryptor, JWT: jwtMgr, RDB: rdb,
		Vector: store, Executor: executor, Registry: reg,
		Streams: worker.NewPublisher(rdb),
	})
	if err != nil {
		return fmt.Errorf("services: %w", err)
	}
	defer svcs.PDFParser.Close()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = svcs.Chat.Close(cleanup)
	}()
	if err = svcs.MCP.RestoreRegistry(ctx); err != nil {
		return fmt.Errorf("MCP registry restore: %w", err)
	}

	// ===== ⑧ RAG 引擎（注入实时解析函数，改配置无需重启） =====
	ragEngine := engine.New(gateway, store, repos.KB, &cfg.RAG, cfg.Milvus.Dim)
	ragEngine.SetModels(svcs.DefaultEmbedding, svcs.DefaultRerank)
	ragEngine.SetEmbeddingResolver(svcs.ResolveKBEmbedding)
	ragEngine.SetRerankResolver(svcs.ResolveRerank)
	svcs.SetRAGEngine(ragEngine)

	// Workflow handlers persist submissions; the separate worker owns Eino execution.

	// ===== ⑩ 长期记忆（抽取用低成本 chat 模型，实时解析） =====
	svcs.ConfigureMemory()

	// ===== ⑪ 对象存储与 kb_search 工具（依赖 RAG 引擎，最后注册） =====
	svcs.KB.SetMinIO(mio, cfg.MinIO.Bucket)
	tool.RegisterKBSearch(reg, kbSearchFunc(svcs, ragEngine))

	// ===== ⑫ Hertz 服务 =====
	h := server.New(
		// Keep cancellation/deadline semantics identical to the real TCP tests.
		// Evaluate alternative transports with the P4 workload, not by assumption.
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(":"+cfg.HTTP.Port),
		server.WithReadTimeout(30*time.Second),
		server.WithMaxRequestBodySize(service.MaxDocSize+(1<<20)),
		server.WithWriteTimeout(0), // SSE body refreshes a 2s deadline per network chunk
		server.WithIdleTimeout(120*time.Second),
		server.WithSenseClientDisconnection(runtime.GOOS != "windows"),
	)
	root := h.Group("/")
	// Tracing 放最外层：根 span 覆盖整条中间件链与 handler
	root.Use(api.Tracing(), api.Recovery(), api.CORS(), api.AccessLog())
	root.GET("/healthz", api.Healthz())
	root.GET("/metrics", api.Metrics())
	api.Register(root, api.New(svcs), jwtMgr)

	// Private diagnostics is opt-in and owns a bounded shutdown lifecycle.
	diagnostics, err := observability.StartDiagnostics(ctx, cfg.Observability.DiagnosticsAddr, sqlDB)
	if err != nil {
		return fmt.Errorf("diagnostics: %w", err)
	}
	var diagnosticsFailure <-chan error
	if diagnostics != nil {
		diagnosticsFailure = diagnostics.Errors()
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_ = diagnostics.Close(cleanup)
		}()
	}

	// ===== ⑬ A2A 对外端点（独立 net/http 端口，回调标准对话链路） =====
	var closeA2A func(context.Context) error
	var a2aFailure <-chan error
	if cfg.A2A.Enabled {
		closeA2A, a2aFailure, err = startA2AServer(ctx, cfg, svcs)
		if err != nil {
			return err
		}
	}

	observability.LogInfo("nexus-agent server started", "port", cfg.HTTP.Port, "env", cfg.Env)

	// ===== ⑭ 优雅关停 =====
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	// Run binds the listener without installing a second signal handler. The
	// main goroutine owns shutdown of both HTTP servers and the Agent root.
	httpFailure := make(chan error, 1)
	go func() { httpFailure <- h.Run() }()
	var serveErr error
	select {
	case <-quit:
	case serveErr = <-a2aFailure:
	case serveErr = <-httpFailure:
	case serveErr = <-diagnosticsFailure:
	}
	observability.LogInfo("shutting down...")

	shutdownCtx, c := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer c()
	cancel() // 通知后台 goroutine
	shutdownErr := errors.Join(svcs.Chat.Close(shutdownCtx), h.Shutdown(shutdownCtx))
	if closeA2A != nil {
		shutdownErr = errors.Join(shutdownErr, closeA2A(shutdownCtx))
	}
	return errors.Join(serveErr, shutdownErr)
}

// startA2AServer 启动 A2A 对外端点。
func startA2AServer(root context.Context, cfg *config.Config, svcs *service.Services) (func(context.Context) error, <-chan error, error) {
	address := cfg.A2A.BindAddr
	if address == "" {
		address = net.JoinHostPort("127.0.0.1", cfg.A2A.Port)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("A2A bind: %w", err)
	}
	publicURL := cfg.A2A.PublicURL
	if publicURL == "" {
		if cfg.Env != "dev" && cfg.Env != "test" {
			listener.Close()
			return nil, nil, fmt.Errorf("A2A public_url is required outside development")
		}
		publicURL = "http://" + listener.Addr().String()
	}
	adapter, err := svcs.A2A.Inbound(root, publicURL)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	mux := http.NewServeMux()
	adapter.RegisterRoutes(mux)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 135 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	failures := make(chan error, 1)
	go func() {
		observability.LogInfo("a2a server listening", "address", listener.Addr().String())
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		failures <- fmt.Errorf("A2A server exited: %w", err)
	}()
	return func(ctx context.Context) error { return errors.Join(server.Shutdown(ctx), adapter.Close(ctx)) }, failures, nil
}

// kbSearchFunc 组装 kb_search 工具的检索回调。
func kbSearchFunc(svcs *service.Services, eng *engine.Engine) tool.KBSearchFunc {
	return func(ctx context.Context, kbID int64, query string, topK int) (string, error) {
		userID, ok := security.UserIDFromContext(ctx)
		if !ok {
			return "", fmt.Errorf("authenticated user context is required")
		}
		kb, err := svcs.Repos.KB.GetKBForUser(ctx, kbID, userID)
		if err != nil {
			return "", err
		}
		refs, err := eng.Retrieve(ctx, kb, query)
		if err != nil {
			return "", err
		}
		var out string
		for _, r := range refs {
			out += fmt.Sprintf("[%s p%d] %s\n", r.Filename, r.Page, r.Content)
		}
		return out, nil
	}
}
