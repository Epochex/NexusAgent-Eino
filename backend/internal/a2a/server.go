package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/limiter"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

type AgentFunc func(context.Context, string) (string, error)
type ServerConfig struct {
	Root           context.Context
	Store          taskstore.Store
	Authenticate   func(context.Context, string) (int64, error)
	MaxConcurrency int
	RunTimeout     time.Duration
}
type ServerOption func(*ServerConfig)

func WithServerConfig(cfg ServerConfig) ServerOption { return func(c *ServerConfig) { *c = cfg } }

type Server struct {
	cfg         ServerConfig
	card        *sdk.AgentCard
	handler     AgentFunc
	rpc         http.Handler
	cardHandler http.Handler
	setupErr    error
	root        context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closing     bool
	tasks       sync.WaitGroup
	active      map[*a2asrv.ExecutorContext]struct{}
}

func NewServer(card *AgentCard, handler AgentFunc, options ...ServerOption) *Server {
	cfg := ServerConfig{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	s := &Server{cfg: cfg, handler: handler, active: make(map[*a2asrv.ExecutorContext]struct{})}
	if card == nil || handler == nil || cfg.Root == nil || cfg.Authenticate == nil || cfg.Store == nil {
		s.setupErr = fmt.Errorf("A2A server requires identity verification, task store, root context and Agent handler")
		return s
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = 8
	}
	if cfg.RunTimeout == 0 {
		cfg.RunTimeout = 2 * time.Minute
	}
	if cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 64 || cfg.RunTimeout < time.Second || cfg.RunTimeout > 2*time.Minute {
		s.setupErr = fmt.Errorf("invalid A2A execution limits")
		return s
	}
	s.cfg = cfg
	s.root, s.cancel = context.WithCancel(cfg.Root)
	raw, err := json.Marshal(card)
	if err != nil || json.Unmarshal(raw, &s.card) != nil {
		s.setupErr = fmt.Errorf("invalid A2A AgentCard")
		return s
	}
	s.card.Capabilities = sdk.AgentCapabilities{}
	s.card.DefaultInputModes = []string{"text/plain"}
	s.card.DefaultOutputModes = []string{"text/plain"}
	s.card.SecuritySchemes = sdk.NamedSecuritySchemes{"bearer": sdk.HTTPAuthSecurityScheme{Scheme: "Bearer", BearerFormat: "JWT"}}
	s.card.SecurityRequirements = sdk.SecurityRequirementsOptions{{"bearer": sdk.SecuritySchemeScopes{}}}
	handlerSDK := a2asrv.NewHandler(&serverExecutor{server: s},
		a2asrv.WithTaskStore(cfg.Store), a2asrv.WithCapabilityChecks(&s.card.Capabilities),
		a2asrv.WithConcurrencyConfig(limiter.ConcurrencyConfig{MaxExecutions: cfg.MaxConcurrency, GetMaxExecutions: func(string) int { return 2 }}),
		a2asrv.WithAgentInactivityTimeout(cfg.RunTimeout+time.Second),
		a2asrv.WithCallInterceptors(serverIdentity{store: cfg.Store}),
		a2asrv.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	s.rpc = a2asrv.NewJSONRPCHandler(handlerSDK, a2asrv.WithTransportPanicHandler(func(any) error { return sdk.ErrInternalError }))
	s.cardHandler = a2asrv.NewStaticAgentCardHandler(s.card)
	return s
}
func (s *Server) ValidationError() error { return s.setupErr }
func (s *Server) CardHandler(w http.ResponseWriter, r *http.Request) {
	if s.setupErr != nil {
		http.Error(w, "A2A service unavailable", 503)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	s.cardHandler.ServeHTTP(w, r)
}
func (s *Server) RPCHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	auth := r.Header.Values("Authorization")
	if len(auth) != 1 || len(auth[0]) > 4096 || !strings.HasPrefix(auth[0], "Bearer ") || s.cfg.Authenticate == nil {
		http.Error(w, "authentication required", 401)
		return
	}
	if s.setupErr != nil || s.root.Err() != nil {
		http.Error(w, "A2A service unavailable", 503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RunTimeout+5*time.Second)
	defer cancel()
	owner, err := s.cfg.Authenticate(ctx, strings.TrimPrefix(auth[0], "Bearer "))
	if err != nil || owner < 1 {
		http.Error(w, "invalid or revoked identity", 401)
		return
	}
	r = r.WithContext(security.WithUserID(ctx, owner))
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	s.rpc.ServeHTTP(w, r)
}
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc(a2asrv.WellKnownAgentCardPath, s.CardHandler)
	mux.HandleFunc("/a2a", s.RPCHandler)
}
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.tasks.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type serverIdentity struct {
	a2asrv.PassthroughCallInterceptor
	store taskstore.Store
}

func (identity serverIdentity) Before(ctx context.Context, call *a2asrv.CallContext, request *a2asrv.Request) (context.Context, any, error) {
	owner, ok := security.UserIDFromContext(ctx)
	if !ok || owner < 1 {
		return ctx, nil, sdk.ErrUnauthenticated
	}
	if call.Tenant() != "" {
		return ctx, nil, sdk.ErrUnsupportedOperation
	}
	call.User = a2asrv.NewAuthenticatedUser(strconv.FormatInt(owner, 10), nil)
	if params := call.ServiceParams(); params != nil {
		if versions, ok := params.Get("A2A-Version"); ok {
			for _, version := range versions {
				if version != "" && version != string(sdk.Version) {
					return ctx, nil, sdk.ErrVersionNotSupported
				}
			}
		}
	}
	if req, ok := request.Payload.(*sdk.SendMessageRequest); ok {
		if req == nil || req.Message == nil {
			return ctx, nil, sdk.ErrUnsupportedOperation
		}
		if req.Message.TaskID != "" {
			if _, err := identity.store.Get(ctx, req.Message.TaskID); err != nil {
				return ctx, nil, err
			}
			return ctx, nil, sdk.ErrUnsupportedOperation
		}
		if req.Config != nil && req.Config.PushConfig != nil {
			return ctx, nil, sdk.ErrUnsupportedOperation
		}
		if _, err := inboundText(req.Message); err != nil {
			return ctx, nil, err
		}
	}
	return limiter.AttachScope(ctx, strconv.FormatInt(owner, 10)), nil, nil
}
func TaskOwner(ctx context.Context) (int64, error) {
	call, ok := a2asrv.CallContextFrom(ctx)
	if !ok || call.User == nil || !call.User.Authenticated {
		return 0, sdk.ErrUnauthenticated
	}
	owner, err := strconv.ParseInt(call.User.Name, 10, 64)
	if err != nil || owner < 1 {
		return 0, sdk.ErrUnauthenticated
	}
	return owner, nil
}
func inboundText(message *sdk.Message) (string, error) {
	if message == nil || message.Role != sdk.MessageRoleUser || len(message.Parts) == 0 || len(message.Parts) > 16 {
		return "", sdk.ErrInvalidParams
	}
	var text strings.Builder
	for _, part := range message.Parts {
		if part == nil {
			return "", sdk.ErrInvalidParams
		}
		value, ok := part.Content.(sdk.Text)
		if !ok {
			return "", sdk.ErrUnsupportedContentType
		}
		if text.Len()+len(value) > 32768 {
			return "", sdk.ErrInvalidParams
		}
		text.WriteString(string(value))
	}
	if strings.TrimSpace(text.String()) == "" || !utf8.ValidString(text.String()) {
		return "", sdk.ErrInvalidParams
	}
	return text.String(), nil
}

type serverExecutor struct{ server *Server }

func (e *serverExecutor) Execute(parent context.Context, execution *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		s := e.server
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			yield(nil, sdk.ErrInternalError)
			return
		}
		s.tasks.Add(1)
		s.active[execution] = struct{}{}
		s.mu.Unlock()
		if execution.User == nil || !execution.User.Authenticated {
			yield(nil, sdk.ErrUnauthenticated)
			return
		}
		owner, err := strconv.ParseInt(execution.User.Name, 10, 64)
		if err != nil || owner < 1 {
			yield(nil, sdk.ErrUnauthenticated)
			return
		}
		text, err := inboundText(execution.Message)
		if err != nil {
			yield(nil, err)
			return
		}
		if !yield(sdk.NewSubmittedTask(execution, execution.Message), nil) {
			return
		}
		if !yield(sdk.NewStatusUpdateEvent(execution, sdk.TaskStateWorking, nil), nil) {
			return
		}
		ctx, cancel := context.WithTimeout(security.WithUserID(parent, owner), s.cfg.RunTimeout)
		defer cancel()
		stop := context.AfterFunc(s.root, cancel)
		defer stop()
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					current, err := s.cfg.Store.Get(ctx, execution.TaskID)
					if err != nil || current.Task.Status.State.Terminal() {
						cancel()
						return
					}
				}
			}
		}()
		defer func() { cancel(); <-monitorDone }()
		answer, err := s.handler(ctx, text)
		if err != nil || ctx.Err() != nil || strings.TrimSpace(answer) == "" || len(answer) > 65536 || !utf8.ValidString(answer) {
			yield(sdk.NewStatusUpdateEvent(execution, sdk.TaskStateFailed, sdk.NewMessageForTask(sdk.MessageRoleAgent, execution, sdk.NewTextPart("Agent execution failed or was interrupted; inspect task status before retrying."))), nil)
			return
		}
		if !yield(sdk.NewArtifactEvent(execution, sdk.NewTextPart(answer)), nil) {
			return
		}
		yield(sdk.NewStatusUpdateEvent(execution, sdk.TaskStateCompleted, nil), nil)
	}
}
func (e *serverExecutor) Cancel(_ context.Context, execution *a2asrv.ExecutorContext) iter.Seq2[sdk.Event, error] {
	return func(yield func(sdk.Event, error) bool) {
		yield(sdk.NewStatusUpdateEvent(execution, sdk.TaskStateCanceled, nil), nil)
	}
}

// The SDK calls Cleanup after its event consumer has persisted the final task
// snapshot. Waiting for the producer alone can close the DB before that commit.
func (e *serverExecutor) Cleanup(_ context.Context, execution *a2asrv.ExecutorContext, _ sdk.SendMessageResult, _ error) {
	s := e.server
	s.mu.Lock()
	_, tracked := s.active[execution]
	delete(s.active, execution)
	s.mu.Unlock()
	if tracked {
		s.tasks.Done()
	}
}
