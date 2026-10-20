package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/pkg/callmeta"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
	"github.com/cloudwego/eino/compose"
)

// Definition is frozen when a run is accepted. Opaque model snapshots hold
// encrypted credentials and immutable settings interpreted by service wiring.
type Definition struct {
	RequiredCapabilities []string                   `json:"required_capabilities,omitempty"`
	Version              int                        `json:"workflow_version,omitempty"`
	DSL                  string                     `json:"dsl"`
	Subflows             map[int64]string           `json:"subflows,omitempty"`
	Models               map[string]json.RawMessage `json:"models,omitempty"`
	KBs                  map[int64]json.RawMessage  `json:"knowledge_bases,omitempty"`
	Tools                map[string]json.RawMessage `json:"tools,omitempty"`
}
type Driver struct {
	store       *Store
	deps        workflow.Deps
	DepsFactory func(context.Context, *Run, *Definition) (workflow.Deps, error)
	RetryDelay  time.Duration
}

func NewDriver(store *Store, deps workflow.Deps) *Driver {
	return &Driver{store: store, deps: deps, RetryDelay: time.Second}
}

func (d *Driver) Execute(ctx context.Context, run *Run) error {
	ctx, cancel := context.WithDeadline(security.WithUserID(ctx, run.OwnerID), run.DeadlineAt)
	defer cancel()
	if run.EngineVersion != EngineVersion || run.GraphSchema != GraphSchema {
		return d.fail(ctx, run, NeedsAttention, ErrVersionMismatch)
	}
	normalized, err := object(run.Definition, 1<<20)
	if err != nil || hash(normalized) != run.DefinitionHash {
		return d.fail(ctx, run, NeedsAttention, ErrVersionMismatch)
	}
	var definition Definition
	if err = json.Unmarshal(run.Definition, &definition); err != nil {
		return d.fail(ctx, run, Failed, err)
	}
	dsl, err := workflow.ParseDSL(definition.DSL)
	if err != nil {
		return d.fail(ctx, run, Failed, err)
	}
	deps := d.deps
	if d.DepsFactory != nil {
		deps, err = d.DepsFactory(ctx, run, &definition)
		if err != nil {
			return d.fail(ctx, run, NeedsAttention, err)
		}
	}
	deps.LookupWorkflow = func(_ context.Context, id int64) (*workflow.DSL, error) {
		text, ok := definition.Subflows[id]
		if !ok {
			return nil, fmt.Errorf("subworkflow %d not frozen into run", id)
		}
		return workflow.ParseDSL(text)
	}
	leaf := deps.ExecuteNode
	base := deps
	base.ExecuteNode = nil
	var retryable, uncertain atomic.Bool
	deps.ExecuteNode = func(ctx context.Context, path string, n *workflow.Node, state map[string]any) (any, error) {
		policy := "readonly"
		if n.Type == "tool" {
			policy = "manual"
			if base.Tools == nil {
				return nil, fmt.Errorf("tool executor unavailable")
			}
			bindings, _ := json.Marshal([]map[string]string{{"tool_code": n.ToolCode}})
			definitions, err := base.Tools.ListContext(ctx, string(bindings))
			if err != nil {
				return nil, err
			}
			if len(definitions) != 1 {
				return nil, fmt.Errorf("tool definition missing")
			}
			if definitions[0].ReplayPolicy != "" {
				policy = definitions[0].ReplayPolicy
			}
		}
		args, err := workflow.RenderArgs(n.Args, state)
		if err != nil {
			return nil, err
		}
		intent, err := json.Marshal(map[string]any{"node": n, "prompt": workflow.RenderTemplate(n.Prompt, state), "query": workflow.RenderTemplate(n.Query, state), "args": args})
		if err != nil {
			return nil, err
		}
		op, cached, err := d.store.BeginOperation(ctx, run, path, n.Type, hash(intent), policy)
		if err != nil {
			return nil, err
		}
		if cached {
			return decodeValue(op.Output)
		}
		ctx = callmeta.WithOperationID(ctx, op.ID)
		var value any
		var callErr error
		if leaf != nil {
			value, callErr = leaf(ctx, path, n, state)
		} else {
			value, callErr = workflow.ExecuteLeaf(ctx, base, n, state)
		}
		commitCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer finish()
		if err := d.store.CompleteOperation(commitCtx, run, op, value, callErr); err != nil {
			return nil, err
		}
		if callErr != nil {
			if policy == "manual" {
				uncertain.Store(true)
				return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, path)
			}
			if op.Attempt < 3 {
				retryable.Store(true)
			}
			return nil, callErr
		}
		return value, nil
	}
	graph, err := workflow.Compile(ctx, dsl, deps, d.store.Checkpoints(run))
	if err != nil {
		return d.fail(ctx, run, Failed, err)
	}
	current, err := d.store.Get(ctx, run.OwnerID, run.ID)
	if err != nil {
		return err
	}
	resume := map[string]any{}
	for _, a := range current.Approvals {
		if a.Decision == "approved" {
			resume[a.InterruptID] = workflow.ApprovalDecision{Approved: true, ArgsHash: a.ArgsHash}
		}
	}
	if len(resume) > 0 {
		ctx = compose.BatchResumeWithData(ctx, resume)
	}
	value, err := decodeValue(run.Input)
	if err != nil {
		return d.fail(ctx, run, Failed, err)
	}
	input, ok := value.(map[string]any)
	if !ok {
		return d.fail(ctx, run, Failed, ErrInvalidInput)
	}
	result, runErr := graph.Invoke(ctx, input, compose.WithCheckPointID(strconv.FormatInt(run.ID, 10)))
	finalCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer finish()
	if info, interrupted := compose.ExtractInterruptInfo(runErr); interrupted {
		var approvals []Approval
		for _, ic := range info.InterruptContexts {
			if !ic.IsRootCause {
				continue
			}
			proposal, ok := ic.Info.(*workflow.ApprovalInfo)
			if !ok {
				return d.fail(finalCtx, run, NeedsAttention, fmt.Errorf("unsupported interrupt payload %T", ic.Info))
			}
			approvals = append(approvals, Approval{InterruptID: ic.ID, Path: proposal.Path, ArgsHash: proposal.ArgsHash, Prompt: proposal.Prompt})
		}
		return d.store.Suspend(finalCtx, run, approvals)
	}
	if runErr != nil {
		if errors.Is(runErr, ErrLeaseLost) {
			return ErrLeaseLost
		}
		if errors.Is(runErr, ErrUnknownOperation) || uncertain.Load() || errors.Is(runErr, ErrVersionMismatch) || errors.Is(runErr, ErrConflict) {
			return d.fail(finalCtx, run, NeedsAttention, runErr)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if retryable.Load() && !errors.Is(runErr, ErrAttemptsExhausted) {
			return d.store.Retry(finalCtx, run, d.RetryDelay, runErr.Error())
		}
		return d.fail(finalCtx, run, Failed, runErr)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return d.fail(finalCtx, run, Failed, err)
	}
	return d.store.Finish(finalCtx, run, Succeeded, raw, "")
}
func (d *Driver) fail(ctx context.Context, run *Run, status string, err error) error {
	return d.store.Finish(ctx, run, status, nil, boundedError(err.Error()))
}
func decodeValue(raw []byte) (any, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var normalize func(any) any
	normalize = func(value any) any {
		switch v := value.(type) {
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return i
			}
			if f, err := v.Float64(); err == nil {
				return f
			}
			return v.String()
		case map[string]any:
			for key, child := range v {
				v[key] = normalize(child)
			}
		case []any:
			for i, child := range v {
				v[i] = normalize(child)
			}
		}
		return value
	}
	return normalize(value), nil
}
