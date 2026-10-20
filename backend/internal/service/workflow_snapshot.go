package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"sort"
	"strconv"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/tool"
	"github.com/chengpeng-cp/nexus-agent/internal/workflow"
)

type frozenModel struct {
	ConfigID     int64               `json:"config_id"`
	ProviderID   int64               `json:"provider_id"`
	Descriptor   llm.ModelDescriptor `json:"descriptor"`
	EncryptedKey string              `json:"encrypted_key"`
}

const frozenKBRerankKey = "\x00kb-rerank" // NUL cannot appear in persisted model aliases.
type frozenTool struct {
	SourceIdentity string         `json:"source_identity,omitempty"`
	BindingVersion int64          `json:"binding_version"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Parameters     map[string]any `json:"parameters"`
	Dangerous      bool           `json:"dangerous"`
	ReplayPolicy   string         `json:"replay_policy"`
}

func (s *WorkflowService) freeze(ctx context.Context, ownerID int64, w *model.Workflow) (*execution.Definition, error) {
	definition := &execution.Definition{Version: w.Version, DSL: w.DSL, Subflows: map[int64]string{}, Models: map[string]json.RawMessage{}, KBs: map[int64]json.RawMessage{}, Tools: map[string]json.RawMessage{}}
	visiting := map[int64]bool{w.ID: true}
	count := 0
	capabilities := map[string]bool{}
	rerankCaptured := false
	var walk func(string, int) error
	walk = func(text string, depth int) error {
		if depth > 8 {
			return errcode.ErrWorkflowDSL.WithMsg("子图嵌套超过 8 层")
		}
		dsl, err := workflow.ParseDSL(text)
		if err != nil {
			return err
		}
		count += len(dsl.Nodes)
		if count > 128 {
			return errcode.ErrWorkflowDSL.WithMsg("展开的节点数超过 128")
		}
		for _, node := range dsl.Nodes {
			switch node.Type {
			case "llm":
				if _, exists := definition.Models[node.Model]; !exists {
					alias := node.Model
					if alias == "" {
						alias = s.svcs.Cfg.LLM.DefaultChat
					}
					configured, err := s.svcs.Repos.LLM.DefaultModel(ctx, alias, model.ModelTypeChat)
					if err != nil {
						return errcode.ErrProviderNotFound.WithCause(err)
					}
					if node.Model != "" && configured.Alias != node.Model {
						return errcode.ErrProviderNotFound.WithMsg("模型别名 %q 不存在", node.Model)
					}
					snapshot, err := s.captureModel(configured)
					if err != nil {
						return err
					}
					definition.Models[node.Model] = snapshot
				}
			case "kb":
				capabilities["rag"] = true
				if !rerankCaptured {
					rerankCaptured = true
					configured, err := s.svcs.Repos.LLM.DefaultModel(ctx, "bge-reranker", model.ModelTypeRerank)
					if err == nil {
						raw, err := s.captureModel(configured)
						if err != nil {
							return err
						}
						definition.Models[frozenKBRerankKey] = raw
					} else if !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}
				}
				kb, err := s.svcs.Repos.KB.GetKBForUser(ctx, node.KbID, ownerID)
				if err != nil {
					return errcode.ErrKBNotFound.WithCause(err)
				}
				raw, err := json.Marshal(kb)
				if err != nil {
					return err
				}
				definition.KBs[kb.ID] = raw
				snapshot, err := s.svcs.captureKBModel(ctx, kb)
				if err != nil {
					return err
				}
				snapshotRaw, err := json.Marshal(snapshot)
				if err != nil {
					return err
				}
				definition.Models["kb-embedding:"+strconv.FormatInt(kb.ID, 10)] = snapshotRaw
			case "tool":
				if node.ToolCode == "kb_search" {
					capabilities["rag"] = true
				}
				if s.svcs.Executor == nil {
					return errcode.ErrToolNotFound
				}
				binding, _ := json.Marshal([]map[string]string{{"tool_code": node.ToolCode}})
				defs, err := s.svcs.Executor.ListContext(ctx, string(binding))
				if err != nil {
					return err
				}
				if len(defs) != 1 {
					return errcode.ErrToolNotFound
				}
				def := defs[0]
				if def.Capability != "" {
					capabilities[def.Capability] = true
				} else {
					capabilities["tool:"+node.ToolCode] = true
				}
				policy := def.ReplayPolicy
				if policy == "" {
					policy = "manual"
				}
				raw, err := json.Marshal(frozenTool{Name: def.Name, Description: def.Description, Parameters: def.Parameters, Dangerous: def.Dangerous, ReplayPolicy: policy, BindingVersion: def.BindingVersion, SourceIdentity: def.SourceIdentity})
				if err != nil {
					return err
				}
				definition.Tools[def.Name] = raw
			case "subflow":
				if visiting[node.Workflow] {
					return errcode.ErrWorkflowDSL.WithMsg("子工作流递归引用")
				}
				if _, frozen := definition.Subflows[node.Workflow]; frozen {
					continue
				}
				child, err := s.Get(ctx, ownerID, node.Workflow)
				if err != nil {
					return err
				}
				if child.Status != 1 {
					return errcode.ErrWorkflowDSL.WithMsg("子工作流已停用")
				}
				visiting[node.Workflow] = true
				definition.Subflows[node.Workflow] = child.DSL
				if err = walk(child.DSL, depth+1); err != nil {
					return err
				}
				delete(visiting, node.Workflow)
			}
		}
		return nil
	}
	if err := walk(w.DSL, 0); err != nil {
		return nil, err
	}
	for capability := range capabilities {
		definition.RequiredCapabilities = append(definition.RequiredCapabilities, capability)
	}
	sort.Strings(definition.RequiredCapabilities)
	return definition, nil
}
func (s *WorkflowService) captureModel(configured *model.ModelConfig) (json.RawMessage, error) {
	return s.svcs.captureModel(configured)
}
func (s *Services) captureModel(configured *model.ModelConfig) (json.RawMessage, error) {
	if configured == nil || configured.Provider == nil || configured.Status != 1 || configured.Provider.Status != 1 {
		return nil, errcode.ErrProviderNotFound.WithMsg("模型或供应商未启用")
	}
	descriptor, err := s.descriptorOf(configured, 0, 0)
	if err != nil {
		return nil, err
	}
	descriptor.APIKey = ""
	return json.Marshal(frozenModel{ConfigID: configured.ID, ProviderID: configured.ProviderID, Descriptor: *descriptor, EncryptedKey: configured.Provider.APIKey})
}
func frozenLookup(definition *execution.Definition) func(context.Context, int64) (*workflow.DSL, error) {
	return func(_ context.Context, id int64) (*workflow.DSL, error) {
		text, ok := definition.Subflows[id]
		if !ok {
			return nil, errcode.ErrWorkflowDSL.WithMsg("子工作流未冻结")
		}
		return workflow.ParseDSL(text)
	}
}

func (s *WorkflowService) frozenDeps(ctx context.Context, run *execution.Run, definition *execution.Definition) (workflow.Deps, error) {
	models := map[string]frozenModel{}
	for alias, raw := range definition.Models {
		var snapshot frozenModel
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return workflow.Deps{}, err
		}
		models[alias] = snapshot
	}
	frozenTools := &frozenExecutor{base: s.svcs.Executor, snapshots: map[string]frozenTool{}}
	for code, raw := range definition.Tools {
		var snapshot frozenTool
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return workflow.Deps{}, err
		}
		frozenTools.snapshots[code] = snapshot
	}
	deps := workflow.Deps{Gateway: s.svcs.Gateway, Tools: frozenTools, RAG: s.svcs.ragEngine, LookupWorkflow: frozenLookup(definition)}
	deps.ResolveModel = func(ctx context.Context, alias string) (*llm.ModelDescriptor, *llm.ModelDescriptor, error) {
		snapshot, ok := models[alias]
		if !ok {
			return nil, nil, errcode.ErrProviderNotFound.WithMsg("模型未冻结")
		}
		current, err := s.svcs.Repos.LLM.GetModel(ctx, snapshot.ConfigID)
		if err != nil || current.Status != 1 || current.Provider == nil || current.Provider.Status != 1 || current.ProviderID != snapshot.ProviderID {
			return nil, nil, errcode.ErrProviderNotFound.WithMsg("模型配置已撤销或不可访问")
		}
		key, err := s.svcs.Encryptor.Decrypt(snapshot.EncryptedKey)
		if err != nil {
			return nil, nil, err
		}
		descriptor := snapshot.Descriptor
		descriptor.APIKey = key
		return &descriptor, nil, nil
	}
	if deps.RAG != nil {
		resolve := deps.ResolveModel
		deps.RAG = deps.RAG.WithEmbeddingResolver(func(ctx context.Context, kb *model.KnowledgeBase) (*llm.ModelDescriptor, int, error) {
			descriptor, _, err := resolve(ctx, "kb-embedding:"+strconv.FormatInt(kb.ID, 10))
			if err != nil {
				return nil, 0, err
			}
			return descriptor, kb.EmbeddingDimension, nil
		})
		deps.RAG = deps.RAG.WithRerankResolver(func(ctx context.Context) (*llm.ModelDescriptor, error) {
			if _, exists := models[frozenKBRerankKey]; !exists {
				return nil, nil
			}
			descriptor, _, err := resolve(ctx, frozenKBRerankKey)
			return descriptor, err
		})
	}
	deps.GetKB = func(ctx context.Context, id int64) (*model.KnowledgeBase, error) {
		owner, ok := security.UserIDFromContext(ctx)
		if !ok || owner != run.OwnerID {
			return nil, errcode.ErrForbidden
		}
		current, err := s.svcs.Repos.KB.GetKBForUser(ctx, id, owner)
		if err != nil {
			return nil, errcode.ErrKBNotFound
		}
		var snapshot model.KnowledgeBase
		raw, ok := definition.KBs[id]
		if !ok || json.Unmarshal(raw, &snapshot) != nil || snapshot.UserID != owner {
			return nil, errcode.ErrKBNotFound
		}
		if snapshot.EmbeddingVersion == "" || snapshot.EmbeddingDimension < 1 || current.EmbeddingVersion != snapshot.EmbeddingVersion || current.MilvusCollection != snapshot.MilvusCollection {
			return nil, execution.ErrVersionMismatch
		}
		snapshot.EmbeddingSnapshot = append(snapshot.EmbeddingSnapshot[:0], definition.Models["kb-embedding:"+strconv.FormatInt(id, 10)]...)
		return &snapshot, nil
	}
	return deps, nil
}

type frozenExecutor struct {
	base      tool.Executor
	snapshots map[string]frozenTool
}

func (f *frozenExecutor) List(raw string) ([]llm.ToolDef, error) {
	return f.ListContext(context.Background(), raw)
}
func (f *frozenExecutor) ListContext(ctx context.Context, bindings string) ([]llm.ToolDef, error) {
	if f.base == nil {
		return nil, errcode.ErrToolNotFound
	}
	current, err := f.base.ListContext(ctx, bindings)
	if err != nil {
		return nil, err
	}
	for i, def := range current {
		frozen, ok := f.snapshots[def.Name]
		if !ok {
			return nil, errcode.ErrToolNotFound
		}
		before, _ := json.Marshal(frozen.Parameters)
		after, _ := json.Marshal(def.Parameters)
		policy := def.ReplayPolicy
		if policy == "" {
			policy = "manual"
		}
		if string(before) != string(after) || policy != frozen.ReplayPolicy || def.BindingVersion != frozen.BindingVersion {
			return nil, fmt.Errorf("%w: tool configuration changed", execution.ErrConflict)
		}
		current[i].Description = frozen.Description
		current[i].Parameters = frozen.Parameters
		current[i].Dangerous = def.Dangerous || frozen.Dangerous
		current[i].ReplayPolicy = frozen.ReplayPolicy
		current[i].SourceIdentity = frozen.SourceIdentity
	}
	return current, nil
}
func (f *frozenExecutor) Invoke(ctx context.Context, bindings, name, args string) tool.ToolResult {
	defs, err := f.ListContext(ctx, bindings)
	if err != nil || len(defs) != 1 || defs[0].Name != name {
		return tool.ToolResult{Status: "error", Output: "tool binding revoked or changed", ToolName: name}
	}
	return f.base.Invoke(tool.WithBindingVersion(ctx, name, defs[0].BindingVersion), bindings, name, args)
}
