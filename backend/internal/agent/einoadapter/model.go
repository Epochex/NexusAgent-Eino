// Package einoadapter adapts platform models and tools without importing runtime.
package einoadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

type GatewayClient interface {
	Chat(context.Context, *llm.ModelDescriptor, *llm.ModelDescriptor, *llm.ChatRequest) (*llm.ChatResponse, *llm.ModelDescriptor, error)
	ChatStream(context.Context, *llm.ModelDescriptor, *llm.ModelDescriptor, *llm.ChatRequest) (<-chan llm.StreamEvent, *llm.ModelDescriptor, error)
}

type chatModel struct {
	gateway           GatewayClient
	primary, fallback *llm.ModelDescriptor
	tools             []*schema.ToolInfo
	stepTimeout       time.Duration
}

func NewChatModel(g GatewayClient, primary, fallback *llm.ModelDescriptor, stepTimeout time.Duration) (model.ToolCallingChatModel, error) {
	if g == nil || primary == nil || primary.ModelName == "" || stepTimeout <= 0 {
		return nil, fmt.Errorf("gateway, primary model and positive step timeout are required")
	}
	return &chatModel{gateway: g, primary: copyDescriptor(primary), fallback: copyDescriptor(fallback), stepTimeout: stepTimeout}, nil
}

func copyDescriptor(m *llm.ModelDescriptor) *llm.ModelDescriptor {
	if m == nil {
		return nil
	}
	next := *m
	if next.MaxOutputTokens == 0 {
		next.MaxOutputTokens = m.MaxTokens
	}
	return &next
}

func (m *chatModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	copied, err := cloneTools(tools)
	if err != nil {
		return nil, err
	}
	next := *m
	next.tools = copied
	return &next, nil
}

func cloneTools(tools []*schema.ToolInfo) ([]*schema.ToolInfo, error) {
	cloned := make([]*schema.ToolInfo, 0, len(tools))
	seen := map[string]bool{}
	for _, tool := range tools {
		if tool == nil || tool.Name == "" || seen[tool.Name] {
			return nil, fmt.Errorf("tool names must be nonempty and unique")
		}
		seen[tool.Name] = true
		var params *jsonschema.Schema
		var err error
		if tool.ParamsOneOf == nil {
			params = &jsonschema.Schema{Type: "object"}
		} else {
			params, err = tool.ParamsOneOf.ToJSONSchema()
			if err != nil {
				return nil, err
			}
		}
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		var copied jsonschema.Schema
		if err = json.Unmarshal(raw, &copied); err != nil {
			return nil, err
		}
		cloned = append(cloned, &schema.ToolInfo{Name: tool.Name, Desc: tool.Desc, ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&copied)})
	}
	return cloned, nil
}

func (m *chatModel) request(input []*schema.Message, opts ...model.Option) (*llm.ChatRequest, *llm.ModelDescriptor, *llm.ModelDescriptor, error) {
	o := model.GetCommonOptions(&model.Options{Tools: m.tools}, opts...)
	if len(o.Stop) > 0 || o.ToolChoice != nil && *o.ToolChoice != schema.ToolChoiceAllowed || len(o.DeferredTools) > 0 || o.ToolSearchTool != nil || o.AgenticToolChoice != nil || len(o.AllowedToolNames) > 0 {
		return nil, nil, nil, fmt.Errorf("unsupported model option")
	}
	primary, fallback := copyDescriptor(m.primary), copyDescriptor(m.fallback)
	if o.Model != nil {
		primary.ModelName = *o.Model
	}
	for _, desc := range []*llm.ModelDescriptor{primary, fallback} {
		if desc == nil {
			continue
		}
		if o.Temperature != nil {
			desc.Temperature = *o.Temperature
		}
		if o.TopP != nil {
			desc.TopP = *o.TopP
		}
		if o.MaxTokens != nil {
			if *o.MaxTokens <= 0 {
				return nil, nil, nil, fmt.Errorf("max tokens must be positive")
			}
			desc.MaxTokens = *o.MaxTokens
		}
	}
	req := &llm.ChatRequest{}
	for _, msg := range input {
		converted, err := FromMessage(msg)
		if err != nil {
			return nil, nil, nil, err
		}
		req.Messages = append(req.Messages, converted)
	}
	tools, err := cloneTools(o.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, info := range tools {
		params, err := info.ParamsOneOf.ToJSONSchema()
		if err != nil {
			return nil, nil, nil, err
		}
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, nil, nil, err
		}
		var fields map[string]any
		if err = json.Unmarshal(raw, &fields); err != nil {
			return nil, nil, nil, err
		}
		req.Tools = append(req.Tools, llm.ToolDef{Name: info.Name, Description: info.Desc, Parameters: fields})
	}
	return req, primary, fallback, nil
}

func (m *chatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	req, primary, fallback, err := m.request(input, opts...)
	if err != nil {
		return nil, err
	}
	stepCtx, cancel := context.WithTimeout(ctx, m.stepTimeout)
	defer cancel()
	res, used, err := m.gateway.Chat(stepCtx, primary, fallback, req)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("model returned a nil response")
	}
	name := res.Model
	if used != nil {
		name = used.ModelName
	}
	return responseMessage(llm.StreamEvent{Content: res.Content, ReasoningContent: res.ReasoningContent, ToolCalls: res.ToolCalls, FinishReason: res.FinishReason, Usage: &res.Usage, Model: name}), nil
}

func responseMessage(ev llm.StreamEvent) *schema.Message {
	m := &schema.Message{Role: schema.Assistant, Content: ev.Content, ReasoningContent: ev.ReasoningContent, Extra: map[string]any{"model": ev.Model}}
	for i, tc := range ev.ToolCalls {
		m.ToolCalls = append(m.ToolCalls, schema.ToolCall{Index: new(i), ID: tc.ID, Type: "function", Function: schema.FunctionCall{Name: tc.Name, Arguments: tc.ArgsJSON}})
	}
	if ev.FinishReason != "" || ev.Usage != nil {
		m.ResponseMeta = &schema.ResponseMeta{FinishReason: ev.FinishReason}
		if ev.Usage != nil {
			m.ResponseMeta.Usage = &schema.TokenUsage{PromptTokens: ev.Usage.PromptTokens, CompletionTokens: ev.Usage.CompletionTokens, TotalTokens: ev.Usage.PromptTokens + ev.Usage.CompletionTokens}
		}
	}
	return m
}

func (m *chatModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	req, primary, fallback, err := m.request(input, opts...)
	if err != nil {
		return nil, err
	}
	req.Stream = true
	stepCtx, cancel := context.WithTimeout(ctx, m.stepTimeout)
	source, used, err := m.gateway.ChatStream(stepCtx, primary, fallback, req)
	if err != nil {
		cancel()
		return nil, err
	}
	reader, writer := schema.Pipe[*schema.Message](1)
	reader.SetAutomaticClose()
	finished := make(chan struct{})
	var completed atomic.Bool
	go func() {
		select {
		case <-stepCtx.Done():
			reader.Close()
		case <-finished:
		}
	}()
	go func() {
		defer close(finished)
		defer cancel()
		defer writer.Close()
		// v0.9.22 has no close callback. Filtered probes detect reader.Close
		// even while upstream is silent; the context watcher bounds blocked Send.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stepCtx.Done():
				return
			case <-ticker.C:
				if writer.Send(nil, nil) {
					return
				}
			case ev, ok := <-source:
				if !ok {
					if stepCtx.Err() == nil {
						completed.Store(true)
					}
					return
				}
				if ev.Err != nil {
					writer.Send(nil, ev.Err)
					return
				}
				if ev.Model == "" {
					if used != nil {
						ev.Model = used.ModelName
					} else {
						ev.Model = primary.ModelName
					}
				}
				if writer.Send(responseMessage(ev), nil) {
					return
				}
			}
		}
	}()
	return schema.StreamReaderWithConvert(reader, func(msg *schema.Message) (*schema.Message, error) {
		if msg == nil {
			return nil, schema.ErrNoValue
		}
		return msg, nil
	}, schema.WithOnEOF(func() (any, error) {
		if !completed.Load() && stepCtx.Err() != nil {
			return nil, stepCtx.Err()
		}
		return nil, io.EOF
	})), nil
}
