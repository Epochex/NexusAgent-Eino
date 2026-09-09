// OpenAI 兼容协议实现：覆盖 DeepSeek / 通义 / 智谱 / Kimi / 豆包 Ark / Ollama。
//
// 教学要点（Ep 02）：
//   1. Chat Completions 的消息协议：assistant 的 tool_calls 如何回传、tool 消息如何接续；
//   2. SSE 流式解析：delta 增量拼接、tool_calls 按 index 聚合片段、[DONE] 终止帧；
//   3. 手写 SSE 解析是理解"流式输出本质"的最佳练习（对比 eino 内部实现）。
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// openaiCompatibleProvider OpenAI 兼容协议实现。
type openaiCompatibleProvider struct {
	providerCode string
	httpClient   *http.Client
}

func (p *openaiCompatibleProvider) client() *http.Client {
	if p.httpClient == nil {
		p.httpClient = &http.Client{Timeout: 0} // 超时由 ctx 控制
	}
	return p.httpClient
}

// ---- 请求结构 ----

type openaiTool struct {
	Type     string       `json:"type"`
	Function openaiFnSpec `json:"function"`
}

type openaiFnSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type openaiRequest struct {
	Model         string         `json:"model"`
	Messages      []openaiMsg    `json:"messages"`
	Tools         []openaiTool   `json:"tools,omitempty"`
	ToolChoice    string         `json:"tool_choice,omitempty"`
	Temperature   float32        `json:"temperature,omitempty"`
	TopP          float32        `json:"top_p,omitempty"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type openaiMsg struct {
	Role             string        `json:"role"`
	Content          any           `json:"content"` // string 或 null（有 tool_calls 时）
	ReasoningContent string        `json:"reasoning_content,omitempty"`
	ToolCalls        []openaiCall  `json:"tool_calls,omitempty"`
	ToolCallID       string        `json:"tool_call_id,omitempty"`
}

type openaiCall struct {
	Index    int    `json:"index,omitempty"`
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Function struct {
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
	} `json:"function"`
}

// ---- 响应结构 ----

type openaiResp struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content          string       `json:"content"`
			ReasoningContent string       `json:"reasoning_content"`
			ToolCalls        []openaiCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *openaiUsage `json:"usage"`
	Error *openaiErr   `json:"error"`
}

type openaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type openaiErr struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// ---- 内部消息转换 ----

func toOpenAIMsgs(msgs []Message) []openaiMsg {
	out := make([]openaiMsg, 0, len(msgs))
	for _, m := range msgs {
		om := openaiMsg{Role: m.Role}
		switch {
		case m.Role == RoleAssistant && len(m.ToolCalls) > 0:
			// 有工具调用的 assistant 消息：content 允许为空
			om.Content = m.Content
			for _, tc := range m.ToolCalls {
				om.ToolCalls = append(om.ToolCalls, openaiCall{
					ID: tc.ID, Type: "function",
					Function: struct {
						Name      string `json:"name,omitempty"`
						Arguments string `json:"arguments,omitempty"`
					}{Name: tc.Name, Arguments: tc.ArgsJSON},
				})
			}
		case m.Role == RoleTool:
			om.Content = m.Content
			om.ToolCallID = m.ToolCallID
		default:
			om.Content = m.Content
			om.ReasoningContent = m.ReasoningContent
		}
		out = append(out, om)
	}
	return out
}

func toOpenAITools(defs []ToolDef) []openaiTool {
	if len(defs) == 0 {
		return nil
	}
	out := make([]openaiTool, 0, len(defs))
	for _, d := range defs {
		out = append(out, openaiTool{
			Type: "function",
			Function: openaiFnSpec{
				Name: d.Name, Description: d.Description, Parameters: d.Parameters,
			},
		})
	}
	return out
}

// ---- 非流式 ----

func (p *openaiCompatibleProvider) Chat(ctx context.Context, desc *ModelDescriptor, req *ChatRequest) (*ChatResponse, error) {
	body := openaiRequest{
		Model: desc.ModelName, Messages: toOpenAIMsgs(req.Messages),
		Tools: toOpenAITools(req.Tools), Temperature: desc.Temperature,
		TopP: desc.TopP, MaxTokens: desc.MaxTokens,
	}
	if len(body.Tools) > 0 {
		body.ToolChoice = "auto"
	}

	httpReq, err := p.newHTTPRequest(ctx, desc, body)
	if err != nil {
		return nil, err
	}

	httpResp, err := p.client().Do(httpReq)
	if err != nil {
		return nil, classifyHTTPError(err)
	}
	defer httpResp.Body.Close()

	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, parseAPIError(httpResp.StatusCode, raw)
	}

	var resp openaiResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("upstream: %s", resp.Error.Message)
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("empty choices")
	}

	ch := resp.Choices[0]
	calls := make([]ToolCall, 0, len(ch.Message.ToolCalls))
	for _, c := range ch.Message.ToolCalls {
		calls = append(calls, ToolCall{ID: c.ID, Name: c.Function.Name, ArgsJSON: c.Function.Arguments})
	}

	usage := Usage{}
	if resp.Usage != nil {
		usage = Usage{PromptTokens: resp.Usage.PromptTokens, CompletionTokens: resp.Usage.CompletionTokens}
	}
	return &ChatResponse{
		Content: ch.Message.Content, ReasoningContent: ch.Message.ReasoningContent,
		ToolCalls: calls, FinishReason: ch.FinishReason, Model: desc.ModelName, Usage: usage,
	}, nil
}

// newHTTPRequest 构造上游请求。
func (p *openaiCompatibleProvider) newHTTPRequest(ctx context.Context, desc *ModelDescriptor, body any) (*http.Request, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	url := strings.TrimRight(desc.BaseURL, "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if desc.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+desc.APIKey)
	}
	return req, nil
}

// ---- 流式 ----

func (p *openaiCompatibleProvider) ChatStream(ctx context.Context, desc *ModelDescriptor, req *ChatRequest) (<-chan StreamEvent, error) {
	body := openaiRequest{
		Model: desc.ModelName, Messages: toOpenAIMsgs(req.Messages),
		Tools: toOpenAITools(req.Tools), Temperature: desc.Temperature,
		TopP: desc.TopP, MaxTokens: desc.MaxTokens,
		Stream: true, StreamOptions: &streamOptions{IncludeUsage: true},
	}
	if len(body.Tools) > 0 {
		body.ToolChoice = "auto"
	}

	httpReq, err := p.newHTTPRequest(ctx, desc, body)
	if err != nil {
		return nil, err
	}

	httpResp, err := p.client().Do(httpReq)
	if err != nil {
		return nil, classifyHTTPError(err)
	}
	if httpResp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		return nil, parseAPIError(httpResp.StatusCode, raw)
	}

	// 事件通道：带缓冲减少阻塞；ctx 取消时 goroutine 退出
	events := make(chan StreamEvent, 64)
	go p.consumeStream(ctx, httpResp.Body, events)
	return events, nil
}

// consumeStream 解析 SSE 数据流。
//
//	SSE 帧格式：
//	  data: {"choices":[{"delta":{"content":"x"},...}]}
//	  data: [DONE]
//
//	tool_calls 的 arguments 是分片到达的（按 index 聚合后拼 JSON）。
func (p *openaiCompatibleProvider) consumeStream(ctx context.Context, body io.ReadCloser, events chan<- StreamEvent) {
	defer close(events)
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 512*1024)

	type callAgg struct {
		id, name, args strings.Builder
	}
	aggs := map[int]*callAgg{}
	finish := ""
	var usage *Usage
	model := ""

	emit := func(ev StreamEvent) {
		select {
		case events <- ev:
		case <-ctx.Done(): // 上游取消，停止读取
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // 忽略空行与注释行
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}

		var frame struct {
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Content          string       `json:"content"`
					ReasoningContent string       `json:"reasoning_content"`
					ToolCalls        []openaiCall `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *openaiUsage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &frame); err != nil {
			continue // 跳过无法解析的帧（部分供应商会发注释帧）
		}
		if frame.Model != "" {
			model = frame.Model
		}

		for _, ch := range frame.Choices {
			d := ch.Delta
			if d.ReasoningContent != "" {
				emit(StreamEvent{ReasoningContent: d.ReasoningContent, Model: model})
			}
			if d.Content != "" {
				emit(StreamEvent{Content: d.Content, Model: model})
			}
			for _, tc := range d.ToolCalls {
				agg := aggs[tc.Index]
				if agg == nil {
					agg = &callAgg{}
					aggs[tc.Index] = agg
				}
				if tc.ID != "" {
					agg.id.WriteString(tc.ID)
				}
				if tc.Function.Name != "" {
					agg.name.WriteString(tc.Function.Name)
				}
				agg.args.WriteString(tc.Function.Arguments)
			}
			if ch.FinishReason != nil && *ch.FinishReason != "" {
				finish = *ch.FinishReason
			}
		}
		if frame.Usage != nil {
			usage = &Usage{PromptTokens: frame.Usage.PromptTokens, CompletionTokens: frame.Usage.CompletionTokens}
		}
	}

	// 终帧：携带聚合完成的工具调用与用量
	calls := make([]ToolCall, 0, len(aggs))
	for i := 0; i < len(aggs); i++ {
		if a, ok := aggs[i]; ok {
			calls = append(calls, ToolCall{ID: a.id.String(), Name: a.name.String(), ArgsJSON: a.args.String()})
		}
	}
	if len(calls) > 0 {
		finish = "tool_calls"
	}
	emit(StreamEvent{ToolCalls: calls, FinishReason: finish, Usage: usage, Model: model})
}

// ---- Embedding ----

func (p *openaiCompatibleProvider) Embed(ctx context.Context, desc *ModelDescriptor, texts []string) ([][]float32, error) {
	payload, _ := json.Marshal(map[string]any{"model": desc.ModelName, "input": texts})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(desc.BaseURL, "/")+"/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if desc.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+desc.APIKey)
	}

	resp, err := p.client().Do(req)
	if err != nil {
		return nil, classifyHTTPError(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp.StatusCode, raw)
	}

	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode embeddings: %w", err)
	}
	vectors := make([][]float32, len(texts))
	for _, d := range out.Data {
		vectors[d.Index] = d.Embedding
	}
	return vectors, nil
}

// ---- Rerank（Jina/SiliconFlow 风格 /rerank 接口）----

type rerankResp struct {
	Results []struct {
		Index          int     `json:"index"`
		RelevanceScore float32 `json:"relevance_score"`
	} `json:"results"`
}

func (p *openaiCompatibleProvider) Rerank(ctx context.Context, desc *ModelDescriptor, query string, docs []string, topN int) ([]RerankResult, error) {
	payload, _ := json.Marshal(map[string]any{
		"model": desc.ModelName, "query": query, "documents": docs, "top_n": topN,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(desc.BaseURL, "/")+"/rerank", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if desc.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+desc.APIKey)
	}

	resp, err := p.client().Do(req)
	if err != nil {
		return nil, classifyHTTPError(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp.StatusCode, raw)
	}
	var out rerankResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode rerank: %w", err)
	}
	results := make([]RerankResult, 0, len(out.Results))
	for _, r := range out.Results {
		results = append(results, RerankResult{Index: r.Index, Score: r.RelevanceScore})
	}
	return results, nil
}

// ---- 错误分类（决定重试/熔断策略）----

type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

// IsRetryable 是否可重试（429/5xx/网络错误）。
func IsRetryable(err error) bool {
	var re *retryableError
	return asRetryable(err, &re)
}

func asRetryable(err error, target **retryableError) bool {
	for err != nil {
		if re, ok := err.(*retryableError); ok {
			*target = re
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func classifyHTTPError(err error) error {
	return &retryableError{err: err}
}

// parseAPIError 上游非 200：429/5xx 标记可重试。
func parseAPIError(status int, body []byte) error {
	msg := string(body)
	var eResp openaiErr
	if json.Unmarshal(body, &eResp) == nil && eResp.Message != "" {
		msg = eResp.Message
	}
	err := fmt.Errorf("upstream %d: %s", status, msg)
	if status == http.StatusTooManyRequests || status >= 500 {
		return &retryableError{err: err}
	}
	return err
}

// defaultFirstTokenTimeout 首 token 超时兜底（网关可覆盖）。
const defaultFirstTokenTimeout = 30 * time.Second
