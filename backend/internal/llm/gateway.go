// LLM 网关主体（td.md §8.1）。
//
// 职责：供应商实例管理 + 熔断 + 失败转移 + 重试 + 并发控制 + 计量打点。
// 网关不查库 —— 调用方（service 层）负责把 model_configs 解析成 ModelDescriptor
// （含解密后的 api_key 与 fallback 描述符），网关保持纯净、可单测。
package llm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/observability"
)

// Gateway 统一大模型网关。
type Gateway struct {
	mu          sync.RWMutex
	providers   map[string]Provider
	breakers    map[string]*CircuitBreaker // 按 provider code
	semaphores  map[string]*Semaphore      // 按 provider code
	retryCfg    RetryConfig
	cfg         *config.LLM
}

// NewGateway 构造。
func NewGateway(cfg *config.LLM) *Gateway {
	return &Gateway{
		providers:  map[string]Provider{},
		breakers:   map[string]*CircuitBreaker{},
		semaphores: map[string]*Semaphore{},
		retryCfg:   DefaultRetryConfig(),
		cfg:        cfg,
	}
}

// providerFor 获取（或创建）供应商实现与配套熔断/信号量。
func (g *Gateway) providerFor(code string) (Provider, *CircuitBreaker, *Semaphore) {
	g.mu.Lock()
	defer g.mu.Unlock()

	p, ok := g.providers[code]
	if !ok {
		p = NewProvider(code)
		g.providers[code] = p
	}
	b, ok := g.breakers[code]
	if !ok {
		b = NewCircuitBreaker(20, g.cfg.CircuitErrorRate, g.cfg.CircuitOpen)
		g.breakers[code] = b
	}
	s, ok := g.semaphores[code]
	if !ok {
		s = NewSemaphore(g.cfg.MaxConnsPerProvider)
		g.semaphores[code] = s
	}
	return p, b, s
}

// runWithGuard 带熔断+限流+重试+指标 的统一执行管道。
func (g *Gateway) runWithGuard(ctx context.Context, desc *ModelDescriptor, fn func(p Provider) error) error {
	p, breaker, sem := g.providerFor(desc.ProviderCode)

	// 熔断检查：打开则立即失败，调用方决定是否走 fallback
	if !breaker.Allow() {
		return ErrCircuitOpen
	}

	start := time.Now()
	err := RetryDo(ctx, g.retryCfg, func(ctx context.Context) error {
		if aErr := sem.Acquire(ctx); aErr != nil {
			return aErr
		}
		defer sem.Release()
		return fn(p)
	})

	// 指标与熔断记录
	status := "ok"
	if err != nil {
		status = "error"
	}
	if m := observability.MetricsInstance(); m != nil {
		m.LLMRequests.WithLabelValues(desc.ModelName, status).Inc()
		m.LLMLatency.WithLabelValues(desc.ModelName).Observe(time.Since(start).Seconds())
	}
	breaker.Record(err == nil)
	return err
}

// Chat 非流式对话（含失败转移）。
// primary 失败且配置了 fallback 时自动切换，返回实际使用的描述符。
func (g *Gateway) Chat(ctx context.Context, primary, fallback *ModelDescriptor, req *ChatRequest) (*ChatResponse, *ModelDescriptor, error) {
	resp, used, err := g.chatOnce(ctx, primary, req)
	if err == nil {
		return resp, used, nil
	}
	if fallback != nil && fallback.ProviderCode != primary.ProviderCode {
		resp2, used2, err2 := g.chatOnce(ctx, fallback, req)
		if err2 == nil {
			return resp2, used2, nil
		}
		return nil, nil, fmt.Errorf("primary: %w; fallback: %w", err, err2)
	}
	return nil, nil, err
}

func (g *Gateway) chatOnce(ctx context.Context, desc *ModelDescriptor, req *ChatRequest) (*ChatResponse, *ModelDescriptor, error) {
	var resp *ChatResponse
	err := g.runWithGuard(ctx, desc, func(p Provider) error {
		var e error
		resp, e = p.Chat(ctx, desc, req)
		return e
	})
	if err != nil {
		return nil, nil, err
	}
	g.recordUsage(desc.ModelName, resp.Usage)
	return resp, desc, nil
}

// ChatStream 流式对话（含失败转移）。
//
// 失败转移策略：仅在"首事件到达前"失败时切换 fallback（流中失败无法重放）。
func (g *Gateway) ChatStream(ctx context.Context, primary, fallback *ModelDescriptor, req *ChatRequest) (<-chan StreamEvent, *ModelDescriptor, error) {
	events, err := g.streamOnce(ctx, primary, req)
	if err == nil {
		return events, primary, nil
	}
	if fallback != nil && fallback.ProviderCode != primary.ProviderCode {
		events2, err2 := g.streamOnce(ctx, fallback, req)
		if err2 == nil {
			return events2, fallback, nil
		}
		return nil, nil, fmt.Errorf("primary: %w; fallback: %w", err, err2)
	}
	return nil, nil, err
}

func (g *Gateway) streamOnce(ctx context.Context, desc *ModelDescriptor, req *ChatRequest) (<-chan StreamEvent, error) {
	var events <-chan StreamEvent
	err := g.runWithGuard(ctx, desc, func(p Provider) error {
		var e error
		events, e = p.ChatStream(ctx, desc, req)
		return e
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Embed 向量化。
func (g *Gateway) Embed(ctx context.Context, desc *ModelDescriptor, texts []string) ([][]float32, error) {
	var vectors [][]float32
	err := g.runWithGuard(ctx, desc, func(p Provider) error {
		var e error
		vectors, e = p.Embed(ctx, desc, texts)
		return e
	})
	return vectors, err
}

// Rerank 重排序。
func (g *Gateway) Rerank(ctx context.Context, desc *ModelDescriptor, query string, docs []string, topN int) ([]RerankResult, error) {
	var results []RerankResult
	err := g.runWithGuard(ctx, desc, func(p Provider) error {
		var e error
		results, e = p.Rerank(ctx, desc, query, docs, topN)
		return e
	})
	return results, err
}

// recordUsage 记录 Token 用量指标。
func (g *Gateway) recordUsage(model string, u Usage) {
	if m := observability.MetricsInstance(); m != nil && u.PromptTokens+u.CompletionTokens > 0 {
		m.LLMTokens.WithLabelValues(model, "prompt").Add(float64(u.PromptTokens))
		m.LLMTokens.WithLabelValues(model, "completion").Add(float64(u.CompletionTokens))
	}
}

// ErrCircuitOpen 熔断打开错误。
var ErrCircuitOpen = fmt.Errorf("provider circuit open")
