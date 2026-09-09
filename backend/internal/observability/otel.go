// Package observability 可观测性：OpenTelemetry 追踪初始化。
// 全链路：HTTP 中间件 → service → agent 每一步决策 → LLM/工具调用 都产生 span，
// 导出 OTLP → Jaeger 可视化（Ep 16）。
package observability

import (
	"context"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracer 初始化 OTel TracerProvider。
// endpoint 形如 http://127.0.0.1:4318（Jaeger OTLP HTTP 端口）。
func InitTracer(ctx context.Context, endpoint string) (func(context.Context) error, error) {
	if strings.TrimSpace(endpoint) == "" {
		return func(context.Context) error { return nil }, nil
	}

	// 拆出 host:port 供 otlptracehttp 使用（它不接受 scheme）
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(u.Host),
		otlptracehttp.WithInsecure(), // 本地部署不走 TLS
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("nexus-agent")),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// W3C traceparent 传播（跨进程演示用）
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
