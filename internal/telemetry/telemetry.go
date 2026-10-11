package telemetry

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ke_http_requests_total",
		Help: "Total HTTP requests processed",
	}, []string{"method", "path", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ke_http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	CacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ke_cache_hits_total",
		Help: "Redis cache hits",
	})

	CacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ke_cache_misses_total",
		Help: "Redis cache misses",
	})

	EventsIngestedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ke_events_ingested_total",
		Help: "Events ingested per ticketer source",
	}, []string{"source", "status"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ke_crawler_circuit_breaker_state",
		Help: "Circuit breaker state (0=closed, 1=half-open, 2=open)",
	}, []string{"domain"})

	OutboxPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ke_outbox_events_published_total",
		Help: "Outbox events published to Kafka",
	})

	DLQMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ke_dlq_messages_total",
		Help: "Messages sent to Dead Letter Queue",
	})
)

func NewLogger(serviceName string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", serviceName)
}

func InitTracer(ctx context.Context, serviceName, otlpEndpoint string) func(context.Context) error {
	if otlpEndpoint == "" {
		return func(context.Context) error { return nil }
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return func(context.Context) error { return nil }
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown
}
