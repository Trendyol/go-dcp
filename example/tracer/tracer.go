package main

import (
	"context"
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type OpenTelemetryRequestTracer struct {
	wrapped trace.Tracer
}

func NewOpenTelemetryRequestTracer(provider trace.TracerProvider) *OpenTelemetryRequestTracer {
	return &OpenTelemetryRequestTracer{
		wrapped: provider.Tracer("com.couchbase.client/go"),
	}
}

func (tracer *OpenTelemetryRequestTracer) RequestSpan(parentContext gocbcore.RequestSpanContext, operationName string) gocbcore.RequestSpan {
	parentCtx := context.Background()
	if ctx, ok := parentContext.(context.Context); ok {
		parentCtx = ctx
	}

	return NewOpenTelemetryRequestSpan(tracer.wrapped.Start(parentCtx, operationName))
}

type OpenTelemetryRequestSpan struct {
	ctx     context.Context
	wrapped trace.Span
}

func NewOpenTelemetryRequestSpan(ctx context.Context, span trace.Span) *OpenTelemetryRequestSpan {
	return &OpenTelemetryRequestSpan{
		ctx:     ctx,
		wrapped: span,
	}
}

func (span *OpenTelemetryRequestSpan) End() {
	span.wrapped.End()
}

func (span *OpenTelemetryRequestSpan) Context() gocbcore.RequestSpanContext {
	return span.ctx
}

func (span *OpenTelemetryRequestSpan) SetAttribute(key string, value interface{}) {
	switch v := value.(type) {
	case string:
		span.wrapped.SetAttributes(attribute.String(key, v))
	case *string:
		span.wrapped.SetAttributes(attribute.String(key, *v))
	case bool:
		span.wrapped.SetAttributes(attribute.Bool(key, v))
	case *bool:
		span.wrapped.SetAttributes(attribute.Bool(key, *v))
	case int:
		span.wrapped.SetAttributes(attribute.Int(key, v))
	case *int:
		span.wrapped.SetAttributes(attribute.Int(key, *v))
	case int64:
		span.wrapped.SetAttributes(attribute.Int64(key, v))
	case *int64:
		span.wrapped.SetAttributes(attribute.Int64(key, *v))
	case uint32:
		span.wrapped.SetAttributes(attribute.Int(key, int(v)))
	case *uint32:
		span.wrapped.SetAttributes(attribute.Int(key, int(*v)))
	case float64:
		span.wrapped.SetAttributes(attribute.Float64(key, v))
	case *float64:
		span.wrapped.SetAttributes(attribute.Float64(key, *v))
	case []string:
		span.wrapped.SetAttributes(attribute.StringSlice(key, v))
	case []bool:
		span.wrapped.SetAttributes(attribute.BoolSlice(key, v))
	case []int:
		span.wrapped.SetAttributes(attribute.IntSlice(key, v))
	case []int64:
		span.wrapped.SetAttributes(attribute.Int64Slice(key, v))
	case []float64:
		span.wrapped.SetAttributes(attribute.Float64Slice(key, v))
	case fmt.Stringer:
		span.wrapped.SetAttributes(attribute.String(key, v.String()))
	default:
		log.Println("Unable to determine value as a type that we can handle")
	}
}

func (span *OpenTelemetryRequestSpan) AddEvent(key string, timestamp time.Time) {
	span.wrapped.AddEvent(key, trace.WithTimestamp(timestamp))
}
