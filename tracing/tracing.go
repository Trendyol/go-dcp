package tracing

import (
	"context"
	"fmt"
	"time"
)

// TracerContext
// ---------------------------------------------------------------------------------------------------------------------
var (
	tracerCtx = tracerContext{tracer: &NoopTracer{}}
)

type tracerContext struct {
	tracer RequestTracer
}

func RegisterRequestTracer(requestTracer RequestTracer) error {
	if _, ok := tracerCtx.tracer.(*NoopTracer); ok {
		tracerCtx.tracer = requestTracer
		return nil
	}

	return fmt.Errorf("RequestTracer already registered %s", requestTracer)
}

// ---------------------------------------------------------------------------------------------------------------------

type RequestTracer interface {
	RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan
}

type RequestSpanContext struct {
	RefCtx context.Context
	Value  interface{}
}

type RequestSpan interface {
	End()
	Context() RequestSpanContext
	AddEvent(name string, timestamp time.Time)

	SetAttribute(key string, value interface{})
}

// noop tracer
// ---------------------------------------------------------------------------------------------------------------------
type noopSpan struct{}

var (
	defaultNoopSpanContext = RequestSpanContext{RefCtx: context.TODO(), Value: nil}
	defaultNoopSpan        = noopSpan{}
)

//nolint:unused
type NoopTracer struct{}

// RequestSpan creates a new RequestSpan.
func (tracer *NoopTracer) RequestSpan(parentContext RequestSpanContext, operationName string) RequestSpan {
	return defaultNoopSpan
}

// End completes the span.
func (span noopSpan) End() {
}

// Context returns the RequestSpanContext for this span.
func (span noopSpan) Context() RequestSpanContext {
	return defaultNoopSpanContext
}

// SetAttribute adds an attribute to this span.
func (span noopSpan) SetAttribute(key string, value interface{}) {
}

// AddEvent adds an event to this span.
func (span noopSpan) AddEvent(key string, timestamp time.Time) {
}

// Trace
// ---------------------------------------------------------------------------------------------------------------------

type Trace struct {
	rsc RequestSpanContext
	rs  RequestSpan
}

func (t *Trace) Finish() {
	if t.rs != nil {
		t.rs.End()
	}
}

func (t *Trace) RootContext() RequestSpanContext {
	if t.rs != nil {
		return t.rs.Context()
	}
	return t.rsc
}

// ---------------------------------------------------------------------------------------------------------------------

type ObserverLabels struct {
	collectionIDs map[uint32]string
	vbID          uint16
}

func NewObserverLabels(vbID uint16, collectionIDs map[uint32]string) *ObserverLabels {
	return &ObserverLabels{vbID: vbID, collectionIDs: collectionIDs}
}

type TracerComponent struct {
	tracer RequestTracer
}

func NewTracerComponent() *TracerComponent {
	requestTracer := tracerCtx.tracer
	return &TracerComponent{
		tracer: requestTracer,
	}
}

func (tc *TracerComponent) StartOpTelemeteryHandler(
	service string,
	operation string,
	traceContext RequestSpanContext,
	observerLabels *ObserverLabels,
) *opTelemetryHandler {
	return &opTelemetryHandler{
		tracer:            tc.createOpTrace(operation, traceContext, observerLabels),
		service:           service,
		operation:         operation,
		start:             time.Now(),
		metricsCompleteFn: tc.ResponseValueRecord,
	}
}

func (tc *TracerComponent) createOpTrace(
	operationName string,
	parentContext RequestSpanContext,
	observerLabels *ObserverLabels,
) *opTracer {
	opSpan := tc.tracer.RequestSpan(parentContext, operationName)
	opSpan.SetAttribute("op.attributes", parentContext.Value)

	// Append labels
	opSpan.SetAttribute("vb_id", observerLabels.vbID)
	opSpan.SetAttribute("collection_ids", observerLabels.collectionIDs)

	return &opTracer{
		parentContext: parentContext,
		opSpan:        opSpan,
	}
}

type ListenerTracerComponent interface {
	InitializeListenerTrace(operationName string, attributes map[string]interface{}) *listenerTracer
	CreateListenerTrace(trace *listenerTracer, operationName string, attributes map[string]interface{}) *listenerTracer
}

type listenerTracerComponent struct {
	tracerComponent *TracerComponent
	opContext       RequestSpanContext
}

func (tc *TracerComponent) NewListenerTracerComponent(opTracerContext RequestSpanContext) ListenerTracerComponent {
	return &listenerTracerComponent{
		tracerComponent: tc,
		opContext:       opTracerContext,
	}
}

func (tc *listenerTracerComponent) InitializeListenerTrace(
	operationName string,
	attributes map[string]interface{},
) *listenerTracer {
	listenerSpan := tc.tracerComponent.tracer.RequestSpan(tc.opContext, operationName)

	// Append labels
	for key, value := range attributes {
		listenerSpan.SetAttribute(key, value)
	}

	return &listenerTracer{
		parentContext: listenerSpan.Context(),
		span:          listenerSpan,
	}
}

func (tc *listenerTracerComponent) CreateListenerTrace(
	trace *listenerTracer,
	operationName string,
	attributes map[string]interface{},
) *listenerTracer {
	listenerSpan := tc.tracerComponent.tracer.RequestSpan(trace.parentContext, operationName)

	// Append labels
	for key, value := range attributes {
		listenerSpan.SetAttribute(key, value)
	}

	return &listenerTracer{
		parentContext: listenerSpan.Context(),
		span:          listenerSpan,
	}
}

// ---------------------------------------------------------------------------------------------------------------------
type opTracer struct {
	parentContext RequestSpanContext
	opSpan        RequestSpan
}

func (tracer *opTracer) Finish() {
	if tracer.opSpan != nil {
		tracer.opSpan.End()
	}
}

func (tracer *opTracer) RootContext() RequestSpanContext {
	if tracer.opSpan != nil {
		return tracer.opSpan.Context()
	}

	return tracer.parentContext
}

type listenerTracer struct {
	parentContext RequestSpanContext
	span          RequestSpan
}

func (tracer *listenerTracer) Finish() {
	if tracer.span != nil {
		tracer.span.End()
	}
}

func (tracer *listenerTracer) RootContext() RequestSpanContext {
	if tracer.span != nil {
		return tracer.span.Context()
	}

	return tracer.parentContext
}

// ---------------------------------------------------------------------------------------------------------------------
type opTelemetryHandler struct {
	start             time.Time
	tracer            *opTracer
	metricsCompleteFn func(string, string, time.Time)
	service           string
	operation         string
}

func (oth *opTelemetryHandler) RootContext() RequestSpanContext {
	return oth.tracer.RootContext()
}

func (oth *opTelemetryHandler) StartTime() time.Time {
	return oth.start
}

func (oth *opTelemetryHandler) Finish() {
	oth.tracer.Finish()
	oth.metricsCompleteFn(oth.service, oth.operation, oth.start)
}

func (tc *TracerComponent) ResponseValueRecord(service, operation string, start time.Time) {
}
