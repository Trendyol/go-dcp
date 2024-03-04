package tracing

import (
	"github.com/couchbase/gocbcore/v10"
)

type Tracer interface {
	RequestSpan(parentContext gocbcore.RequestSpanContext, operationName string) gocbcore.RequestSpan
}

type RequestSpanContext interface {
}

const (
	spanAttribDcpKey   = "system"
	spanAttribDcpValue = "dcp"
)

type Trace struct {
	rsc RequestSpanContext
	rs  gocbcore.RequestSpan
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

type TracerComponent struct {
	tracer Tracer
}

func NewTracer(tracer Tracer) *TracerComponent {
	return &TracerComponent{
		tracer: tracer,
	}
}

func (tc *TracerComponent) CreateTrace(operationName string, parentContext RequestSpanContext) *Trace {
	s := tc.tracer.RequestSpan(parentContext, operationName)
	s.SetAttribute(spanAttribDcpKey, spanAttribDcpValue)

	return &Trace{
		rsc: parentContext,
		rs:  s,
	}
}
