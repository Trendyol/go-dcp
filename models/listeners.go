package models

import (
	"github.com/Trendyol/go-dcp/tracing"
)

type ListenerContext struct {
	Commit                  func()
	Event                   interface{}
	Ack                     func()
	ListenerTracerComponent tracing.ListenerTracerComponent
}

type ListenerArgs struct {
	Event        interface{}
	TraceContext tracing.RequestSpanContext
}

type DcpStreamEndContext struct {
	Err   error
	Event DcpStreamEnd
}

type (
	Listener      func(*ListenerContext)
	ListenerCh    chan ListenerArgs
	ListenerEndCh chan DcpStreamEndContext
)
