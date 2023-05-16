package couchbase

import (
	"context"

	"github.com/couchbase/gocbcore/v10"
)

type AsyncOp interface {
	Resolve()
	Wait(op gocbcore.PendingOp, err error) error
}

type asyncOp struct {
	ctx    context.Context
	signal chan struct{}
}

func (m *asyncOp) Resolve() {
	m.signal <- struct{}{}
}

func (m *asyncOp) Wait(op gocbcore.PendingOp, err error) error {
	if err != nil {
		return err
	}

	select {
	case <-m.ctx.Done():
		op.Cancel()
	case <-m.signal:
	}

	return m.ctx.Err()
}

func NewAsyncOp(ctx context.Context) AsyncOp {
	return &asyncOp{
		signal: make(chan struct{}, 1),
		ctx:    ctx,
	}
}
