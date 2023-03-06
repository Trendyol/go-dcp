package helpers

import (
	"context"

	"github.com/couchbase/gocbcore/v10"
)

type AsyncOp interface {
	Reject()
	Resolve()
	Wait(op gocbcore.PendingOp, err error) error
}

type asyncOp struct {
	ctx         context.Context
	signal      chan struct{}
	wasResolved bool
}

func (m *asyncOp) Reject() {
	m.signal <- struct{}{}
}

func (m *asyncOp) Resolve() {
	m.wasResolved = true
	m.signal <- struct{}{}
}

func (m *asyncOp) Wait(op gocbcore.PendingOp, err error) error {
	if err != nil {
		return err
	}

	select {
	case <-m.signal:
	case <-m.ctx.Done():
		op.Cancel()
		<-m.signal
	}

	return m.ctx.Err()
}

func NewAsyncOp(ctx context.Context) AsyncOp {
	return &asyncOp{
		signal: make(chan struct{}, 1),
		ctx:    ctx,
	}
}
