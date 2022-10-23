package godcpclient

import (
	"context"
	"github.com/couchbase/gocbcore/v10"
)

type asyncOp struct {
	signal      chan struct{}
	wasResolved bool
	ctx         context.Context
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

	return nil
}

func newAsyncOp(ctx context.Context) *asyncOp {
	if ctx == nil {
		ctx = context.Background()
	}

	return &asyncOp{
		signal: make(chan struct{}, 1),
		ctx:    ctx,
	}
}
