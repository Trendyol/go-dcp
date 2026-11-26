package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/tracing"
	"github.com/Trendyol/go-dcp/wrapper"
	"github.com/couchbase/gocbcore/v10"
)

// Embedding an uninitialized interface makes unexpected network calls fail
// immediately. CloseStream delivers the same end notification as the SDK.
type streamTestClient struct {
	couchbase.Client
	observer couchbase.Observer
	opened   chan uint16
}

func (c *streamTestClient) GetNumVBuckets() int { return 1 }

func (c *streamTestClient) CloseStream(vbID uint16) error {
	c.observer.End(models.DcpStreamEnd{VbID: vbID}, gocbcore.ErrDCPStreamClosed)
	return nil
}

func (c *streamTestClient) OpenStream(
	vbID uint16, _ map[uint32]string, _ *models.Offset, _ couchbase.Observer,
) error {
	c.opened <- vbID
	return nil
}

func newTestStream() *stream {
	cfg := &config.Dcp{}
	cfg.RollbackMitigation.Disabled = true
	client := &streamTestClient{}
	s := &stream{
		client: client, config: cfg, eventHandler: &models.EmptyEventHandler{},
		lifecycle: newStreamLifecycle(),
		stopCh:    make(chan struct{}),
		offsets:   wrapper.CreateConcurrentSwissMap[uint16, *models.Offset](1),
		observers: wrapper.CreateConcurrentSwissMap[uint16, couchbase.Observer](1),
	}
	s.lifecycle.activeStreams.Store(1)
	s.activeStreams.Store(1)
	s.offsets.Store(0, &models.Offset{})
	lifecycle := s.lifecycle
	client.observer = couchbase.NewObserver(cfg, 0, 0, s.listen,
		func(ctx models.DcpStreamEndContext) { s.listenEnd(ctx, lifecycle) }, nil, tracing.NewTracerComponent())
	s.observers.Store(0, client.observer)
	return s
}

func TestStream_RebalanceCompletionDoesNotStopReopenedStream(t *testing.T) {
	s := newTestStream()
	s.balancing = true
	s.Close(false)

	// The waiter can be scheduled after rebalance completes. Its old completion
	// must not be interpreted using the new stream's balancing state.
	s.balancing = false
	s.wait(s.lifecycle)
	select {
	case <-s.stopCh:
		t.Fatal("rebalance completion stopped the consumer after the stream reopened")
	default:
	}
}

func TestStream_LateEndDoesNotCompleteReopenedStream(t *testing.T) {
	s := newTestStream()
	for range 3 {
		old := s.lifecycle
		s.balancing = true
		s.Close(false)
		s.lifecycle = newStreamLifecycle()
		s.lifecycle.activeStreams.Store(1)
		s.balancing = false
		s.listenEnd(models.DcpStreamEndContext{Event: models.DcpStreamEnd{VbID: 0}}, old)
		s.wait(old)
		select {
		case <-s.stopCh:
			t.Fatal("old completion terminated the consumer")
		case <-s.lifecycle.done:
			t.Fatal("old callback completed the new stream")
		default:
		}
		if got := s.lifecycle.activeStreams.Load(); got != 1 {
			t.Fatalf("old callback changed new active streams: %d", got)
		}
		// Restore transport fixtures for the next opening without DB access.
		s.offsets.Store(0, &models.Offset{})
		s.observers = wrapper.CreateConcurrentSwissMap[uint16, couchbase.Observer](1)
	}
}

func TestStream_EndCompletesConsumer(t *testing.T) {
	for _, err := range []error{nil, gocbcore.ErrDCPStreamClosed} {
		s := newTestStream()
		s.listenEnd(models.DcpStreamEndContext{Event: models.DcpStreamEnd{VbID: 0}, Err: err}, s.lifecycle)
		s.wait(s.lifecycle)
		select {
		case <-s.stopCh:
		default:
			t.Fatal("last stream end did not stop the consumer")
		}
	}
}

func TestStream_CloseStopsConsumer(t *testing.T) {
	s := newTestStream()
	s.Close(true)
	s.wait(s.lifecycle)
	select {
	case <-s.stopCh:
	default:
		t.Fatal("terminal close did not stop the consumer")
	}
}

func TestStreamLifecycle_ConcurrentCompletionIsDeliveredOnce(t *testing.T) {
	lifecycle := newStreamLifecycle()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); lifecycle.finish(true) }()
	}
	wg.Wait()
	if !<-lifecycle.done {
		t.Fatal("terminal completion was lost")
	}
	select {
	case <-lifecycle.done:
		t.Fatal("completion was delivered more than once")
	default:
	}
}

func TestStream_UnexpectedEndReopensStream(t *testing.T) {
	s := newTestStream()
	client := s.client.(*streamTestClient)
	client.opened = make(chan uint16, 1)
	s.listenEnd(models.DcpStreamEndContext{
		Event: models.DcpStreamEnd{VbID: 0}, Err: gocbcore.ErrDCPStreamDisconnected,
	}, s.lifecycle)
	select {
	case vbID := <-client.opened:
		if vbID != 0 {
			t.Fatalf("reopened wrong vbucket: %d", vbID)
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected end did not reopen the stream")
	}
	select {
	case <-s.lifecycle.done:
		t.Fatal("recoverable error completed the stream")
	default:
	}
}

func TestStream_CloseClearsActiveStreamMetric(t *testing.T) {
	for _, rebalance := range []bool{false, true} {
		s := newTestStream()
		s.balancing = rebalance
		s.Close(!rebalance)
		_, active := s.GetMetric()
		if active != 0 {
			t.Fatalf("closed stream reports %d active streams", active)
		}
	}
}

type rebalanceEndHandler struct {
	models.EmptyEventHandler
	before func()
}

func (h *rebalanceEndHandler) BeforeRebalanceStart() { h.before() }

func TestStream_EndDuringRebalanceStartDoesNotStopConsumer(t *testing.T) {
	s := newTestStream()
	old := s.lifecycle
	s.config.Dcp.Group.Membership.RebalanceDelay = time.Hour
	s.eventHandler = &rebalanceEndHandler{before: func() {
		s.listenEnd(models.DcpStreamEndContext{Event: models.DcpStreamEnd{VbID: 0}}, old)
	}}
	s.Rebalance()
	s.rebalanceTimer.Stop()
	s.balancing = false
	s.wait(old)
	select {
	case <-s.stopCh:
		t.Fatal("end during rebalance start stopped the consumer")
	default:
	}
}

func TestStream_MetricDuringLifecycleReplacement(t *testing.T) {
	s := newTestStream()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 1000 {
			s.GetMetric()
		}
	}()
	for range 1000 {
		s.lifecycle = newStreamLifecycle()
	}
	wg.Wait()
}
