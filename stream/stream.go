package stream

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-dcp/stream/offset"

	"github.com/Trendyol/go-dcp/tracing"

	"github.com/Trendyol/go-dcp/membership"

	"github.com/couchbase/gocbcore/v10"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/metadata"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/helpers"
)

type Stream interface {
	Open()
	Rebalance()
	Save()
	Close(bool)
	GetOffsets() (*wrapper.ConcurrentSwissMap[uint16, *models.Offset], *wrapper.ConcurrentSwissMap[uint16, bool], bool)
	GetObservers() *wrapper.ConcurrentSwissMap[uint16, couchbase.Observer]
	GetMetric() (*Metric, int32)
	UnmarkDirtyOffsets()
	GetCheckpointMetric() *CheckpointMetric
	IsOpen() bool
}

type Metric struct {
	ProcessLatency int64
	DcpLatency     int64
	Rebalance      int
}

type stream struct {
	client                    couchbase.Client
	metadata                  metadata.Metadata
	checkpoint                Checkpoint
	rollbackMitigation        couchbase.RollbackMitigation
	vBucketDiscovery          VBucketDiscovery
	eventHandler              models.EventHandler
	config                    *config.Dcp
	metric                    *Metric
	rebalanceTimer            *time.Timer
	vbIDRange                 *models.VbIDRange
	dirtyOffsets              *wrapper.ConcurrentSwissMap[uint16, bool]
	stopCh                    chan struct{}
	consumer                  models.Consumer
	bucketInfo                *couchbase.BucketInfo
	lifecycle                 *streamLifecycle
	offsets                   *wrapper.ConcurrentSwissMap[uint16, *models.Offset]
	observers                 *wrapper.ConcurrentSwissMap[uint16, couchbase.Observer]
	collectionIDs             map[uint32]string
	streamEndNotSupportedData *streamEndNotSupportedData
	tracerComponent           *tracing.TracerComponent
	rebalanceLock             sync.Mutex
	activeStreams             atomic.Int32
	anyDirtyOffset            bool
	balancing                 bool
	closeWithCancel           bool
	open                      bool
}

// Each opening owns its completion. Neither a late callback nor a delayed
// waiter from an earlier opening may terminate a stream opened by rebalance.
type streamLifecycle struct {
	done          chan bool
	mu            sync.Mutex
	finishOnce    sync.Once
	closing       bool
	activeStreams atomic.Int32
}

func newStreamLifecycle() *streamLifecycle {
	return &streamLifecycle{done: make(chan bool, 1)}
}

func (l *streamLifecycle) finish(terminal bool) {
	l.finishOnce.Do(func() { l.done <- terminal })
}

type streamEndNotSupportedData struct {
	queue  chan struct{}
	ending bool
}

func (s *stream) setOffset(vbID uint16, offset *models.Offset, dirty bool) {
	if s.vbIDRange.In(vbID) {
		if current, ok := s.offsets.Load(vbID); ok && current.SeqNo > offset.SeqNo {
			return
		}
		s.offsets.Store(vbID, offset)
		s.consumer.TrackOffset(vbID, offset)
		if !dirty {
			return
		}

		s.dirtyOffsets.StoreIf(vbID, func(p bool, f bool) (v bool, s bool) {
			if !f || (f && !p) {
				return true, true
			}

			return p, false
		})
	} else {
		logger.Log.Warn("vbID does not belong to our range", "vbID", vbID)
	}
}

func (s *stream) waitAndForward(
	payload interface{},
	spanCtx tracing.RequestSpanContext,
	offset *models.Offset,
	vbID uint16,
	eventTime time.Time,
) {
	if helpers.IsMetadata(payload) {
		s.setOffset(vbID, offset, false)
		return
	}

	s.metric.DcpLatency = time.Since(eventTime).Milliseconds()

	ctx := &models.ListenerContext{
		Commit: s.checkpoint.Save,
		Event:  payload,
		Ack: func() {
			s.setOffset(vbID, offset, true)
			s.anyDirtyOffset = true
		},
		ListenerTracerComponent: s.tracerComponent.NewListenerTracerComponent(spanCtx),
	}

	start := time.Now()

	s.consumer.ConsumeEvent(ctx)

	s.metric.ProcessLatency = time.Since(start).Milliseconds()
}

func (s *stream) listen(args models.ListenerArgs) {
	switch v := args.Event.(type) {
	case models.DcpMutation:
		s.waitAndForward(v, args.TraceContext, v.Offset, v.VbID, v.EventTime)
	case models.DcpDeletion:
		s.waitAndForward(v, args.TraceContext, v.Offset, v.VbID, v.EventTime)
	case models.DcpExpiration:
		s.waitAndForward(v, args.TraceContext, v.Offset, v.VbID, v.EventTime)
	case models.DcpSeqNoAdvanced:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpCollectionCreation:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpCollectionDeletion:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpCollectionFlush:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpScopeCreation:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpScopeDeletion:
		s.setOffset(v.VbID, v.Offset, true)
	case models.DcpCollectionModification:
		s.setOffset(v.VbID, v.Offset, true)
	default:
	}
}

func (s *stream) reopenStream(vbID uint16) {
	retry := 5

	for {
		err := s.openStream(vbID)
		if err == nil {
			logger.Log.Info("re-open stream", "vbID", vbID)
			break
		} else {
			logger.Log.Warn("cannot re-open stream", "vbID", vbID, "error", err)
		}

		retry--
		if retry == 0 {
			logger.Log.Error("error while re-open stream", "vbID", vbID, "error", err)
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func (s *stream) listenEnd(endContext models.DcpStreamEndContext, lifecycle *streamLifecycle) {
	if s.streamEndNotSupportedData != nil && s.streamEndNotSupportedData.ending {
		<-s.streamEndNotSupportedData.queue
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.closing {
		return
	}

	if !s.closeWithCancel && endContext.Err != nil {
		if !errors.Is(endContext.Err, gocbcore.ErrDCPStreamClosed) {
			logger.Log.Warn("end stream got error", "vbID", endContext.Event.VbID, "error", endContext.Err)
		} else {
			logger.Log.Debug("end stream got error", "vbID", endContext.Event.VbID, "error", endContext.Err)
		}
	}

	if endContext.Err == nil {
		logger.Log.Debug("end stream", "vbID", endContext.Event.VbID)
	}

	if endContext.Err != nil &&
		!errors.Is(endContext.Err, gocbcore.ErrDCPStreamClosed) {
		go s.reopenStream(endContext.Event.VbID)
	} else {
		activeStreams := lifecycle.activeStreams.Add(-1)
		s.activeStreams.Store(activeStreams)
		if activeStreams == 0 {
			lifecycle.finish(true)
		}
	}
}

func (s *stream) Open() {
	lifecycle := newStreamLifecycle()
	s.lifecycle = lifecycle

	s.eventHandler.BeforeStreamStart()

	vBuckets := s.client.GetNumVBuckets()
	vbIDs := s.vBucketDiscovery.Get()
	s.vbIDRange = &models.VbIDRange{
		Start: vbIDs[0],
		End:   vbIDs[len(vbIDs)-1],
	}

	lifecycle.activeStreams.Store(int32(len(vbIDs)))
	s.activeStreams.Store(int32(len(vbIDs)))

	latestSeqNoInitializer := offset.NewOffsetLatestSeqNoInit(s.config)

	s.checkpoint = NewCheckpoint(s, vbIDs, s.client, s.metadata, s.config, latestSeqNoInitializer)
	s.offsets, s.dirtyOffsets, s.anyDirtyOffset = s.checkpoint.Load()

	s.observers = wrapper.CreateConcurrentSwissMap[uint16, couchbase.Observer](uint64(vBuckets))
	s.offsets.Range(func(vbID uint16, offset *models.Offset) bool {
		s.observers.Store(
			vbID,
			couchbase.NewObserver(s.config,
				vbID, offset.LatestSeqNo, s.listen,
				func(ctx models.DcpStreamEndContext) { s.listenEnd(ctx, lifecycle) },
				s.collectionIDs, s.tracerComponent,
			),
		)

		return true
	})

	if !s.config.RollbackMitigation.Disabled {
		if s.bucketInfo.IsEphemeral() {
			logger.Log.Info("rollback mitigation is disabled for ephemeral bucket")
			s.config.RollbackMitigation.Disabled = true
		} else {
			s.rollbackMitigation = couchbase.NewRollbackMitigation(s.client, s.config, vbIDs, s.dispatchPersistSeqNo)
			s.rollbackMitigation.Start()
		}
	}

	s.openAllStreams(vbIDs)

	logger.Log.Info("stream started")
	s.eventHandler.AfterStreamStart()

	s.checkpoint.StartSchedule()

	go s.wait(lifecycle)
	s.open = true
}

func (s *stream) IsOpen() bool {
	return s.open
}

func (s *stream) Rebalance() {
	if s.balancing && s.rebalanceTimer != nil {
		// Is rebalance timer triggered already
		if s.rebalanceTimer.Stop() {
			s.rebalanceTimer.Reset(s.config.Dcp.Group.Membership.RebalanceDelay)
			logger.Log.Info("latest rebalance time is resetted")
		} else {
			s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.Rebalance)
			logger.Log.Info("latest rebalance time is reassigned")
		}
		return
	}
	logger.Log.Info("rebalance starting")
	s.rebalanceLock.Lock()
	s.lifecycle.mu.Lock()
	s.lifecycle.closing = true
	s.lifecycle.mu.Unlock()

	s.eventHandler.BeforeRebalanceStart()

	if !s.balancing {
		s.balancing = true
		s.Close(false)
	}

	s.eventHandler.AfterRebalanceStart()

	if s.config.Dcp.Group.Membership.Type == membership.DynamicMembershipType {
		s.rebalanceTimer = time.AfterFunc(0, s.rebalance)
		logger.Log.Info("rebalance delay is disabled on dynamic membership")
	} else {
		s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.rebalance)
		logger.Log.Info("rebalance will start", "delay", s.config.Dcp.Group.Membership.RebalanceDelay)
	}
}

func (s *stream) rebalance() {
	logger.Log.Info("reassigning vbuckets and opening stream is starting")

	defer s.rebalanceLock.Unlock()

	s.eventHandler.BeforeRebalanceEnd()
	s.Open()
	s.metric.Rebalance++

	logger.Log.Info("rebalance is finished")
	s.balancing = false
	s.eventHandler.AfterRebalanceEnd()
}

func (s *stream) Save() {
	s.checkpoint.Save()
}

func (s *stream) dispatchPersistSeqNo(persistSeqNo *models.PersistSeqNo) {
	if s.observers != nil {
		if observer, ok := s.observers.Load(persistSeqNo.VbID); ok {
			observer.SetPersistSeqNo(persistSeqNo.SeqNo)
		}
	}
}

func (s *stream) openStream(vbID uint16) error {
	offset, exist := s.offsets.Load(vbID)
	if !exist {
		err := fmt.Errorf("vbID %d not found on offset map", vbID)
		logger.Log.Error("error while opening stream", "vbID", vbID, "error", err)
		return err
	}
	observer, _ := s.observers.Load(vbID)
	return s.client.OpenStream(vbID, s.collectionIDs, offset, observer)
}

func (s *stream) openAllStreams(vbIDs []uint16) {
	openWg := &sync.WaitGroup{}
	openWg.Add(len(vbIDs))

	for _, vbID := range vbIDs {
		go func(innerVbId uint16) {
			err := s.openStream(innerVbId)
			if err != nil {
				logger.Log.Error("error while open stream", "vbID", innerVbId, "error", err)
				panic(err)
			}
			openWg.Done()
		}(vbID)
	}

	openWg.Wait()
}

func (s *stream) closeAllStreams() {
	// We need to do this without async when couchbase version below v5.5.0.
	// Because "gocbcore - memdopmap.go - FindOpenStream" is not thread safe.
	// BTW We cannot use ConcurrentSwissMap either. You know it's concurrent :/
	if s.streamEndNotSupportedData != nil {
		s.streamEndNotSupportedData.ending = true
		for vbID := s.vbIDRange.Start; vbID <= s.vbIDRange.End; vbID++ {
			s.streamEndNotSupportedData.queue <- struct{}{}
			if err := s.client.CloseStream(vbID); err != nil {
				logger.Log.Error(
					"cannot close stream on stream end not supporting mode",
					"vbID", vbID,
					"error", err,
				)
			}
		}
		s.streamEndNotSupportedData.ending = false
	} else {
		var wg sync.WaitGroup
		wg.Add(s.offsets.Count())
		s.offsets.Range(func(vbID uint16, _ *models.Offset) bool {
			go func(vbID uint16) {
				if err := s.client.CloseStream(vbID); err != nil {
					logger.Log.Error("cannot close stream", "vbID", vbID, "error", err)
				}

				wg.Done()
			}(vbID)

			return true
		})

		wg.Wait()
	}
}

func (s *stream) wait(lifecycle *streamLifecycle) {
	if <-lifecycle.done {
		close(s.stopCh)
	}
}

func (s *stream) Close(closeWithCancel bool) {
	vBuckets := s.client.GetNumVBuckets()
	lifecycle := s.lifecycle
	lifecycle.mu.Lock()
	lifecycle.closing = true
	lifecycle.mu.Unlock()

	s.closeWithCancel = closeWithCancel

	s.eventHandler.BeforeStreamStop()

	if !s.config.RollbackMitigation.Disabled {
		s.rollbackMitigation.Stop()
	}

	s.observers.Range(func(_ uint16, observer couchbase.Observer) bool {
		observer.Close()
		return true
	})

	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	s.closeAllStreams()

	s.observers.Range(func(_ uint16, observer couchbase.Observer) bool {
		observer.CloseEnd()
		return true
	})
	s.observers = nil

	s.offsets = wrapper.CreateConcurrentSwissMap[uint16, *models.Offset](uint64(vBuckets))
	s.dirtyOffsets = wrapper.CreateConcurrentSwissMap[uint16, bool](uint64(vBuckets))

	logger.Log.Info("stream stopped")
	s.eventHandler.AfterStreamStop()
	s.open = false
	lifecycle.activeStreams.Store(0)
	s.activeStreams.Store(0)

	lifecycle.finish(!s.balancing)
}

func (s *stream) GetOffsets() (*wrapper.ConcurrentSwissMap[uint16, *models.Offset], *wrapper.ConcurrentSwissMap[uint16, bool], bool) {
	return s.offsets, s.dirtyOffsets, s.anyDirtyOffset
}

func (s *stream) GetObservers() *wrapper.ConcurrentSwissMap[uint16, couchbase.Observer] {
	return s.observers
}

func (s *stream) GetMetric() (*Metric, int32) {
	return s.metric, s.activeStreams.Load()
}

func (s *stream) GetCheckpointMetric() *CheckpointMetric {
	return s.checkpoint.GetMetric()
}

func (s *stream) UnmarkDirtyOffsets() {
	vBuckets := s.client.GetNumVBuckets()
	s.anyDirtyOffset = false
	s.dirtyOffsets = wrapper.CreateConcurrentSwissMap[uint16, bool](uint64(vBuckets))
}

func NewStream(client couchbase.Client,
	metadata metadata.Metadata,
	config *config.Dcp,
	version *couchbase.Version,
	bucketInfo *couchbase.BucketInfo,
	vBucketDiscovery VBucketDiscovery,
	consumer models.Consumer,
	collectionIDs map[uint32]string,
	stopCh chan struct{},
	eventHandler models.EventHandler,
	tc *tracing.TracerComponent,
) Stream {
	stream := &stream{
		client:           client,
		metadata:         metadata,
		consumer:         consumer,
		config:           config,
		bucketInfo:       bucketInfo,
		vBucketDiscovery: vBucketDiscovery,
		collectionIDs:    collectionIDs,
		lifecycle:        newStreamLifecycle(),
		stopCh:           stopCh,
		eventHandler:     eventHandler,
		metric:           &Metric{},
		tracerComponent:  tc,
	}

	if version.Lower(couchbase.SrvVer550) {
		stream.streamEndNotSupportedData = &streamEndNotSupportedData{
			ending: false,
			queue:  make(chan struct{}, 1),
		}
	}

	return stream
}
