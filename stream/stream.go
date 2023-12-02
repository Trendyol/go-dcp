package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/asaskevich/EventBus"

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
	GetObserver() couchbase.Observer
	GetMetric() (*Metric, int)
	UnmarkDirtyOffsets()
	GetCheckpointMetric() *CheckpointMetric
}

type Metric struct {
	ProcessLatency int64
	DcpLatency     int64
	Rebalance      int
}

type stream struct {
	client                     couchbase.Client
	metadata                   metadata.Metadata
	checkpoint                 Checkpoint
	rollbackMitigation         couchbase.RollbackMitigation
	observer                   couchbase.Observer
	vBucketDiscovery           VBucketDiscovery
	bus                        EventBus.Bus
	eventHandler               models.EventHandler
	stopCh                     chan struct{}
	finishStreamWithCloseCh    chan struct{}
	rebalanceTimer             *time.Timer
	dirtyOffsets               *wrapper.ConcurrentSwissMap[uint16, bool]
	listener                   models.Listener
	config                     *config.Dcp
	metric                     *Metric
	finishStreamWithEndEventCh chan struct{}
	collectionIDs              map[uint32]string
	offsets                    *wrapper.ConcurrentSwissMap[uint16, *models.Offset]
	activeStreams              int
	rebalanceLock              sync.Mutex
	anyDirtyOffset             bool
	balancing                  bool
	closeWithCancel            bool
}

func (s *stream) setOffset(vbID uint16, offset *models.Offset, dirty bool) {
	s.offsets.Store(vbID, offset)
	s.dirtyOffsets.Store(vbID, dirty)
}

func (s *stream) waitAndForward(payload interface{}, offset *models.Offset, vbID uint16, eventTime time.Time) {
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
	}

	start := time.Now()

	s.listener(ctx)

	s.metric.ProcessLatency = time.Since(start).Milliseconds()
}

func (s *stream) listen() {
	for args := range s.observer.Listen() {
		event := args.Event

		switch v := event.(type) {
		case models.DcpMutation:
			s.waitAndForward(v, v.Offset, v.VbID, v.EventTime)
		case models.DcpDeletion:
			s.waitAndForward(v, v.Offset, v.VbID, v.EventTime)
		case models.DcpExpiration:
			s.waitAndForward(v, v.Offset, v.VbID, v.EventTime)
		case models.DcpSeqNoAdvanced:
			s.setOffset(v.VbID, v.Offset, true)
		default:
		}
	}
}

func (s *stream) reopenStream(vbID uint16) {
	go func(innerVbID uint16) {
		for {
			err := s.openStream(innerVbID)
			if err == nil {
				logger.Log.Info("re-open stream, vbID: %d", innerVbID)
				break
			} else {
				logger.Log.Error("cannot re-open stream, vbID: %d, err: %v", innerVbID, err)
			}

			time.Sleep(time.Second)
		}
	}(vbID)
}

func (s *stream) listenEnd() {
	for endContext := range s.observer.ListenEnd() {
		if !s.closeWithCancel && endContext.Err != nil {
			if !errors.Is(endContext.Err, gocbcore.ErrDCPStreamClosed) {
				logger.Log.Error("end stream vbId: %v got error: %v", endContext.Event.VbID, endContext.Err)
			} else {
				logger.Log.Debug("end stream vbId: %v got error: %v", endContext.Event.VbID, endContext.Err)
			}
		}

		if endContext.Err == nil {
			logger.Log.Debug("end stream vbId: %v", endContext.Event.VbID)
		}

		if !s.closeWithCancel && endContext.Err != nil &&
			(errors.Is(endContext.Err, gocbcore.ErrSocketClosed) ||
				errors.Is(endContext.Err, gocbcore.ErrDCPBackfillFailed)) {
			s.reopenStream(endContext.Event.VbID)
		} else {
			s.activeStreams--
			if s.activeStreams == 0 {
				s.finishStreamWithEndEventCh <- struct{}{}
			}
		}
	}
}

func (s *stream) Open() {
	s.eventHandler.BeforeStreamStart()

	vbIds := s.vBucketDiscovery.Get()

	if !s.config.RollbackMitigation.Disabled {
		s.rollbackMitigation = couchbase.NewRollbackMitigation(s.client, s.config, vbIds, s.bus)
		s.rollbackMitigation.Start()
	}

	s.activeStreams = len(vbIds)

	s.checkpoint = NewCheckpoint(s, vbIds, s.client, s.metadata, s.config)
	s.offsets, s.dirtyOffsets, s.anyDirtyOffset = s.checkpoint.Load()
	s.observer = couchbase.NewObserver(s.config, s.collectionIDs, s.bus)

	s.openAllStreams(vbIds)

	go s.listenEnd()
	go s.listen()

	logger.Log.Info("stream started")
	s.eventHandler.AfterStreamStart()

	s.checkpoint.StartSchedule()

	go s.wait()
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

	s.eventHandler.BeforeRebalanceStart()

	if !s.balancing {
		s.balancing = true
		s.Save()
		s.Close(false)
	}

	s.eventHandler.AfterRebalanceStart()

	s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.rebalance)

	logger.Log.Info("rebalance will start after %v", s.config.Dcp.Group.Membership.RebalanceDelay)
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

func (s *stream) openStream(vbID uint16) error {
	offset, exist := s.offsets.Load(vbID)
	if !exist {
		err := fmt.Errorf("vbID: %d not found on offset map", vbID)
		logger.Log.Error("error while opening stream, err: %v", err)
		return err
	}
	return s.client.OpenStream(vbID, s.collectionIDs, offset, s.observer)
}

func (s *stream) openAllStreams(vbIds []uint16) {
	openWg := &sync.WaitGroup{}
	openWg.Add(len(vbIds))

	for _, vbID := range vbIds {
		go func(innerVbId uint16) {
			err := s.openStream(innerVbId)
			if err != nil {
				logger.Log.Error("cannot open stream, vbID: %d, err: %v", innerVbId, err)
				panic(err)
			}
			openWg.Done()
		}(vbID)
	}

	openWg.Wait()
}

func (s *stream) closeAllStreams(internal bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		var err error

		var wg sync.WaitGroup
		wg.Add(s.offsets.Count())
		s.offsets.Range(func(vbID uint16, _ *models.Offset) bool {
			go func(vbID uint16) {
				defer wg.Done()
				if internal {
					s.observer.End(models.DcpStreamEnd{VbID: vbID}, nil)
				} else {
					err = s.client.CloseStream(vbID)
				}
			}(vbID)
			return true
		})
		wg.Wait()
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *stream) wait() {
	select {
	case <-s.finishStreamWithCloseCh:
	case <-s.finishStreamWithEndEventCh:
	}

	if !s.balancing {
		close(s.stopCh)
	}
}

func (s *stream) Close(closeWithCancel bool) {
	s.closeWithCancel = closeWithCancel

	s.eventHandler.BeforeStreamStop()

	if !s.config.RollbackMitigation.Disabled {
		s.rollbackMitigation.Stop()
	}

	s.observer.Close()

	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	err := s.closeAllStreams(s.config.Dcp.Config.DisableStreamEndByClient)
	if err != nil {
		logger.Log.Error("cannot close all streams: %v", err)
	}

	s.finishStreamWithCloseCh <- struct{}{}
	s.observer.CloseEnd()
	s.observer = nil

	s.offsets = wrapper.CreateConcurrentSwissMap[uint16, *models.Offset](1024)
	s.dirtyOffsets = wrapper.CreateConcurrentSwissMap[uint16, bool](1024)

	logger.Log.Info("stream stopped")
	s.eventHandler.AfterStreamStop()
}

func (s *stream) GetOffsets() (*wrapper.ConcurrentSwissMap[uint16, *models.Offset], *wrapper.ConcurrentSwissMap[uint16, bool], bool) {
	return s.offsets, s.dirtyOffsets, s.anyDirtyOffset
}

func (s *stream) GetObserver() couchbase.Observer {
	return s.observer
}

func (s *stream) GetMetric() (*Metric, int) {
	return s.metric, s.activeStreams
}

func (s *stream) GetCheckpointMetric() *CheckpointMetric {
	return s.checkpoint.GetMetric()
}

func (s *stream) UnmarkDirtyOffsets() {
	s.anyDirtyOffset = false
	s.dirtyOffsets = wrapper.CreateConcurrentSwissMap[uint16, bool](1024)
}

func NewStream(client couchbase.Client,
	metadata metadata.Metadata,
	config *config.Dcp,
	vBucketDiscovery VBucketDiscovery,
	listener models.Listener,
	collectionIDs map[uint32]string,
	stopCh chan struct{},
	bus EventBus.Bus,
	eventHandler models.EventHandler,
) Stream {
	return &stream{
		client:                     client,
		metadata:                   metadata,
		listener:                   listener,
		config:                     config,
		vBucketDiscovery:           vBucketDiscovery,
		collectionIDs:              collectionIDs,
		finishStreamWithCloseCh:    make(chan struct{}, 1),
		finishStreamWithEndEventCh: make(chan struct{}, 1),
		stopCh:                     stopCh,
		bus:                        bus,
		eventHandler:               eventHandler,
		metric:                     &Metric{},
	}
}
