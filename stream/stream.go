package stream

import (
	"context"
	"sync"
	"time"

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
	Close()
	GetOffsets() (*wrapper.SyncMap[uint16, *models.Offset], *wrapper.SyncMap[uint16, bool], bool)
	GetObserver() couchbase.Observer
	GetMetric() *Metric
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
	bus                        helpers.Bus
	eventHandler               models.EventHandler
	stopCh                     chan struct{}
	finishStreamWithCloseCh    chan struct{}
	rebalanceTimer             *time.Timer
	dirtyOffsets               *wrapper.SyncMap[uint16, bool]
	listener                   models.Listener
	config                     *config.Dcp
	metric                     *Metric
	finishStreamWithEndEventCh chan struct{}
	collectionIDs              map[uint32]string
	offsets                    *wrapper.SyncMap[uint16, *models.Offset]
	activeStreams              int
	rebalanceLock              sync.Mutex
	anyDirtyOffset             bool
	balancing                  bool
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

func (s *stream) listenEnd() {
	for range s.observer.ListenEnd() {
		s.activeStreams--
		if s.activeStreams == 0 {
			s.finishStreamWithEndEventCh <- struct{}{}
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

	logger.Log.Printf("stream started")
	s.eventHandler.AfterStreamStart()

	s.checkpoint.StartSchedule()

	go s.wait()
}

func (s *stream) Rebalance() {
	if s.balancing && s.rebalanceTimer != nil {
		// Is rebalance timer triggered already
		if s.rebalanceTimer.Stop() {
			s.rebalanceTimer.Reset(s.config.Dcp.Group.Membership.RebalanceDelay)
			logger.Log.Printf("latest rebalance time is resetted")
		} else {
			s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.Rebalance)
			logger.Log.Printf("latest rebalance time is reassigned")
		}
		return
	}
	logger.Log.Printf("rebalance starting")
	s.rebalanceLock.Lock()

	s.eventHandler.BeforeRebalanceStart()

	if !s.balancing {
		s.balancing = true
		s.Save()
		s.Close()
	}

	s.eventHandler.AfterRebalanceStart()

	s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.rebalance)

	logger.Log.Printf("rebalance will start after %v", s.config.Dcp.Group.Membership.RebalanceDelay)
}

func (s *stream) rebalance() {
	logger.Log.Printf("reassigning vbuckets and opening stream is starting")

	defer s.rebalanceLock.Unlock()
	s.balancing = false

	s.eventHandler.BeforeRebalanceEnd()
	s.Open()
	s.metric.Rebalance++

	logger.Log.Printf("rebalance is finished")
	s.eventHandler.AfterRebalanceEnd()
}

func (s *stream) Save() {
	s.checkpoint.Save()
}

func (s *stream) openAllStreams(vbIds []uint16) {
	openWg := &sync.WaitGroup{}
	openWg.Add(len(vbIds))

	for _, vbID := range vbIds {
		go func(innerVbId uint16) {
			offset, _ := s.offsets.Load(innerVbId)
			err := s.client.OpenStream(innerVbId, s.collectionIDs, offset, s.observer)
			if err != nil {
				logger.ErrorLog.Printf("cannot open stream, vbID: %d, err: %v", innerVbId, err)
				panic(err)
			}

			openWg.Done()
		}(vbID)
	}

	openWg.Wait()
}

func (s *stream) closeAllStreams() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		var err error

		s.offsets.Range(func(vbID uint16, _ *models.Offset) bool {
			err = s.client.CloseStream(vbID)

			return true
		})

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

func (s *stream) Close() {
	s.eventHandler.BeforeStreamStop()

	if !s.config.RollbackMitigation.Disabled {
		s.rollbackMitigation.Stop()
	}

	s.observer.Close()

	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	err := s.closeAllStreams()
	if err != nil {
		logger.ErrorLog.Printf("cannot close all streams: %v", err)
	}

	s.finishStreamWithCloseCh <- struct{}{}
	s.observer.CloseEnd()
	s.observer = nil
	s.offsets = &wrapper.SyncMap[uint16, *models.Offset]{}
	s.dirtyOffsets = &wrapper.SyncMap[uint16, bool]{}

	logger.Log.Printf("stream stopped")
	s.eventHandler.AfterStreamStop()
}

func (s *stream) GetOffsets() (*wrapper.SyncMap[uint16, *models.Offset], *wrapper.SyncMap[uint16, bool], bool) {
	return s.offsets, s.dirtyOffsets, s.anyDirtyOffset
}

func (s *stream) GetObserver() couchbase.Observer {
	return s.observer
}

func (s *stream) GetMetric() *Metric {
	return s.metric
}

func (s *stream) GetCheckpointMetric() *CheckpointMetric {
	return s.checkpoint.GetMetric()
}

func (s *stream) UnmarkDirtyOffsets() {
	s.anyDirtyOffset = false
	s.dirtyOffsets = &wrapper.SyncMap[uint16, bool]{}
}

func NewStream(client couchbase.Client,
	metadata metadata.Metadata,
	config *config.Dcp,
	vBucketDiscovery VBucketDiscovery,
	listener models.Listener,
	collectionIDs map[uint32]string,
	stopCh chan struct{},
	bus helpers.Bus,
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
