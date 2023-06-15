package stream

import (
	"context"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-client/wrapper"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/Trendyol/go-dcp-client/metadata"

	"github.com/VividCortex/ewma"

	"github.com/Trendyol/go-dcp-client/couchbase"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
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
	ProcessLatency ewma.MovingAverage
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
	offsets                    *wrapper.SyncMap[uint16, *models.Offset]
	rebalanceTimer             *time.Timer
	finishStreamWithEndEventCh chan struct{}
	stopCh                     chan struct{}
	dirtyOffsets               *wrapper.SyncMap[uint16, bool]
	listener                   models.Listener
	finishListenerBuffer       chan struct{}
	config                     *config.Dcp
	metric                     *Metric
	finishStreamWithCloseCh    chan struct{}
	collectionIDs              map[uint32]string
	activeStreams              int
	anyDirtyOffset             bool
	balancing                  bool
	rebalanceLock              sync.Mutex
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

	s.metric.ProcessLatency.Add(float64(time.Since(start).Milliseconds()))
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
	close(s.finishListenerBuffer)
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

	s.checkpoint.StartSchedule()

	go s.wait()
}

func (s *stream) Rebalance() {
	if s.balancing {
		s.rebalanceTimer.Reset(s.config.Dcp.Group.Membership.RebalanceDelay)
		logger.Log.Printf("latest rebalance time is resetted")
		return
	}
	s.rebalanceLock.Lock()
	if !s.balancing {
		s.balancing = true
		s.Save()
		s.Close()
	}

	logger.Log.Printf("rebalance will start after %v", s.config.Dcp.Group.Membership.RebalanceDelay)

	s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, func() {
		defer s.rebalanceLock.Unlock()
		s.Open()

		s.balancing = false

		logger.Log.Printf("rebalance is finished")

		s.metric.Rebalance++
	})
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
	if !s.config.RollbackMitigation.Disabled {
		s.rollbackMitigation.Stop()
	}

	err := s.closeAllStreams()
	if err != nil {
		logger.ErrorLog.Printf("cannot close all streams: %v", err)
	}

	s.observer.Close()
	<-s.finishListenerBuffer

	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	s.finishStreamWithCloseCh <- struct{}{}
	s.observer.CloseEnd()
	s.observer = nil
	s.offsets = &wrapper.SyncMap[uint16, *models.Offset]{}
	s.dirtyOffsets = &wrapper.SyncMap[uint16, bool]{}

	logger.Log.Printf("stream stopped")
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
		finishListenerBuffer:       make(chan struct{}),
		stopCh:                     stopCh,
		bus:                        bus,
		metric: &Metric{
			ProcessLatency: ewma.NewMovingAverage(config.Metric.AverageWindowSec),
		},
	}
}
