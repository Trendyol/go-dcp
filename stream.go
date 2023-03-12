package godcpclient

import (
	"context"
	"sync"
	"time"

	"github.com/VividCortex/ewma"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
)

type Stream interface {
	Open()
	Rebalance()
	Save()
	Close()
	LockOffsets()
	UnlockOffsets()
	GetOffsetsWithDirty() (map[uint16]*models.Offset, bool)
	GetObserver() gDcp.Observer
	GetMetric() StreamMetric
	UnmarkDirty()
}

type StreamMetric struct {
	AverageProcessMs ewma.MovingAverage
}

type stream struct {
	client           gDcp.Client
	metadata         Metadata
	checkpoint       Checkpoint
	metric           StreamMetric
	observer         gDcp.Observer
	vBucketDiscovery VBucketDiscovery
	collectionIDs    map[uint32]string
	listener         models.Listener
	offsets          map[uint16]*models.Offset
	stopCh           chan struct{}
	config           *helpers.Config
	activeStreams    *sync.WaitGroup
	streamsLock      *sync.Mutex
	offsetsLock      *sync.Mutex
	dirty            bool
	balancing        bool
	rebalanceTimer   *time.Timer
}

func (s *stream) setOffset(vbID uint16, offset *models.Offset) {
	s.LockOffsets()
	defer s.UnlockOffsets()
	s.offsets[vbID] = offset
}

func (s *stream) waitAndForward(payload interface{}, offset *models.Offset, vbID uint16) {
	ctx := &models.ListenerContext{
		Commit: s.checkpoint.Save,
		Event:  payload,
		Ack: func() {
			s.setOffset(vbID, offset)
			s.dirty = true
		},
	}

	if helpers.IsMetadata(payload) {
		s.setOffset(vbID, offset)
	} else {
		start := time.Now()

		s.listener(ctx)

		s.metric.AverageProcessMs.Add(float64(time.Since(start).Milliseconds()))
	}
}

func (s *stream) listen() {
	for args := range s.observer.Listen() {
		event := args.Event

		switch v := event.(type) {
		case models.DcpMutation:
			s.waitAndForward(v, v.Offset, v.VbID)
		case models.DcpDeletion:
			s.waitAndForward(v, v.Offset, v.VbID)
		case models.DcpExpiration:
			s.waitAndForward(v, v.Offset, v.VbID)
		default:
		}
	}
}

func (s *stream) listenEnd() {
	for range s.observer.ListenEnd() {
		s.activeStreams.Done()
	}
}

func (s *stream) Open() {
	vbIds := s.vBucketDiscovery.Get()
	vBucketNumber := len(vbIds)

	uuIDMap, err := s.client.GetVBucketUUIDMap(vbIds)
	if err != nil {
		logger.Panic(err, "cannot get vbucket uuid map")
	}

	s.activeStreams.Add(vBucketNumber)

	openWg := &sync.WaitGroup{}
	openWg.Add(vBucketNumber)

	s.checkpoint = NewCheckpoint(s, vbIds, s.client.GetBucketUUID(), s.metadata, s.config)
	s.offsets = s.checkpoint.Load()

	observer := gDcp.NewObserver(s.config, s.collectionIDs, uuIDMap)

	for _, vbID := range vbIds {
		go func(innerVbId uint16) {
			ch := make(chan error)

			err := s.client.OpenStream(
				innerVbId,
				uuIDMap[innerVbId],
				s.collectionIDs,
				s.offsets[innerVbId],
				observer,
				func(entries []gocbcore.FailoverEntry, err error) {
					ch <- err
				},
			)
			if err != nil {
				logger.Panic(err, "cannot open stream, vbID: %d", innerVbId)
			}

			if err = <-ch; err != nil {
				logger.Panic(err, "cannot open stream, vbID: %d", innerVbId)
			}

			s.streamsLock.Lock()
			defer s.streamsLock.Unlock()

			openWg.Done()
		}(vbID)
	}
	openWg.Wait()

	s.observer = observer
	go s.listenEnd()
	go s.listen()

	logger.Debug("stream started")

	s.checkpoint.StartSchedule()

	go s.wait()
}

func (s *stream) rebalance() {
	s.balancing = true

	s.Save()
	s.Close()
	s.Open()

	s.balancing = false

	logger.Debug("rebalance is finished")
}

func (s *stream) Rebalance() {
	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
		logger.Debug("latest rebalance is canceled")
	}

	s.rebalanceTimer = time.AfterFunc(s.config.Dcp.Group.Membership.RebalanceDelay, s.rebalance)

	logger.Debug("rebalance will be started in %v", s.config.Dcp.Group.Membership.RebalanceDelay)
}

func (s *stream) Save() {
	s.checkpoint.Save()
}

func (s *stream) closeAllStreams() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		var err error

		s.LockOffsets()
		for vbID := range s.offsets {
			err = s.client.CloseStream(vbID)
		}
		s.UnlockOffsets()

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
	s.activeStreams.Wait()
	if !s.balancing {
		close(s.stopCh)
	}
}

func (s *stream) Close() {
	s.observer.Close()

	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	err := s.closeAllStreams()
	if err != nil {
		logger.Error(err, "cannot close all streams")
	}

	s.activeStreams.Wait()
	s.observer.CloseEnd()
	s.observer = nil
	s.offsets = map[uint16]*models.Offset{}

	logger.Debug("stream stopped")
}

func (s *stream) LockOffsets() {
	s.offsetsLock.Lock()
}

func (s *stream) UnlockOffsets() {
	s.offsetsLock.Unlock()
}

func (s *stream) GetOffsetsWithDirty() (map[uint16]*models.Offset, bool) {
	return s.offsets, s.dirty
}

func (s *stream) GetObserver() gDcp.Observer {
	return s.observer
}

func (s *stream) GetMetric() StreamMetric {
	return s.metric
}

func (s *stream) UnmarkDirty() {
	s.dirty = false
}

func NewStream(client gDcp.Client,
	metadata Metadata,
	config *helpers.Config,
	vBucketDiscovery VBucketDiscovery,
	listener models.Listener,
	collectionIDs map[uint32]string,
	stopCh chan struct{},
) Stream {
	return &stream{
		client:           client,
		metadata:         metadata,
		listener:         listener,
		config:           config,
		vBucketDiscovery: vBucketDiscovery,
		offsetsLock:      &sync.Mutex{},
		streamsLock:      &sync.Mutex{},
		collectionIDs:    collectionIDs,
		activeStreams:    &sync.WaitGroup{},
		stopCh:           stopCh,
		metric: StreamMetric{
			AverageProcessMs: ewma.NewMovingAverage(config.Metric.AverageWindowSec),
		},
	}
}
