package godcpclient

import (
	"context"
	"os"
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
	GetOffsets() map[uint16]models.Offset
	GetObservers() map[uint16]gDcp.Observer
	GetMetric() StreamMetric
}

type StreamMetric struct {
	AverageTookMs ewma.MovingAverage
}

type stream struct {
	client           gDcp.Client
	metadata         Metadata
	checkpoint       Checkpoint
	offsetsLock      sync.Mutex
	observers        map[uint16]gDcp.Observer
	vBucketDiscovery VBucketDiscovery
	offsets          map[uint16]models.Offset
	config           helpers.Config
	listener         models.Listener
	activeStreams    sync.WaitGroup
	streamsLock      sync.Mutex
	mainListenerCh   models.ListenerCh
	collectionIDs    map[uint32]string
	metric           StreamMetric
	rebalanceLock    sync.Mutex
	balancing        bool
	cancelCh         chan os.Signal
}

func (s *stream) waitAndForward(payload interface{}, offset models.Offset, vbID uint16) {
	ctx := &models.ListenerContext{
		Commit: s.checkpoint.Save,
		Status: models.Noop,
		Event:  payload,
	}

	start := time.Now()

	s.listener(ctx)

	s.metric.AverageTookMs.Add(float64(time.Since(start).Milliseconds()))

	if ctx.Status == models.Ack {
		s.LockOffsets()
		s.offsets[vbID] = offset
		s.UnlockOffsets()
	}
}

func (s *stream) listen() {
	for args := range s.mainListenerCh {
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

//nolint:funlen
func (s *stream) Open() {
	vbIds := s.vBucketDiscovery.Get()
	vBucketNumber := len(vbIds)

	uuIDMap, err := s.client.GetVBucketUUIDMap(vbIds)
	if err != nil {
		logger.Panic(err, "cannot get vbucket uuid map")
	}

	s.activeStreams.Add(vBucketNumber)

	var openWg sync.WaitGroup
	openWg.Add(vBucketNumber)

	s.checkpoint = NewCheckpoint(s, vbIds, s.client.GetBucketUUID(), s.metadata, s.config)
	s.offsets = s.checkpoint.Load()

	for _, vbID := range vbIds {
		go func(innerVbId uint16) {
			ch := make(chan error)

			observer := gDcp.NewObserver(s.collectionIDs, uuIDMap[innerVbId])

			go func() {
				for args := range observer.Listen() {
					s.mainListenerCh <- args
				}
			}()

			go func() {
				for range observer.ListenEnd() {
					s.activeStreams.Done()
				}
			}()

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

			s.observers[innerVbId] = observer

			openWg.Done()
		}(vbID)
	}
	openWg.Wait()
	go s.listen()
	logger.Debug("stream started")
	s.checkpoint.StartSchedule()

	go func() {
		s.activeStreams.Wait()
		if !s.balancing {
			close(s.cancelCh)
		}
	}()
}

func (s *stream) Rebalance() {
	s.rebalanceLock.Lock()
	defer s.rebalanceLock.Unlock()

	s.balancing = true

	s.Save()
	s.Close()
	s.Open()

	s.balancing = false

	logger.Debug("rebalance is finished")
}

func (s *stream) Save() {
	if s.checkpoint != nil {
		s.checkpoint.Save()
	}
}

func (s *stream) closeAllStreams() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)

	go func() {
		var err error

		for vbID := range s.observers {
			err = s.client.CloseStream(vbID)
		}

		errCh <- err
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *stream) Close() {
	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	err := s.closeAllStreams()
	if err != nil {
		logger.Error(err, "cannot close all streams")
	}

	s.activeStreams.Wait()

	for _, observer := range s.observers {
		observer.Close()
	}

	s.observers = map[uint16]gDcp.Observer{}
	s.offsets = map[uint16]models.Offset{}

	logger.Debug("stream stopped")
}

func (s *stream) LockOffsets() {
	s.offsetsLock.Lock()
}

func (s *stream) UnlockOffsets() {
	s.offsetsLock.Unlock()
}

func (s *stream) GetOffsets() map[uint16]models.Offset {
	return s.offsets
}

func (s *stream) GetObservers() map[uint16]gDcp.Observer {
	return s.observers
}

func (s *stream) GetMetric() StreamMetric {
	return s.metric
}

func NewStream(client gDcp.Client,
	metadata Metadata,
	config helpers.Config,
	vBucketDiscovery VBucketDiscovery,
	listener models.Listener,
	collectionIDs map[uint32]string,
	cancelCh chan os.Signal,
) Stream {
	return &stream{
		client:           client,
		metadata:         metadata,
		listener:         listener,
		config:           config,
		vBucketDiscovery: vBucketDiscovery,
		offsetsLock:      sync.Mutex{},
		observers:        map[uint16]gDcp.Observer{},
		mainListenerCh:   make(models.ListenerCh, 1),
		collectionIDs:    collectionIDs,
		rebalanceLock:    sync.Mutex{},
		activeStreams:    sync.WaitGroup{},
		cancelCh:         cancelCh,
		metric: StreamMetric{
			AverageTookMs: ewma.NewMovingAverage(10.0),
		},
	}
}
