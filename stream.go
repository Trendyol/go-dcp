package godcpclient

import (
	"sync"
	"time"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"
	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
)

type Stream interface {
	Open()
	Rebalance()
	Wait()
	Save()
	Pause()
	Resume()
	Close(fromRebalance bool)
	LockOffsets()
	UnlockOffsets()
	GetOffsets() map[uint16]models.Offset
	LockObservers()
	UnlockObservers()
	GetObservers() map[uint16]gDcp.Observer
}

type stream struct {
	client           gDcp.Client
	Metadata         Metadata
	checkpoint       Checkpoint
	offsetsLock      sync.Mutex
	observersLock    sync.Mutex
	observers        map[uint16]gDcp.Observer
	vBucketDiscovery VBucketDiscovery
	streams          map[uint16]*uint16
	offsets          map[uint16]models.Offset
	rebalanceTimer   *time.Timer
	config           helpers.Config
	listener         models.Listener
	finishedStreams  sync.WaitGroup
	streamsLock      sync.Mutex
	mainListenerCh   models.ListenerCh
	collectionIDs    map[uint32]string
}

func (s *stream) waitAndForward(payload interface{}, offset models.Offset, vbID uint16) {
	ctx := &models.ListenerContext{
		Commit: s.checkpoint.Save,
		Status: models.Noop,
		Event:  payload,
	}

	s.listener(ctx)

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

func (s *stream) listenEnd(observer gDcp.Observer) {
	for end := range observer.ListenEnd() {
		s.CleanStreamOfVbID(end.VbID, false)
	}
}

func (s *stream) Open() {
	vbIds := s.vBucketDiscovery.Get()
	vBucketNumber := len(vbIds)

	uuIDMap, err := s.client.GetVBucketUUIDMap(vbIds)
	if err != nil {
		logger.Panic(err, "cannot get vbucket uuid map")
	}

	s.finishedStreams = sync.WaitGroup{}
	s.finishedStreams.Add(vBucketNumber)

	s.streams = make(map[uint16]*uint16)

	var openWg sync.WaitGroup
	openWg.Add(vBucketNumber)

	s.checkpoint = NewCheckpoint(s, vbIds, s.client.GetBucketUUID(), s.Metadata, s.config)
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

			go s.listenEnd(observer)

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

			s.streams[innerVbId] = &innerVbId
			s.observers[innerVbId] = observer

			openWg.Done()
		}(vbID)
	}
	openWg.Wait()
	go s.listen()
	logger.Debug("all streams are opened")
	s.checkpoint.StartSchedule()
}

func (s *stream) Rebalance() {
	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
	}

	s.rebalanceTimer = time.AfterFunc(time.Second*5, func() {
		s.Pause()
		s.Resume()
		logger.Debug("rebalance is finished")
	})
}

func (s *stream) Pause() {
	s.Save()
	s.Close(true)
}

func (s *stream) Resume() {
	s.Open()
}

func (s *stream) Wait() {
	s.finishedStreams.Wait()
	logger.Debug("all streams are finished")
}

func (s *stream) Save() {
	if s.checkpoint != nil {
		s.checkpoint.Save()
	}
}

func (s *stream) CleanStreamOfVbID(vbID uint16, ignoreFinish bool) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()

	delete(s.offsets, vbID)

	if s.streams[vbID] != nil {
		s.streams[vbID] = nil

		if !ignoreFinish {
			s.finishedStreams.Done()
		}
	}
}

func (s *stream) CloseWithVbID(vbID uint16, ignoreFinish bool) {
	ch := make(chan error)

	err := s.client.CloseStream(vbID, func(err error) {
		ch <- err
	})
	if err != nil {
		logger.Panic(err, "cannot close stream, vbID: %d", vbID)
	}

	if err = <-ch; err != nil {
		logger.Panic(err, "cannot close stream, vbID: %d", vbID)
	}

	s.CleanStreamOfVbID(vbID, ignoreFinish)
}

func (s *stream) Close(ignoreFinish bool) {
	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	for _, stream := range s.streams {
		if stream != nil {
			s.CloseWithVbID(*stream, ignoreFinish)
		}
	}

	for _, observer := range s.observers {
		observer.Close()
	}

	logger.Debug("all streams are closed")
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

func (s *stream) LockObservers() {
	s.observersLock.Lock()
}

func (s *stream) UnlockObservers() {
	s.observersLock.Unlock()
}

func (s *stream) GetObservers() map[uint16]gDcp.Observer {
	return s.observers
}

func NewStream(client gDcp.Client,
	metadata Metadata,
	config helpers.Config,
	vBucketDiscovery VBucketDiscovery,
	listener models.Listener,
	collectionIDs map[uint32]string,
) Stream {
	return &stream{
		client:           client,
		Metadata:         metadata,
		listener:         listener,
		config:           config,
		vBucketDiscovery: vBucketDiscovery,
		offsetsLock:      sync.Mutex{},
		observersLock:    sync.Mutex{},
		observers:        map[uint16]gDcp.Observer{},
		mainListenerCh:   make(models.ListenerCh, 1),
		collectionIDs:    collectionIDs,
	}
}
