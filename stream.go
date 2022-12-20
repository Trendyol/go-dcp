package godcpclient

import (
	"log"
	"sync"
	"time"

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
	GetObserver() Observer
}

type stream struct {
	client     Client
	Metadata   Metadata
	checkpoint Checkpoint

	finishedStreams sync.WaitGroup

	listeners []Listener

	streamsLock sync.Mutex
	streams     map[uint16]*uint16

	config helpers.Config

	observer Observer

	vBucketDiscovery VBucketDiscovery

	rebalanceTimer *time.Timer
}

func (s *stream) listener(event interface{}, err error) {
	if end, ok := event.(DcpStreamEnd); ok {
		s.CleanStreamOfVbID(end.VbID, false)
	}

	if err != nil {
		return
	}

	if helpers.IsMetadata(event) {
		return
	}

	for _, listener := range s.listeners {
		listener(event, err)
	}
}

func (s *stream) Open() {
	var collectionID *uint32

	if s.client.IsCollectionModeEnabled() {
		id, err := s.client.GetCollectionID(s.config.ScopeName, s.config.CollectionName)
		if err != nil {
			panic(err)
		}

		collectionID = &id
	}

	vbIds := s.vBucketDiscovery.Get()
	vBucketNumber := len(vbIds)

	s.observer = NewObserver(vbIds, s.listener)

	failoverLogs, err := s.client.GetFailoverLogs(vbIds)
	if err != nil {
		panic(err)
	}

	vbSeqNos, err := s.client.GetVBucketSeqNos()
	if err != nil {
		panic(err)
	}

	s.finishedStreams = sync.WaitGroup{}
	s.finishedStreams.Add(vBucketNumber)

	s.streams = make(map[uint16]*uint16)

	var openWg sync.WaitGroup
	openWg.Add(vBucketNumber)

	s.checkpoint = NewCheckpoint(s.observer, vbIds, failoverLogs, vbSeqNos, s.client.GetBucketUUID(), s.Metadata, s.config)
	observerState := s.checkpoint.Load()

	for _, vbID := range vbIds {
		go func(innerVbId uint16) {
			ch := make(chan error)

			err := s.client.OpenStream(
				innerVbId,
				failoverLogs[innerVbId][0].VbUUID,
				collectionID,
				observerState[innerVbId],
				s.observer,
				func(entries []gocbcore.FailoverEntry, err error) {
					ch <- err
				},
			)
			if err != nil {
				panic(err)
			}

			if err = <-ch; err != nil {
				panic(err)
			}

			s.streamsLock.Lock()
			defer s.streamsLock.Unlock()

			s.streams[innerVbId] = &innerVbId

			openWg.Done()
		}(vbID)
	}

	openWg.Wait()
	log.Printf("all streams are opened")

	s.checkpoint.StartSchedule()
}

func (s *stream) Rebalance() {
	if s.rebalanceTimer != nil {
		s.rebalanceTimer.Stop()
	}

	s.rebalanceTimer = time.AfterFunc(time.Second*10, func() {
		s.Pause()
		s.Resume()
		log.Printf("rebalance is finished")
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
	log.Printf("all streams are finished")
}

func (s *stream) Save() {
	if s.checkpoint != nil {
		s.checkpoint.Save(false)
	}
}

func (s *stream) CleanStreamOfVbID(vbID uint16, ignoreFinish bool) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()

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
		panic(err)
	}

	if err = <-ch; err != nil {
		panic(err)
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

	log.Printf("all streams are closed")
}

func (s *stream) GetObserver() Observer {
	return s.observer
}

func NewStream(client Client, metadata Metadata, config helpers.Config, vBucketDiscovery VBucketDiscovery, listeners ...Listener) Stream {
	return &stream{
		client:           client,
		Metadata:         metadata,
		listeners:        listeners,
		config:           config,
		vBucketDiscovery: vBucketDiscovery,
	}
}
