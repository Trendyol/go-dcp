package godcpclient

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/couchbase/gocbcore/v10"
	"log"
	"sync"
	"time"
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
		s.CleanStreamOfVbId(end.VbID, false)
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

	s.checkpoint = NewCheckpoint(s.observer, vbIds, failoverLogs, vbSeqNos, s.client.GetBucketUuid(), s.Metadata, s.config)
	observerState := s.checkpoint.Load()

	for _, vbId := range vbIds {
		go func(innerVbId uint16) {
			ch := make(chan error)

			err := s.client.OpenStream(
				innerVbId,
				failoverLogs[innerVbId][0].VbUUID,
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
		}(vbId)
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

func (s *stream) CleanStreamOfVbId(vbId uint16, ignoreFinish bool) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()

	if s.streams[vbId] != nil {
		s.streams[vbId] = nil

		if !ignoreFinish {
			s.finishedStreams.Done()
		}
	}
}

func (s *stream) CloseWithVbId(vbId uint16, ignoreFinish bool) {
	ch := make(chan error)

	err := s.client.CloseStream(vbId, func(err error) {
		ch <- err
	})

	if err != nil {
		panic(err)
	}

	if err = <-ch; err != nil {
		panic(err)
	}

	s.CleanStreamOfVbId(vbId, ignoreFinish)
}

func (s *stream) Close(ignoreFinish bool) {
	if s.checkpoint != nil {
		s.checkpoint.StopSchedule()
	}

	for _, stream := range s.streams {
		if stream != nil {
			s.CloseWithVbId(*stream, ignoreFinish)
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
