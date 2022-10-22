package godcpclient

import (
	"github.com/couchbase/gocbcore/v10"
	"log"
	"sync"
)

type Stream interface {
	Start()
	Wait()
	Save()
	Stop()
}

type stream struct {
	client     Client
	Metadata   Metadata
	checkpoint Checkpoint

	finishedStreams sync.WaitGroup

	listeners []Listener

	streamsLock sync.Mutex
	streams     []uint16
}

func (s *stream) Listener(event interface{}, err error) {
	if err != nil {
		return
	}

	if _, ok := event.(DcpStreamEnd); ok {
		s.finishedStreams.Done()
	}

	if IsMetadata(event) {
		return
	}

	for _, listener := range s.listeners {
		listener(event, err)
	}
}

func (s *stream) Start() {
	vbIds := s.client.GetMembership().GetVBuckets()
	vBucketNumber := len(vbIds)

	observer := NewObserver(vbIds, s.Listener)

	failoverLogs, err := s.client.GetFailoverLogs(vbIds)

	if err != nil {
		panic(err)
	}

	s.finishedStreams.Add(vBucketNumber)

	var openWg sync.WaitGroup
	openWg.Add(vBucketNumber)

	s.checkpoint = NewCheckpoint(observer, vbIds, failoverLogs, s.client.GetBucketUuid(), s.Metadata)
	observerState := s.checkpoint.Load(s.client.GetGroupName())

	for _, vbId := range vbIds {
		go func(innerVbId uint16) {
			ch := make(chan error)

			err := s.client.OpenStream(
				innerVbId,
				failoverLogs[innerVbId].VbUUID,
				observerState[innerVbId],
				observer,
				func(entries []gocbcore.FailoverEntry, err error) {
					ch <- err
				},
			)

			err = <-ch

			if err != nil {
				panic(err)
			}

			s.streamsLock.Lock()
			defer s.streamsLock.Unlock()

			s.streams = append(s.streams, innerVbId)

			openWg.Done()
		}(vbId)
	}

	openWg.Wait()
	log.Printf("All streams are opened")

	s.checkpoint.StartSchedule(s.client.GetGroupName())
}

func (s *stream) Wait() {
	s.finishedStreams.Wait()
}

func (s *stream) Save() {
	s.checkpoint.Save(s.client.GetGroupName())
}

func (s *stream) Stop() {
	s.checkpoint.StopSchedule()

	for _, stream := range s.streams {
		ch := make(chan error)

		err := s.client.CloseStream(stream, func(err error) {
			ch <- err
		})

		err = <-ch

		if err != nil {
			panic(err)
		}

		s.finishedStreams.Done()
	}

	s.streams = []uint16{}
	log.Printf("All streams are closed")
}

func NewStream(client Client, metadata Metadata, listeners ...Listener) Stream {
	return &stream{
		client:   client,
		Metadata: metadata,

		finishedStreams: sync.WaitGroup{},

		listeners: listeners,
	}
}
