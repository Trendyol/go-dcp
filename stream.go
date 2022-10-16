package main

import (
	"github.com/couchbase/gocbcore/v10"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Stream interface {
	Start() Stream
	Wait()
	SaveCheckpoint()
	Stop()
	AddListener(listener Listener)
}

type stream struct {
	client          Client
	finishedStreams sync.WaitGroup
	listeners       []Listener
	streamsLock     sync.Mutex
	streams         []uint16
	checkpoint      Checkpoint
	Metadata        Metadata
}

func (s *stream) Listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	if event == EndName {
		s.finishedStreams.Done()
	}

	if IsMetadata(data) {
		return
	}

	for _, listener := range s.listeners {
		listener(event, data, err)
	}
}

func (s *stream) Start() Stream {
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
	s.checkpoint.StartSchedule(s.client.GetGroupName())
	log.Printf("All streams are opened")

	return s
}

func (s *stream) Wait() {
	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s.finishedStreams.Wait()
		close(cancelCh)
	}()
	<-cancelCh
	s.Stop()
	s.checkpoint.StopSchedule()
	s.SaveCheckpoint()
}

func (s *stream) SaveCheckpoint() {
	s.checkpoint.Save(s.client.GetGroupName())
}

func (s *stream) Stop() {
	if len(s.streams) == 0 {
		return
	}

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
	s.streams = nil
	log.Printf("All streams are closed")
}

func (s *stream) AddListener(listener Listener) {
	s.listeners = append(s.listeners, listener)
}

func NewStream(client Client, metadata Metadata, listeners ...Listener) Stream {
	return &stream{
		client:          client,
		finishedStreams: sync.WaitGroup{},
		listeners:       listeners,
		Metadata:        metadata,
	}
}
