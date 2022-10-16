package main

import (
	"github.com/couchbase/gocbcore/v10"
	"sync"
)

type Stream interface {
	Start()
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
	vbUuidMapLock   sync.Mutex
	vbUuidMap       map[uint16]uint64
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

func (s *stream) Start() {
	vBucketNumber, err := s.client.GetNumVBuckets()

	if err != nil {
		panic(err)
	}

	observer := NewObserverWithListener(vBucketNumber, s.Listener)

	seqNos, err := s.client.GetVBucketSeqNos()

	if err != nil {
		panic(err)
	}

	failoverLogs, err := s.client.GetFailoverLogs(vBucketNumber)

	s.finishedStreams.Add(len(seqNos))

	var openWg sync.WaitGroup
	openWg.Add(len(seqNos))

	if err != nil {
		panic(err)
	}

	s.checkpoint = NewCheckpoint(observer, vBucketNumber, failoverLogs, s.Metadata)
	observerState := s.checkpoint.Load(s.client.GetGroupName())

	for _, entry := range seqNos {
		go func(innerEntry gocbcore.VbSeqNoEntry) {
			ch := make(chan error)

			err := s.client.OpenStream(
				innerEntry.VbID,
				failoverLogs[int(innerEntry.VbID)].VbUUID,
				observerState[int(innerEntry.VbID)],
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

			s.streams = append(s.streams, innerEntry.VbID)

			openWg.Done()
		}(entry)
	}

	openWg.Wait()
}

func (s *stream) Wait() {
	s.finishedStreams.Wait()
	s.SaveCheckpoint()
}

func (s *stream) SaveCheckpoint() {
	s.checkpoint.Save(s.client.GetGroupName())
}

func (s *stream) Stop() {
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
}

func (s *stream) AddListener(listener Listener) {
	s.listeners = append(s.listeners, listener)
}

func NewStream(client Client, metadata Metadata) Stream {
	return &stream{
		client:          client,
		finishedStreams: sync.WaitGroup{},
		listeners:       []Listener{},
		Metadata:        metadata,
	}
}

func NewStreamWithListener(client Client, metadata Metadata, listener Listener) Stream {
	return &stream{
		client:          client,
		finishedStreams: sync.WaitGroup{},
		listeners:       []Listener{listener},
		Metadata:        metadata,
	}
}
