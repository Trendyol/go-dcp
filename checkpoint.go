package main

import (
	"github.com/couchbase/gocbcore/v10"
	"log"
	"sync"
	"time"
)

type Checkpoint interface {
	Save(groupName string)
	Load(groupName string) map[uint16]*ObserverState
	Clear(groupName string)
	StartSchedule(groupName string)
	StopSchedule()
}

type checkpointDocumentSnapshot struct {
	StartSeqNo uint64 `json:"startSeqno"`
	EndSeqNo   uint64 `json:"endSeqno"`
}

type checkpointDocumentCheckpoint struct {
	VbUuid   uint64                     `json:"vbuuid"`
	SeqNo    uint64                     `json:"seqno"`
	Snapshot checkpointDocumentSnapshot `json:"snapshot"`
}

type CheckpointDocument struct {
	Checkpoint checkpointDocumentCheckpoint `json:"checkpoint"`
	BucketUuid string                       `json:"bucketUuid"`
}

func NewCheckpointDocument(bucketUuid string) CheckpointDocument {
	return CheckpointDocument{
		Checkpoint: checkpointDocumentCheckpoint{
			VbUuid: 0,
			SeqNo:  0,
			Snapshot: checkpointDocumentSnapshot{
				StartSeqNo: 0,
				EndSeqNo:   0,
			},
		},
		BucketUuid: bucketUuid,
	}
}

type checkpoint struct {
	observer     Observer
	vbIds        []uint16
	failoverLogs map[uint16]gocbcore.FailoverEntry
	metadata     Metadata
	bucketUuid   string
	saveLock     sync.Mutex
	loadLock     sync.Mutex
	schedule     *time.Ticker
}

func (s *checkpoint) Save(groupName string) {
	s.saveLock.Lock()
	defer s.saveLock.Unlock()

	state := s.observer.GetState()

	dump := map[uint16]CheckpointDocument{}

	for vbId, observerState := range state {
		dump[vbId] = CheckpointDocument{
			Checkpoint: checkpointDocumentCheckpoint{
				VbUuid: uint64(s.failoverLogs[vbId].VbUUID),
				SeqNo:  observerState.SeqNo,
				Snapshot: checkpointDocumentSnapshot{
					StartSeqNo: observerState.StartSeqNo,
					EndSeqNo:   observerState.EndSeqNo,
				},
			},
			BucketUuid: s.bucketUuid,
		}
	}

	s.metadata.Save(dump, groupName, s.bucketUuid)
	log.Printf("Saved checkpoint")
}

func (s *checkpoint) Load(groupName string) map[uint16]*ObserverState {
	s.loadLock.Lock()
	defer s.loadLock.Unlock()

	dump := s.metadata.Load(s.vbIds, groupName, s.bucketUuid)

	var observerState = map[uint16]*ObserverState{}

	for vbId, doc := range dump {
		observerState[vbId] = &ObserverState{
			SeqNo:      doc.Checkpoint.SeqNo,
			StartSeqNo: doc.Checkpoint.Snapshot.StartSeqNo,
			EndSeqNo:   doc.Checkpoint.Snapshot.EndSeqNo,
		}
	}

	s.observer.SetState(observerState)
	log.Printf("Loaded checkpoint")

	return observerState
}

func (s *checkpoint) Clear(groupName string) {
	s.metadata.Clear(s.vbIds, groupName)
	log.Printf("Cleared checkpoint")
}

func (s *checkpoint) StartSchedule(groupName string) {
	go func() {
		s.schedule = time.NewTicker(10 * time.Second)
		go func() {
			time.Sleep(10 * time.Second)
			for range s.schedule.C {
				s.Save(groupName)
			}
		}()
	}()
	log.Printf("Started checkpoint schedule")
}

func (s *checkpoint) StopSchedule() {
	s.schedule.Stop()
	log.Printf("Stopped checkpoint schedule")
}

func NewCheckpoint(observer Observer, vbIds []uint16, failoverLogs map[uint16]gocbcore.FailoverEntry, bucketUuid string, metadata Metadata) Checkpoint {
	return &checkpoint{
		observer:     observer,
		vbIds:        vbIds,
		failoverLogs: failoverLogs,
		bucketUuid:   bucketUuid,
		metadata:     metadata,
	}
}
