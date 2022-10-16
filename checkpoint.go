package main

import "github.com/couchbase/gocbcore/v10"

type Checkpoint interface {
	Save(groupName string)
	Load(groupName string) map[int]ObserverState
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

type checkpointDocument struct {
	Checkpoint checkpointDocumentCheckpoint `json:"checkpoint"`
	BucketUuid string                       `json:"bucketUuid"`
}

func NewCheckpointDocument() checkpointDocument {
	return checkpointDocument{
		Checkpoint: checkpointDocumentCheckpoint{
			VbUuid: 0,
			SeqNo:  0,
			Snapshot: checkpointDocumentSnapshot{
				StartSeqNo: 0,
				EndSeqNo:   0,
			},
		},
		BucketUuid: "",
	}
}

type checkpoint struct {
	observer      Observer
	vBucketNumber int
	failoverLogs  map[int]gocbcore.FailoverEntry
	metadata      Metadata
}

func (s *checkpoint) Save(groupName string) {
	state := s.observer.GetState()

	dump := map[int]checkpointDocument{}

	for vbId, observerState := range state {
		dump[vbId] = checkpointDocument{
			Checkpoint: checkpointDocumentCheckpoint{
				VbUuid: uint64(s.failoverLogs[vbId].VbUUID),
				SeqNo:  observerState.LastSeqNo,
				Snapshot: checkpointDocumentSnapshot{
					StartSeqNo: observerState.LastSnapStart,
					EndSeqNo:   observerState.LastSnapEnd,
				},
			},
			BucketUuid: "",
		}
	}

	s.metadata.Save(dump, groupName)
}

func (s *checkpoint) Load(groupName string) map[int]ObserverState {
	dump := s.metadata.Load(s.vBucketNumber, groupName)

	var observerState = map[int]ObserverState{}

	for vbId, doc := range dump {
		observerState[vbId] = ObserverState{
			LastSeqNo:     doc.Checkpoint.SeqNo,
			LastSnapStart: doc.Checkpoint.Snapshot.StartSeqNo,
			LastSnapEnd:   doc.Checkpoint.Snapshot.EndSeqNo,
		}
	}

	s.observer.SetState(observerState)

	return observerState
}

func NewCheckpoint(observer Observer, vBucketNumber int, failoverLogs map[int]gocbcore.FailoverEntry, metadata Metadata) Checkpoint {
	return &checkpoint{
		observer:      observer,
		vBucketNumber: vBucketNumber,
		failoverLogs:  failoverLogs,
		metadata:      metadata,
	}
}
