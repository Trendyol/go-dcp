package main

import "github.com/couchbase/gocbcore/v10"

type Checkpoint interface {
	Save(groupName string)
	Load(groupName string) map[uint16]ObserverState
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
}

func (s *checkpoint) Save(groupName string) {
	state := s.observer.GetState()

	dump := map[uint16]CheckpointDocument{}

	for vbId, observerState := range state {
		dump[vbId] = CheckpointDocument{
			Checkpoint: checkpointDocumentCheckpoint{
				VbUuid: uint64(s.failoverLogs[vbId].VbUUID),
				SeqNo:  observerState.LastSeqNo,
				Snapshot: checkpointDocumentSnapshot{
					StartSeqNo: observerState.LastSnapStart,
					EndSeqNo:   observerState.LastSnapEnd,
				},
			},
			BucketUuid: s.bucketUuid,
		}
	}

	s.metadata.Save(dump, groupName, s.bucketUuid)
}

func (s *checkpoint) Load(groupName string) map[uint16]ObserverState {
	dump := s.metadata.Load(s.vbIds, groupName, s.bucketUuid)

	var observerState = map[uint16]ObserverState{}

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

func NewCheckpoint(observer Observer, vbIds []uint16, failoverLogs map[uint16]gocbcore.FailoverEntry, bucketUuid string, metadata Metadata) Checkpoint {
	return &checkpoint{
		observer:     observer,
		vbIds:        vbIds,
		failoverLogs: failoverLogs,
		bucketUuid:   bucketUuid,
		metadata:     metadata,
	}
}
