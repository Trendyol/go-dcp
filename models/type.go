package models

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type Offset struct {
	*SnapshotMarker
	VbUUID gocbcore.VbUUID
	SeqNo  uint64
}

type SnapshotMarker struct {
	StartSeqNo uint64
	EndSeqNo   uint64
}

type InternalDcpMutation struct {
	EventTime time.Time
	*gocbcore.DcpMutation
	Offset         *Offset
	CollectionName string
}

type InternalDcpDeletion struct {
	EventTime time.Time
	*gocbcore.DcpDeletion
	Offset         *Offset
	CollectionName string
}

type InternalDcpExpiration struct {
	EventTime time.Time
	*gocbcore.DcpExpiration
	Offset         *Offset
	CollectionName string
}

type InternalDcpSeqNoAdvance struct {
	*gocbcore.DcpSeqNoAdvanced
	Offset *Offset
}

func (i *InternalDcpMutation) IsCreated() bool {
	return i.RevNo == 1
}

type (
	DcpSnapshotMarker         = gocbcore.DcpSnapshotMarker
	DcpMutation               = InternalDcpMutation
	DcpDeletion               = InternalDcpDeletion
	DcpExpiration             = InternalDcpExpiration
	DcpStreamEnd              = gocbcore.DcpStreamEnd
	DcpCollectionCreation     = gocbcore.DcpCollectionCreation
	DcpCollectionDeletion     = gocbcore.DcpCollectionDeletion
	DcpCollectionFlush        = gocbcore.DcpCollectionFlush
	DcpScopeCreation          = gocbcore.DcpScopeCreation
	DcpScopeDeletion          = gocbcore.DcpScopeDeletion
	DcpCollectionModification = gocbcore.DcpCollectionModification
	DcpOSOSnapshot            = gocbcore.DcpOSOSnapshot
	DcpSeqNoAdvanced          = InternalDcpSeqNoAdvance
)

type CheckpointDocumentSnapshot struct {
	StartSeqNo uint64 `json:"startSeqno"`
	EndSeqNo   uint64 `json:"endSeqno"`
}

type CheckpointDocumentCheckpoint struct {
	Snapshot *CheckpointDocumentSnapshot `json:"snapshot"`
	VbUUID   uint64                      `json:"vbuuid"`
	SeqNo    uint64                      `json:"seqno"`
}

type CheckpointDocument struct {
	Checkpoint *CheckpointDocumentCheckpoint `json:"checkpoint"`
	BucketUUID string                        `json:"bucketUuid"`
}

func NewEmptyCheckpointDocument(bucketUUID string) *CheckpointDocument {
	return &CheckpointDocument{
		Checkpoint: &CheckpointDocumentCheckpoint{
			VbUUID: 0,
			SeqNo:  0,
			Snapshot: &CheckpointDocumentSnapshot{
				StartSeqNo: 0,
				EndSeqNo:   0,
			},
		},
		BucketUUID: bucketUUID,
	}
}
