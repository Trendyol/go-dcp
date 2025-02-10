package models

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type Offset struct {
	*SnapshotMarker
	VbUUID      gocbcore.VbUUID
	SeqNo       uint64
	LatestSeqNo uint64
}

type VbIDRange struct {
	Start uint16
	End   uint16
}

func (v *VbIDRange) In(vbID uint16) bool {
	return vbID >= v.Start && vbID <= v.End
}

type PersistSeqNo struct {
	VbID  uint16
	SeqNo gocbcore.SeqNo
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

func (i *InternalDcpMutation) IsCreated() bool {
	return i.RevNo == 1
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

type InternalDcpCollectionCreation struct {
	*gocbcore.DcpCollectionCreation
	Offset         *Offset
	CollectionName string
}

type InternalDcpCollectionDeletion struct {
	*gocbcore.DcpCollectionDeletion
	Offset         *Offset
	CollectionName string
}

type InternalDcpCollectionFlush struct {
	*gocbcore.DcpCollectionFlush
	Offset         *Offset
	CollectionName string
}

type InternalDcpScopeCreation struct {
	*gocbcore.DcpScopeCreation
	Offset *Offset
}

type InternalDcpScopeDeletion struct {
	*gocbcore.DcpScopeDeletion
	Offset *Offset
}

type InternalDcpCollectionModification struct {
	*gocbcore.DcpCollectionModification
	Offset         *Offset
	CollectionName string
}

type InternalDcpOSOSnapshot struct {
	*gocbcore.DcpOSOSnapshot
	Offset *Offset
}

type PingResult struct {
	MemdEndpoint string
	MgmtEndpoint string
}

type AgentQueue struct {
	Address string
	IsDcp   bool
	Current int
	Max     int
}

type (
	DcpSnapshotMarker         = gocbcore.DcpSnapshotMarker
	DcpMutation               = InternalDcpMutation
	DcpDeletion               = InternalDcpDeletion
	DcpExpiration             = InternalDcpExpiration
	DcpStreamEnd              = gocbcore.DcpStreamEnd
	DcpCollectionCreation     = InternalDcpCollectionCreation
	DcpCollectionDeletion     = InternalDcpCollectionDeletion
	DcpCollectionFlush        = InternalDcpCollectionFlush
	DcpScopeCreation          = InternalDcpScopeCreation
	DcpScopeDeletion          = InternalDcpScopeDeletion
	DcpCollectionModification = InternalDcpCollectionModification
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
