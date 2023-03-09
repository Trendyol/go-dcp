package models

import (
	"github.com/couchbase/gocbcore/v10"
)

type Offset struct {
	SnapshotMarker
	VbUUID gocbcore.VbUUID
	SeqNo  uint64
}

type SnapshotMarker struct {
	StartSeqNo uint64
	EndSeqNo   uint64
}

type InternalDcpMutation struct {
	gocbcore.DcpMutation
	Offset
	CollectionName *string
}

type InternalDcpDeletion struct {
	gocbcore.DcpDeletion
	Offset
	CollectionName *string
}

type InternalDcpExpiration struct {
	gocbcore.DcpExpiration
	Offset
	CollectionName *string
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
	DcpSeqNoAdvanced          = gocbcore.DcpSeqNoAdvanced
)
