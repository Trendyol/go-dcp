package godcpclient

import "github.com/couchbase/gocbcore/v10"

type InternalDcpMutation struct {
	gocbcore.DcpMutation
}

func (i *InternalDcpMutation) IsCreated() bool {
	return i.RevNo == 1
}

type (
	DcpSnapshotMarker         = gocbcore.DcpSnapshotMarker
	DcpMutation               = InternalDcpMutation
	DcpDeletion               = gocbcore.DcpDeletion
	DcpExpiration             = gocbcore.DcpExpiration
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
