package main

import (
	"github.com/couchbase/gocbcore/v10"
	"sync"
)

type Observer interface {
	SnapshotMarker(marker gocbcore.DcpSnapshotMarker)
	Mutation(mutation gocbcore.DcpMutation)
	Deletion(deletion gocbcore.DcpDeletion)
	Expiration(expiration gocbcore.DcpExpiration)
	End(dcpEnd gocbcore.DcpStreamEnd, err error)
	CreateCollection(creation gocbcore.DcpCollectionCreation)
	DeleteCollection(deletion gocbcore.DcpCollectionDeletion)
	FlushCollection(flush gocbcore.DcpCollectionFlush)
	CreateScope(creation gocbcore.DcpScopeCreation)
	DeleteScope(deletion gocbcore.DcpScopeDeletion)
	ModifyCollection(modification gocbcore.DcpCollectionModification)
	OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot)
	SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced)
	GetState() map[uint16]*ObserverState
	SetState(map[uint16]*ObserverState)
}

type ObserverState struct {
	SeqNo      uint64
	StartSeqNo uint64
	EndSeqNo   uint64
}

type observer struct {
	stateLock sync.Mutex
	state     map[uint16]*ObserverState

	listener Listener

	vbIds []uint16
}

func (so *observer) SnapshotMarker(marker gocbcore.DcpSnapshotMarker) {
	so.stateLock.Lock()

	so.state[marker.VbID].StartSeqNo = marker.StartSeqNo
	so.state[marker.VbID].EndSeqNo = marker.EndSeqNo

	if so.state[marker.VbID].SeqNo < marker.StartSeqNo || so.state[marker.VbID].SeqNo > marker.EndSeqNo {
		so.state[marker.VbID].SeqNo = marker.StartSeqNo
	}

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(SnapshotMarkerName, marker, nil)
	}
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) {
	so.stateLock.Lock()

	so.state[mutation.VbID].SeqNo = mutation.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(MutationName, mutation, nil)
	}
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) {
	so.stateLock.Lock()

	so.state[deletion.VbID].SeqNo = deletion.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(DeletionName, deletion, nil)
	}
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) {
	so.stateLock.Lock()

	so.state[expiration.VbID].SeqNo = expiration.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(ExpirationName, expiration, nil)
	}
}

func (so *observer) End(dcpEnd gocbcore.DcpStreamEnd, err error) {
	if so.listener != nil {
		so.listener(EndName, dcpEnd, err)
	}
}

func (so *observer) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	if so.listener != nil {
		so.listener(CreateCollectionName, creation, nil)
	}
}

func (so *observer) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	if so.listener != nil {
		so.listener(DeleteCollectionName, deletion, nil)
	}
}

func (so *observer) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	if so.listener != nil {
		so.listener(FlushCollectionName, flush, nil)
	}
}

func (so *observer) CreateScope(creation gocbcore.DcpScopeCreation) {
	if so.listener != nil {
		so.listener(CreateScopeName, creation, nil)
	}
}

func (so *observer) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	if so.listener != nil {
		so.listener(DeleteScopeName, deletion, nil)
	}
}

func (so *observer) ModifyCollection(modification gocbcore.DcpCollectionModification) {
	if so.listener != nil {
		so.listener(ModifyCollectionName, modification, nil)
	}
}

func (so *observer) OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot) {
	if so.listener != nil {
		so.listener(OSOSnapshotName, snapshot, nil)
	}
}

func (so *observer) SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced) {
	so.stateLock.Lock()

	so.state[advanced.VbID].SeqNo = advanced.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(SeqNoAdvancedName, advanced, nil)
	}
}

func (so *observer) GetState() map[uint16]*ObserverState {
	so.stateLock.Lock()
	defer so.stateLock.Unlock()

	return so.state
}

func (so *observer) SetState(state map[uint16]*ObserverState) {
	so.stateLock.Lock()
	defer so.stateLock.Unlock()

	so.state = state
}

func NewObserver(vbIds []uint16, listener Listener) Observer {
	return &observer{
		stateLock: sync.Mutex{},
		state:     map[uint16]*ObserverState{},
		listener:  listener,
		vbIds:     vbIds,
	}
}
