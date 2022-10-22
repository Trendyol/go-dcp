package godcpclient

import (
	"sync"
)

type Observer interface {
	SnapshotMarker(marker DcpSnapshotMarker)
	Mutation(mutation DcpMutation)
	Deletion(deletion DcpDeletion)
	Expiration(expiration DcpExpiration)
	End(dcpEnd DcpStreamEnd, err error)
	CreateCollection(creation DcpCollectionCreation)
	DeleteCollection(deletion DcpCollectionDeletion)
	FlushCollection(flush DcpCollectionFlush)
	CreateScope(creation DcpScopeCreation)
	DeleteScope(deletion DcpScopeDeletion)
	ModifyCollection(modification DcpCollectionModification)
	OSOSnapshot(snapshot DcpOSOSnapshot)
	SeqNoAdvanced(advanced DcpSeqNoAdvanced)
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

func (so *observer) SnapshotMarker(marker DcpSnapshotMarker) {
	so.stateLock.Lock()

	so.state[marker.VbID].StartSeqNo = marker.StartSeqNo
	so.state[marker.VbID].EndSeqNo = marker.EndSeqNo

	if so.state[marker.VbID].SeqNo < marker.StartSeqNo || so.state[marker.VbID].SeqNo > marker.EndSeqNo {
		so.state[marker.VbID].SeqNo = marker.StartSeqNo
	}

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(marker, nil)
	}
}

func (so *observer) Mutation(mutation DcpMutation) {
	so.stateLock.Lock()

	so.state[mutation.VbID].SeqNo = mutation.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(mutation, nil)
	}
}

func (so *observer) Deletion(deletion DcpDeletion) {
	so.stateLock.Lock()

	so.state[deletion.VbID].SeqNo = deletion.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(deletion, nil)
	}
}

func (so *observer) Expiration(expiration DcpExpiration) {
	so.stateLock.Lock()

	so.state[expiration.VbID].SeqNo = expiration.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(expiration, nil)
	}
}

func (so *observer) End(dcpEnd DcpStreamEnd, err error) {
	if so.listener != nil {
		so.listener(dcpEnd, err)
	}
}

func (so *observer) CreateCollection(creation DcpCollectionCreation) {
	if so.listener != nil {
		so.listener(creation, nil)
	}
}

func (so *observer) DeleteCollection(deletion DcpCollectionDeletion) {
	if so.listener != nil {
		so.listener(deletion, nil)
	}
}

func (so *observer) FlushCollection(flush DcpCollectionFlush) {
	if so.listener != nil {
		so.listener(flush, nil)
	}
}

func (so *observer) CreateScope(creation DcpScopeCreation) {
	if so.listener != nil {
		so.listener(creation, nil)
	}
}

func (so *observer) DeleteScope(deletion DcpScopeDeletion) {
	if so.listener != nil {
		so.listener(deletion, nil)
	}
}

func (so *observer) ModifyCollection(modification DcpCollectionModification) {
	if so.listener != nil {
		so.listener(modification, nil)
	}
}

func (so *observer) OSOSnapshot(snapshot DcpOSOSnapshot) {
	if so.listener != nil {
		so.listener(snapshot, nil)
	}
}

func (so *observer) SeqNoAdvanced(advanced DcpSeqNoAdvanced) {
	so.stateLock.Lock()

	so.state[advanced.VbID].SeqNo = advanced.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(advanced, nil)
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
