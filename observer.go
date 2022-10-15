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
	GetState() map[int]ObserverState
	SetState(map[int]ObserverState)
}

type observer struct {
	lock          sync.Mutex
	lastSeqNo     []uint64
	lastSnapStart []uint64
	lastSnapEnd   []uint64
	listener      Listener
}

type ObserverState struct {
	LastSeqNo     uint64
	LastSnapStart uint64
	LastSnapEnd   uint64
}

func (so *observer) SnapshotMarker(marker gocbcore.DcpSnapshotMarker) {
	so.lock.Lock()
	defer so.lock.Unlock()

	so.lastSnapStart[marker.VbID] = marker.StartSeqNo
	so.lastSnapEnd[marker.VbID] = marker.EndSeqNo

	if so.lastSeqNo[marker.VbID] < marker.StartSeqNo || so.lastSeqNo[marker.VbID] > marker.EndSeqNo {
		so.lastSeqNo[marker.VbID] = marker.StartSeqNo
	}

	if so.listener != nil {
		so.listener(SnapshotMarkerName, marker, nil)
	}
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) {
	so.lock.Lock()
	defer so.lock.Unlock()

	so.lastSeqNo[mutation.VbID] = mutation.SeqNo

	if so.listener != nil {
		so.listener(MutationName, mutation, nil)
	}
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) {
	so.lock.Lock()
	defer so.lock.Unlock()

	so.lastSeqNo[deletion.VbID] = deletion.SeqNo

	if so.listener != nil {
		so.listener(DeletionName, deletion, nil)
	}
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) {
	so.lock.Lock()
	defer so.lock.Unlock()

	so.lastSeqNo[expiration.VbID] = expiration.SeqNo

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
	so.lock.Lock()
	defer so.lock.Unlock()

	so.lastSeqNo[advanced.VbID] = advanced.SeqNo

	if so.listener != nil {
		so.listener(SeqNoAdvancedName, advanced, nil)
	}
}

func (so *observer) GetState() map[int]ObserverState {
	so.lock.Lock()
	defer so.lock.Unlock()

	observerState := make(map[int]ObserverState)

	for i := 0; i < len(so.lastSeqNo); i++ {
		observerState[i] = ObserverState{
			LastSeqNo:     so.lastSeqNo[i],
			LastSnapStart: so.lastSnapStart[i],
			LastSnapEnd:   so.lastSnapEnd[i],
		}
	}

	return observerState
}

func (so *observer) SetState(state map[int]ObserverState) {
	so.lock.Lock()
	defer so.lock.Unlock()

	for i := 0; i < len(so.lastSeqNo); i++ {
		so.lastSeqNo[i] = state[i].LastSeqNo
		so.lastSnapStart[i] = state[i].LastSnapStart
		so.lastSnapEnd[i] = state[i].LastSnapEnd
	}
}

func NewObserver(vBucketNumber int) Observer {
	return &observer{
		lock:          sync.Mutex{},
		lastSeqNo:     make([]uint64, vBucketNumber),
		lastSnapStart: make([]uint64, vBucketNumber),
		lastSnapEnd:   make([]uint64, vBucketNumber),
	}
}

func NewObserverWithListener(vBucketNumber int, listener Listener) Observer {
	return &observer{
		lock:          sync.Mutex{},
		lastSeqNo:     make([]uint64, vBucketNumber),
		lastSnapStart: make([]uint64, vBucketNumber),
		lastSnapEnd:   make([]uint64, vBucketNumber),
		listener:      listener,
	}
}
