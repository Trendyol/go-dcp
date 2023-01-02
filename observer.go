package godcpclient

import (
	"sync"

	"github.com/couchbase/gocbcore/v10"
)

type Observer interface {
	SnapshotMarker(marker DcpSnapshotMarker)
	Mutation(mutation gocbcore.DcpMutation)
	Deletion(deletion gocbcore.DcpDeletion)
	Expiration(expiration gocbcore.DcpExpiration)
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
	GetMetric() ObserverMetric
}

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

type ObserverState struct {
	SeqNo      uint64
	StartSeqNo uint64
	EndSeqNo   uint64
}

type observer struct {
	stateLock sync.Mutex
	state     map[uint16]*ObserverState

	metricLock sync.Mutex
	metric     ObserverMetric

	listener Listener

	vbIds []uint16

	collectionIDs map[uint32]string
}

func (so *observer) convertToCollectionName(collectionID uint32) *string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return &name
	}

	return nil
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

func (so *observer) Mutation(mutation gocbcore.DcpMutation) {
	so.stateLock.Lock()

	so.state[mutation.VbID].SeqNo = mutation.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(InternalDcpMutation{
			DcpMutation:    mutation,
			CollectionName: so.convertToCollectionName(mutation.CollectionID),
		}, nil)
	}

	so.metricLock.Lock()

	so.metric.TotalMutations++

	so.metricLock.Unlock()
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) {
	so.stateLock.Lock()

	so.state[deletion.VbID].SeqNo = deletion.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(InternalDcpDeletion{
			DcpDeletion:    deletion,
			CollectionName: so.convertToCollectionName(deletion.CollectionID),
		}, nil)
	}

	so.metricLock.Lock()

	so.metric.TotalDeletions++

	so.metricLock.Unlock()
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) {
	so.stateLock.Lock()

	so.state[expiration.VbID].SeqNo = expiration.SeqNo

	so.stateLock.Unlock()

	if so.listener != nil {
		so.listener(InternalDcpExpiration{
			DcpExpiration:  expiration,
			CollectionName: so.convertToCollectionName(expiration.CollectionID),
		}, nil)
	}

	so.metricLock.Lock()

	so.metric.TotalExpirations++

	so.metricLock.Unlock()
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

func (so *observer) GetMetric() ObserverMetric {
	so.metricLock.Lock()
	defer so.metricLock.Unlock()

	return so.metric
}

func NewObserver(vbIds []uint16, listener Listener, collectionIDs map[uint32]string) Observer {
	return &observer{
		stateLock: sync.Mutex{},
		state:     map[uint16]*ObserverState{},

		metricLock: sync.Mutex{},
		metric:     ObserverMetric{},

		listener: listener,

		vbIds: vbIds,

		collectionIDs: collectionIDs,
	}
}
