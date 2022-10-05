package main

import (
	"github.com/couchbase/gocbcore/v10"
	"sync"
)

type Mutation struct {
	Key          []byte
	Expiry       uint32
	LockTime     uint32
	Cas          uint64
	Value        []byte
	CollectionID uint32
	StreamID     uint16
}

type Deletion struct {
	Key          []byte
	IsExpiration bool
	DeleteTime   uint32
	Cas          uint64
	CollectionID uint32
	StreamID     uint16
}

type snapshotMarker struct {
	lastSnapStart uint64
	lastSnapEnd   uint64
}

type seqNoMarker struct {
	startSeqNo uint64
	endSeqNo   uint64
}

type dcpStreamObserver struct {
	lock sync.Mutex

	//mutations      map[string]Mutation //TODO: Could use struct with {counter, Lock} for each thing so we don't lock everything
	//deletions      map[string]Deletion
	//expirations    map[string]Deletion
	//collCreations  uint32
	//scopeCreations uint32
	//collDels       uint32
	//scopeDels      uint32

	dataRange map[uint16]seqNoMarker    // todo make it durable
	lastSeqNo map[uint16]uint64         // todo make it durable
	snapshots map[uint16]snapshotMarker // todo make it durable
	endWg     sync.WaitGroup

	OnMutation   func(mutation Mutation)
	OnDeletion   func(deletion Deletion)
	OnExpiration func(deletion Deletion)
}

func (so *dcpStreamObserver) SnapshotMarker(marker gocbcore.DcpSnapshotMarker) {
	so.lock.Lock()
	so.snapshots[marker.VbID] = snapshotMarker{marker.StartSeqNo, marker.EndSeqNo}
	if so.lastSeqNo[marker.VbID] < marker.StartSeqNo || so.lastSeqNo[marker.VbID] > marker.EndSeqNo {
		so.lastSeqNo[marker.VbID] = marker.StartSeqNo
	}
	so.lock.Unlock()
}

func (so *dcpStreamObserver) Mutation(dcpMutation gocbcore.DcpMutation) {
	mutation := Mutation{
		Key:          dcpMutation.Key,
		Expiry:       dcpMutation.Expiry,
		LockTime:     dcpMutation.LockTime,
		Cas:          dcpMutation.Cas,
		Value:        dcpMutation.Value,
		CollectionID: dcpMutation.CollectionID,
		StreamID:     dcpMutation.StreamID,
	}
	so.lock.Lock()
	// so.mutations[string(dcpMutation.Key)] = mutation // state
	if so.OnMutation != nil {
		so.OnMutation(mutation) // callback
	}
	so.lock.Unlock()
}

func (so *dcpStreamObserver) Deletion(dcpDeletion gocbcore.DcpDeletion) {
	deletion := Deletion{
		Key:          dcpDeletion.Key,
		IsExpiration: false,
		DeleteTime:   dcpDeletion.DeleteTime,
		Cas:          dcpDeletion.Cas,
		CollectionID: dcpDeletion.CollectionID,
		StreamID:     dcpDeletion.StreamID,
	}

	so.lock.Lock()
	if so.OnDeletion != nil {
		so.OnDeletion(deletion)
	}
	// so.deletions[string(dcpDeletion.Key)] = deletion // state
	so.lock.Unlock()
}

func (so *dcpStreamObserver) Expiration(dcpExpiration gocbcore.DcpExpiration) {
	expiration := Deletion{
		IsExpiration: true,
		DeleteTime:   dcpExpiration.DeleteTime,
		Cas:          dcpExpiration.Cas,
		CollectionID: dcpExpiration.CollectionID,
		StreamID:     dcpExpiration.StreamID,
	}

	so.lock.Lock()
	// so.deletions[string(dcpExpiration.Key)] = expiration
	if so.OnExpiration != nil {
		so.OnExpiration(expiration)
	}
	so.lock.Unlock()
}

func (so *dcpStreamObserver) End(dcpEnd gocbcore.DcpStreamEnd, err error) {
	// todo save state end restart from latest seq
	so.endWg.Done()
}

func (so *dcpStreamObserver) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	// so.lock.Lock()
	// so.collCreations++
	// so.lock.Unlock()
}

func (so *dcpStreamObserver) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
}

func (so *dcpStreamObserver) FlushCollection(flush gocbcore.DcpCollectionFlush) {
}

func (so *dcpStreamObserver) CreateScope(creation gocbcore.DcpScopeCreation) {
	// so.lock.Lock()
	// so.scopeCreations++
	// so.lock.Unlock()
}

func (so *dcpStreamObserver) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
}

func (so *dcpStreamObserver) ModifyCollection(modification gocbcore.DcpCollectionModification) {
}

func (so *dcpStreamObserver) OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot) {
}

func (so *dcpStreamObserver) SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced) {
	so.lastSeqNo[advanced.VbID] = advanced.SeqNo
}
