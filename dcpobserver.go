package main

import (
	"fmt"
	"github.com/couchbase/gocbcore/v10"
	"sync"
)

type Mutation struct {
	Expiry       uint32
	Locktime     uint32
	Cas          uint64
	Value        []byte
	CollectionID uint32
	StreamID     uint16
}

type Deletion struct {
	IsExpiration bool
	DeleteTime   uint32
	Cas          uint64
	CollectionID uint32
	StreamID     uint16
}

type SnapshotMarker struct {
	lastSnapStart uint64
	lastSnapEnd   uint64
}

type SeqnoMarker struct {
	startSeqNo uint64
	endSeqNo   uint64
}

type dcpStreamObserver struct {
	lock           sync.Mutex
	mutations      map[string]Mutation //TODO: Could use struct with {counter, Lock} for each thing so we don't lock everything
	deletions      map[string]Deletion
	expirations    map[string]Deletion
	collCreations  uint32
	scopeCreations uint32
	collDels       uint32
	scopeDels      uint32
	dataRange      map[uint16]SeqnoMarker
	lastSeqno      map[uint16]uint64
	snapshots      map[uint16]SnapshotMarker
	endWg          sync.WaitGroup

	OnMutation func(mutation gocbcore.DcpMutation)
	OnDeletion func(deletion gocbcore.DcpDeletion)
}

func (so *dcpStreamObserver) SnapshotMarker(marker gocbcore.DcpSnapshotMarker) {
	//println("SNAPSHOT MARKER")
	so.lock.Lock()
	so.snapshots[marker.VbID] = SnapshotMarker{marker.StartSeqNo, marker.EndSeqNo}
	if so.lastSeqno[marker.VbID] < marker.StartSeqNo || so.lastSeqno[marker.VbID] > marker.EndSeqNo {
		so.lastSeqno[marker.VbID] = marker.StartSeqNo
	}
	so.lock.Unlock()
}

func (so *dcpStreamObserver) Mutation(dcpMutation gocbcore.DcpMutation) {
	mutation := Mutation{
		Expiry:       dcpMutation.Expiry,
		Locktime:     dcpMutation.LockTime,
		Cas:          dcpMutation.Cas,
		Value:        dcpMutation.Value,
		CollectionID: dcpMutation.CollectionID,
		StreamID:     dcpMutation.StreamID,
	}
	so.lock.Lock()
	so.mutations[string(dcpMutation.Key)] = mutation // state

	fmt.Printf("Mutations Total: %d", len(so.mutations))
	so.OnMutation(dcpMutation) // callback
	so.lock.Unlock()
}

func (so *dcpStreamObserver) Deletion(dcpDeletion gocbcore.DcpDeletion) {
	deletion := Deletion{
		IsExpiration: false,
		DeleteTime:   dcpDeletion.DeleteTime,
		Cas:          dcpDeletion.Cas,
		CollectionID: dcpDeletion.CollectionID,
		StreamID:     dcpDeletion.StreamID,
	}

	so.lock.Lock()
	so.deletions[string(dcpDeletion.Key)] = deletion // state
	println("DELETION")
	so.OnDeletion(dcpDeletion)
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
	so.deletions[string(dcpExpiration.Key)] = expiration
	so.lock.Unlock()
}

func (so *dcpStreamObserver) End(dcpEnd gocbcore.DcpStreamEnd, err error) {
	so.endWg.Done()
}

func (so *dcpStreamObserver) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	//fmt.Printf("Collection Created: %d\n", collectionId)
	so.lock.Lock()
	so.collCreations++
	fmt.Printf("Collections Total: %d\n", so.collCreations)
	so.lock.Unlock()
}

func (so *dcpStreamObserver) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	fmt.Printf("Collection Deleted: %d\n", deletion.CollectionID)
}

func (so *dcpStreamObserver) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	fmt.Printf("Collection Flushed: %d\n", flush.CollectionID)
}

func (so *dcpStreamObserver) CreateScope(creation gocbcore.DcpScopeCreation) {
	// fmt.Printf("Scope Created: %d\n", scopeId)
	so.lock.Lock()
	so.scopeCreations++
	fmt.Printf("Total scopes: %d\n", so.scopeCreations)
	so.lock.Unlock()
}

func (so *dcpStreamObserver) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	fmt.Printf("Scope Deleted: %d\n", deletion.ScopeID)
}

func (so *dcpStreamObserver) ModifyCollection(modification gocbcore.DcpCollectionModification) {
	fmt.Printf("Modified Collection: %d\n", modification.CollectionID)
}

func (so *dcpStreamObserver) OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot) {
	// println("OSO_SS")
}

func (so *dcpStreamObserver) SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced) {
	// println("SEQNO ADVANCED")
	so.lastSeqno[advanced.VbID] = advanced.SeqNo
}
