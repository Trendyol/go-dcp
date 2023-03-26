package couchbase

import (
	"sync"

	"github.com/Trendyol/go-dcp-client/helpers"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/couchbase/gocbcore/v10"
)

type Observer interface {
	SnapshotMarker(marker models.DcpSnapshotMarker)
	Mutation(mutation gocbcore.DcpMutation)
	Deletion(deletion gocbcore.DcpDeletion)
	Expiration(expiration gocbcore.DcpExpiration)
	End(dcpEnd models.DcpStreamEnd, err error)
	CreateCollection(creation models.DcpCollectionCreation)
	DeleteCollection(deletion models.DcpCollectionDeletion)
	FlushCollection(flush models.DcpCollectionFlush)
	CreateScope(creation models.DcpScopeCreation)
	DeleteScope(deletion models.DcpScopeDeletion)
	ModifyCollection(modification models.DcpCollectionModification)
	OSOSnapshot(snapshot models.DcpOSOSnapshot)
	SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced)
	GetMetrics() map[uint16]*ObserverMetric
	LockMetrics()
	UnlockMetrics()
	Listen() models.ListenerCh
	Close()
	CloseEnd()
	ListenEnd() chan models.DcpStreamEnd
	AddCatchup(vbID uint16, seqNo uint64)
}

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

type observer struct {
	currentSnapshots       map[uint16]*models.SnapshotMarker
	uuIDs                  map[uint16]gocbcore.VbUUID
	metrics                map[uint16]*ObserverMetric
	collectionIDs          map[uint32]string
	catchup                map[uint16]uint64
	listenerCh             models.ListenerCh
	endCh                  chan models.DcpStreamEnd
	currentSnapshotsLock   *sync.Mutex
	metricsLock            *sync.Mutex
	catchupLock            *sync.Mutex
	checkCatchup           map[uint16]bool
	catchupNeededVbIDCount int
}

func (so *observer) AddCatchup(vbID uint16, seqNo uint64) {
	so.catchupLock.Lock()
	defer so.catchupLock.Unlock()

	so.checkCatchup[vbID] = true
	so.catchup[vbID] = seqNo
	so.catchupNeededVbIDCount++
}

func (so *observer) isCatchupDone(vbID uint16, seqNo uint64) bool {
	if so.catchupNeededVbIDCount == 0 {
		return true
	}

	so.catchupLock.Lock()
	defer so.catchupLock.Unlock()

	if catchupSeqNo, ok := so.catchup[vbID]; ok {
		if seqNo >= catchupSeqNo {
			delete(so.catchup, vbID)
			delete(so.checkCatchup, vbID)
			so.catchupNeededVbIDCount--

			return seqNo != catchupSeqNo
		}
	} else {
		return true
	}

	return false
}

func (so *observer) convertToCollectionName(collectionID uint32) string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return name
	}

	return helpers.DefaultCollectionName
}

// nolint:staticcheck
func (so *observer) sendOrSkip(args models.ListenerArgs) {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	so.listenerCh <- args
}

func (so *observer) SnapshotMarker(event models.DcpSnapshotMarker) {
	so.currentSnapshotsLock.Lock()

	so.currentSnapshots[event.VbID] = &models.SnapshotMarker{
		StartSeqNo: event.StartSeqNo,
		EndSeqNo:   event.EndSeqNo,
	}

	so.currentSnapshotsLock.Unlock()

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) { //nolint:dupl
	if !so.isCatchupDone(mutation.VbID, mutation.SeqNo) {
		return
	}

	so.currentSnapshotsLock.Lock()

	if currentSnapshot, ok := so.currentSnapshots[mutation.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: &mutation,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         so.uuIDs[mutation.VbID],
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
			},
		})
	}

	so.currentSnapshotsLock.Unlock()

	so.LockMetrics()
	defer so.UnlockMetrics()

	if metric, ok := so.metrics[mutation.VbID]; ok {
		metric.TotalMutations++
	} else {
		so.metrics[mutation.VbID] = &ObserverMetric{
			TotalMutations: 1,
		}
	}
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) { //nolint:dupl
	if !so.isCatchupDone(deletion.VbID, deletion.SeqNo) {
		return
	}

	so.currentSnapshotsLock.Lock()

	if currentSnapshot, ok := so.currentSnapshots[deletion.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: &deletion,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         so.uuIDs[deletion.VbID],
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
			},
		})
	}

	so.currentSnapshotsLock.Unlock()

	so.LockMetrics()
	defer so.UnlockMetrics()

	if metric, ok := so.metrics[deletion.VbID]; ok {
		metric.TotalDeletions++
	} else {
		so.metrics[deletion.VbID] = &ObserverMetric{
			TotalDeletions: 1,
		}
	}
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) { //nolint:dupl
	if !so.isCatchupDone(expiration.VbID, expiration.SeqNo) {
		return
	}

	so.currentSnapshotsLock.Lock()

	if currentSnapshot, ok := so.currentSnapshots[expiration.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: &expiration,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         so.uuIDs[expiration.VbID],
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
			},
		})
	}

	so.currentSnapshotsLock.Unlock()

	so.LockMetrics()
	defer so.UnlockMetrics()

	if metric, ok := so.metrics[expiration.VbID]; ok {
		metric.TotalExpirations++
	} else {
		so.metrics[expiration.VbID] = &ObserverMetric{
			TotalExpirations: 1,
		}
	}
}

func (so *observer) End(event models.DcpStreamEnd, _ error) {
	so.endCh <- event
}

func (so *observer) CreateCollection(event models.DcpCollectionCreation) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) DeleteCollection(event models.DcpCollectionDeletion) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) FlushCollection(event models.DcpCollectionFlush) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) CreateScope(event models.DcpScopeCreation) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) DeleteScope(event models.DcpScopeDeletion) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) ModifyCollection(event models.DcpCollectionModification) {
	if !so.isCatchupDone(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) OSOSnapshot(event models.DcpOSOSnapshot) {
	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced) {
	if !so.isCatchupDone(advanced.VbID, advanced.SeqNo) {
		return
	}

	snapshot := &models.SnapshotMarker{
		StartSeqNo: advanced.SeqNo,
		EndSeqNo:   advanced.SeqNo,
	}

	so.currentSnapshotsLock.Lock()

	so.currentSnapshots[advanced.VbID] = snapshot

	so.sendOrSkip(models.ListenerArgs{
		Event: models.InternalDcpSeqNoAdvance{
			DcpSeqNoAdvanced: &advanced,
			Offset: &models.Offset{
				SnapshotMarker: snapshot,
				VbUUID:         so.uuIDs[advanced.VbID],
				SeqNo:          advanced.SeqNo,
			},
		},
	})

	so.currentSnapshotsLock.Unlock()
}

func (so *observer) GetMetrics() map[uint16]*ObserverMetric {
	return so.metrics
}

func (so *observer) LockMetrics() {
	so.metricsLock.Lock()
}

func (so *observer) UnlockMetrics() {
	so.metricsLock.Unlock()
}

func (so *observer) Listen() models.ListenerCh {
	return so.listenerCh
}

func (so *observer) ListenEnd() chan models.DcpStreamEnd {
	return so.endCh
}

// nolint:staticcheck
func (so *observer) Close() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	close(so.listenerCh)
}

// nolint:staticcheck
func (so *observer) CloseEnd() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	close(so.endCh)
}

func NewObserver(
	config *helpers.Config,
	collectionIDs map[uint32]string,
	uuIDMap map[uint16]gocbcore.VbUUID,
) Observer {
	return &observer{
		currentSnapshots:     map[uint16]*models.SnapshotMarker{},
		currentSnapshotsLock: &sync.Mutex{},

		uuIDs: uuIDMap,

		metrics:     map[uint16]*ObserverMetric{},
		metricsLock: &sync.Mutex{},

		catchup:      map[uint16]uint64{},
		checkCatchup: map[uint16]bool{},
		catchupLock:  &sync.Mutex{},

		collectionIDs: collectionIDs,

		listenerCh: make(models.ListenerCh, config.Dcp.Listener.BufferSize),
		endCh:      make(chan models.DcpStreamEnd, 1),
	}
}
