package couchbase

import (
	"sync"
	"time"

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
	ListenEnd() models.ListenerEndCh
	AddCatchup(vbID uint16, seqNo uint64)
	SetFailoverLogs(vbID uint16, logs []gocbcore.FailoverEntry)
}

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

type observer struct {
	bus                    helpers.Bus
	metrics                map[uint16]*ObserverMetric
	listenerEndCh          models.ListenerEndCh
	collectionIDs          map[uint32]string
	catchup                map[uint16]uint64
	metricsLock            *sync.Mutex
	currentSnapshots       map[uint16]*models.SnapshotMarker
	currentSnapshotsLock   *sync.Mutex
	listenerCh             models.ListenerCh
	persistSeqNo           map[uint16]gocbcore.SeqNo
	catchupLock            *sync.Mutex
	failoverLogsLock       *sync.Mutex
	persistSeqNoLock       *sync.Mutex
	failoverLogs           map[uint16][]gocbcore.FailoverEntry
	config                 *helpers.Config
	catchupNeededVbIDCount int
	closed                 bool
}

func (so *observer) AddCatchup(vbID uint16, seqNo uint64) {
	so.catchupLock.Lock()
	defer so.catchupLock.Unlock()

	so.catchup[vbID] = seqNo
	so.catchupNeededVbIDCount++
}

func (so *observer) persistSeqNoChangedListener(event interface{}) {
	tuple := event.([]interface{})

	vbID := tuple[0].(uint16)
	seqNo := tuple[1].(gocbcore.SeqNo)

	if seqNo != 0 {
		so.persistSeqNoLock.Lock()

		if so.persistSeqNo[vbID] != seqNo {
			so.persistSeqNo[vbID] = seqNo
		}

		so.persistSeqNoLock.Unlock()
	}
}

func (so *observer) checkPersistSeqNo(vbID uint16, seqNo uint64) bool {
	so.persistSeqNoLock.Lock()
	endSeqNo, ok := so.persistSeqNo[vbID]
	so.persistSeqNoLock.Unlock()

	return (ok && gocbcore.SeqNo(seqNo) <= endSeqNo) || so.closed
}

func (so *observer) isCatchupDone(vbID uint16, seqNo uint64) bool {
	if so.config.RollbackMitigation.Enabled && !so.checkPersistSeqNo(vbID, seqNo) {
		ticker := time.NewTicker(10 * time.Millisecond)
		for range ticker.C {
			if so.checkPersistSeqNo(vbID, seqNo) {
				break
			}
		}
	}

	if so.catchupNeededVbIDCount == 0 {
		return true
	}

	so.catchupLock.Lock()
	defer so.catchupLock.Unlock()

	if catchupSeqNo, ok := so.catchup[vbID]; ok {
		if seqNo >= catchupSeqNo {
			delete(so.catchup, vbID)
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
					VbUUID:         so.failoverLogs[mutation.VbID][0].VbUUID,
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
				EventTime:      time.Unix(int64(mutation.Cas/1000000000), 0),
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
					VbUUID:         so.failoverLogs[deletion.VbID][0].VbUUID,
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
				EventTime:      time.Unix(int64(deletion.Cas/1000000000), 0),
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
					VbUUID:         so.failoverLogs[expiration.VbID][0].VbUUID,
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
				EventTime:      time.Unix(int64(expiration.Cas/1000000000), 0),
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
	so.listenerEndCh <- event
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
				VbUUID:         so.failoverLogs[advanced.VbID][0].VbUUID,
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

func (so *observer) ListenEnd() models.ListenerEndCh {
	return so.listenerEndCh
}

// nolint:staticcheck
func (so *observer) Close() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	so.closed = true
	close(so.listenerCh)
}

func (so *observer) SetFailoverLogs(vbID uint16, logs []gocbcore.FailoverEntry) {
	if len(logs) == 0 {
		return
	}

	so.failoverLogsLock.Lock()
	defer so.failoverLogsLock.Unlock()

	so.failoverLogs[vbID] = logs
}

// nolint:staticcheck
func (so *observer) CloseEnd() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	close(so.listenerEndCh)
}

func NewObserver(
	config *helpers.Config,
	collectionIDs map[uint32]string,
	bus helpers.Bus,
) Observer {
	observer := &observer{
		currentSnapshots:     map[uint16]*models.SnapshotMarker{},
		currentSnapshotsLock: &sync.Mutex{},

		failoverLogs:     map[uint16][]gocbcore.FailoverEntry{},
		failoverLogsLock: &sync.Mutex{},

		metrics:     map[uint16]*ObserverMetric{},
		metricsLock: &sync.Mutex{},

		catchup:     map[uint16]uint64{},
		catchupLock: &sync.Mutex{},

		collectionIDs: collectionIDs,

		listenerCh:    make(models.ListenerCh, config.Dcp.Listener.BufferSize),
		listenerEndCh: make(models.ListenerEndCh, 1),

		bus: bus,

		persistSeqNo:     map[uint16]gocbcore.SeqNo{},
		persistSeqNoLock: &sync.Mutex{},

		config: config,
	}

	observer.bus.Subscribe(helpers.PersistSeqNoChangedBusEventName, observer.persistSeqNoChangedListener)

	return observer
}
