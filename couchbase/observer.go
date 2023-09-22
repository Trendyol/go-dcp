package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/logger"

	dcp "github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/models"

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
	GetMetrics() *wrapper.ConcurrentSwissMap[uint16, *ObserverMetric]
	Listen() models.ListenerCh
	Close()
	CloseEnd()
	ListenEnd() models.ListenerEndCh
	AddCatchup(vbID uint16, seqNo gocbcore.SeqNo)
	SetVbUUID(vbID uint16, vbUUID gocbcore.VbUUID)
}

const DefaultCollectionName = "_default"

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

func (om *ObserverMetric) AddMutation() {
	om.TotalMutations++
}

func (om *ObserverMetric) AddDeletion() {
	om.TotalDeletions++
}

func (om *ObserverMetric) AddExpiration() {
	om.TotalExpirations++
}

type observer struct {
	bus                    helpers.Bus
	metrics                *wrapper.ConcurrentSwissMap[uint16, *ObserverMetric]
	listenerEndCh          models.ListenerEndCh
	collectionIDs          map[uint32]string
	catchup                *wrapper.ConcurrentSwissMap[uint16, uint64]
	currentSnapshots       *wrapper.ConcurrentSwissMap[uint16, *models.SnapshotMarker]
	listenerCh             models.ListenerCh
	persistSeqNo           *wrapper.ConcurrentSwissMap[uint16, gocbcore.SeqNo]
	uuIDMap                *wrapper.ConcurrentSwissMap[uint16, gocbcore.VbUUID]
	config                 *dcp.Dcp
	catchupNeededVbIDCount int
	closed                 bool
}

func (so *observer) AddCatchup(vbID uint16, seqNo gocbcore.SeqNo) {
	so.catchup.Store(vbID, uint64(seqNo))
	so.catchupNeededVbIDCount++
}

func (so *observer) persistSeqNoChangedListener(event interface{}) {
	persistSeqNo := event.(models.PersistSeqNo)

	if persistSeqNo.SeqNo != 0 {
		currentPersistSeqNo, _ := so.persistSeqNo.Load(persistSeqNo.VbID)

		if persistSeqNo.SeqNo > currentPersistSeqNo {
			so.persistSeqNo.Store(persistSeqNo.VbID, persistSeqNo.SeqNo)
		}
	}
}

func (so *observer) checkPersistSeqNo(vbID uint16, seqNo uint64) bool {
	endSeqNo, ok := so.persistSeqNo.Load(vbID)

	return (ok && gocbcore.SeqNo(seqNo) <= endSeqNo) || so.closed
}

func (so *observer) needCatchup(vbID uint16, seqNo uint64) bool {
	if so.catchupNeededVbIDCount == 0 {
		return false
	}

	if catchupSeqNo, ok := so.catchup.Load(vbID); ok {
		if seqNo >= catchupSeqNo {
			so.catchup.Delete(vbID)
			so.catchupNeededVbIDCount--

			logger.Log.Info("catchup completed for vbID: %d", vbID)

			return seqNo == catchupSeqNo
		}
	} else {
		return false
	}

	return true
}

func (so *observer) waitRollbackMitigation(vbID uint16, seqNo uint64) {
	for {
		if so.checkPersistSeqNo(vbID, seqNo) {
			break
		}

		time.Sleep(so.config.RollbackMitigation.Interval / 5)
	}
}

func (so *observer) canForward(vbID uint16, seqNo uint64) bool {
	if !so.config.RollbackMitigation.Disabled {
		so.waitRollbackMitigation(vbID, seqNo)
	}

	return !so.needCatchup(vbID, seqNo)
}

func (so *observer) convertToCollectionName(collectionID uint32) string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return name
	}

	return DefaultCollectionName
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
	so.currentSnapshots.Store(event.VbID, &models.SnapshotMarker{
		StartSeqNo: event.StartSeqNo,
		EndSeqNo:   event.EndSeqNo,
	})

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) { //nolint:dupl
	if !so.canForward(mutation.VbID, mutation.SeqNo) {
		return
	}

	if currentSnapshot, ok := so.currentSnapshots.Load(mutation.VbID); ok && currentSnapshot != nil {
		vbUUID, _ := so.uuIDMap.Load(mutation.VbID)

		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: &mutation,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         vbUUID,
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
				EventTime:      time.Unix(int64(mutation.Cas/1000000000), 0),
			},
		})
	}

	if metric, ok := so.metrics.Load(mutation.VbID); ok {
		metric.AddMutation()
	} else {
		so.metrics.Store(mutation.VbID, &ObserverMetric{
			TotalMutations: 1,
		})
	}
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) { //nolint:dupl
	if !so.canForward(deletion.VbID, deletion.SeqNo) {
		return
	}

	if currentSnapshot, ok := so.currentSnapshots.Load(deletion.VbID); ok && currentSnapshot != nil {
		vbUUID, _ := so.uuIDMap.Load(deletion.VbID)

		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: &deletion,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         vbUUID,
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
				EventTime:      time.Unix(int64(deletion.Cas/1000000000), 0),
			},
		})
	}

	if metric, ok := so.metrics.Load(deletion.VbID); ok {
		metric.AddDeletion()
	} else {
		so.metrics.Store(deletion.VbID, &ObserverMetric{
			TotalDeletions: 1,
		})
	}
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) { //nolint:dupl
	if !so.canForward(expiration.VbID, expiration.SeqNo) {
		return
	}

	if currentSnapshot, ok := so.currentSnapshots.Load(expiration.VbID); ok && currentSnapshot != nil {
		vbUUID, _ := so.uuIDMap.Load(expiration.VbID)

		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: &expiration,
				Offset: &models.Offset{
					SnapshotMarker: currentSnapshot,
					VbUUID:         vbUUID,
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
				EventTime:      time.Unix(int64(expiration.Cas/1000000000), 0),
			},
		})
	}

	if metric, ok := so.metrics.Load(expiration.VbID); ok {
		metric.AddExpiration()
	} else {
		so.metrics.Store(expiration.VbID, &ObserverMetric{
			TotalExpirations: 1,
		})
	}
}

// nolint:staticcheck
func (so *observer) End(event models.DcpStreamEnd, _ error) {
	defer func() {
		if r := recover(); r != nil {
			// listenerEndCh channel is closed
		}
	}()

	so.listenerEndCh <- event
}

func (so *observer) CreateCollection(event models.DcpCollectionCreation) {
	if !so.canForward(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) DeleteCollection(event models.DcpCollectionDeletion) {
	if !so.canForward(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) FlushCollection(event models.DcpCollectionFlush) {
	if !so.canForward(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) CreateScope(event models.DcpScopeCreation) {
	if !so.canForward(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) DeleteScope(event models.DcpScopeDeletion) {
	if !so.canForward(event.VbID, event.SeqNo) {
		return
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) ModifyCollection(event models.DcpCollectionModification) {
	if !so.canForward(event.VbID, event.SeqNo) {
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
	if !so.canForward(advanced.VbID, advanced.SeqNo) {
		return
	}

	snapshot := &models.SnapshotMarker{
		StartSeqNo: advanced.SeqNo,
		EndSeqNo:   advanced.SeqNo,
	}

	so.currentSnapshots.Store(advanced.VbID, snapshot)

	vbUUID, _ := so.uuIDMap.Load(advanced.VbID)

	so.sendOrSkip(models.ListenerArgs{
		Event: models.InternalDcpSeqNoAdvance{
			DcpSeqNoAdvanced: &advanced,
			Offset: &models.Offset{
				SnapshotMarker: snapshot,
				VbUUID:         vbUUID,
				SeqNo:          advanced.SeqNo,
			},
		},
	})
}

func (so *observer) GetMetrics() *wrapper.ConcurrentSwissMap[uint16, *ObserverMetric] {
	return so.metrics
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

	// to drain buffered channel
	closedListenerCh := make(models.ListenerCh)
	close(closedListenerCh)
	so.listenerCh = closedListenerCh
}

func (so *observer) SetVbUUID(vbID uint16, vbUUID gocbcore.VbUUID) {
	so.uuIDMap.Store(vbID, vbUUID)
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
	config *dcp.Dcp,
	collectionIDs map[uint32]string,
	bus helpers.Bus,
) Observer {
	observer := &observer{
		currentSnapshots: wrapper.CreateConcurrentSwissMap[uint16, *models.SnapshotMarker](1024),
		uuIDMap:          wrapper.CreateConcurrentSwissMap[uint16, gocbcore.VbUUID](100),
		metrics:          wrapper.CreateConcurrentSwissMap[uint16, *ObserverMetric](100),
		catchup:          wrapper.CreateConcurrentSwissMap[uint16, uint64](100),
		collectionIDs:    collectionIDs,
		listenerCh:       make(models.ListenerCh, config.Dcp.Listener.BufferSize),
		listenerEndCh:    make(models.ListenerEndCh, 1),
		bus:              bus,
		persistSeqNo:     wrapper.CreateConcurrentSwissMap[uint16, gocbcore.SeqNo](100),
		config:           config,
	}

	observer.bus.Subscribe(helpers.PersistSeqNoChangedBusEventName, observer.persistSeqNoChangedListener)

	return observer
}
