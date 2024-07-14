package couchbase

import (
	"time"

	"github.com/asaskevich/EventBus"

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
	CreateCollection(creation gocbcore.DcpCollectionCreation)
	DeleteCollection(deletion gocbcore.DcpCollectionDeletion)
	FlushCollection(flush gocbcore.DcpCollectionFlush)
	CreateScope(creation gocbcore.DcpScopeCreation)
	DeleteScope(deletion gocbcore.DcpScopeDeletion)
	ModifyCollection(modification gocbcore.DcpCollectionModification)
	OSOSnapshot(snapshot models.DcpOSOSnapshot)
	SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced)
	GetMetrics() *ObserverMetric
	GetPersistSeqNo() gocbcore.SeqNo
	Listen() models.ListenerCh
	Close()
	CloseEnd()
	ListenEnd() models.ListenerEndCh
	SetCatchup(seqNo gocbcore.SeqNo)
	SetVbUUID(vbUUID gocbcore.VbUUID)
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
	bus             EventBus.Bus
	currentSnapshot *models.SnapshotMarker
	listenerEndCh   models.ListenerEndCh
	collectionIDs   map[uint32]string
	metrics         *ObserverMetric
	listenerCh      models.ListenerCh
	vbUUID          *gocbcore.VbUUID
	config          *dcp.Dcp
	catchupSeqNo    uint64
	persistSeqNo    gocbcore.SeqNo
	vbID            uint16
	isCatchupNeed   bool
	closed          bool
}

func (so *observer) SetCatchup(seqNo gocbcore.SeqNo) {
	so.catchupSeqNo = uint64(seqNo)
	so.isCatchupNeed = true
}

func (so *observer) persistSeqNoChangedListener(persistSeqNo models.PersistSeqNo) {
	if persistSeqNo.SeqNo != 0 {
		if persistSeqNo.SeqNo > so.persistSeqNo {
			so.persistSeqNo = persistSeqNo.SeqNo
		}
	} else {
		logger.Log.Trace("persistSeqNo: %v on vbId: %v", persistSeqNo.SeqNo, persistSeqNo.VbID)
	}
}

func (so *observer) checkPersistSeqNo(seqNo uint64) bool {
	return gocbcore.SeqNo(seqNo) <= so.persistSeqNo || so.closed
}

func (so *observer) needCatchup(seqNo uint64) bool {
	if !so.isCatchupNeed {
		return false
	}

	if seqNo >= so.catchupSeqNo {
		so.isCatchupNeed = false
		logger.Log.Info("catchup completed for vbID: %d", so.vbID)
		return seqNo == so.catchupSeqNo
	}

	return true
}

func (so *observer) waitRollbackMitigation(seqNo uint64) {
	for {
		if so.checkPersistSeqNo(seqNo) {
			break
		}

		time.Sleep(so.config.RollbackMitigation.Interval / 5)
	}
}

func (so *observer) canForward(seqNo uint64) bool {
	if !so.config.RollbackMitigation.Disabled {
		so.waitRollbackMitigation(seqNo)
	}

	return !so.needCatchup(seqNo)
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
	so.currentSnapshot = &models.SnapshotMarker{
		StartSeqNo: event.StartSeqNo,
		EndSeqNo:   event.EndSeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) { //nolint:dupl
	if !so.canForward(mutation.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: &mutation,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
				EventTime:      time.Unix(int64(mutation.Cas/1000000000), 0),
			},
		})
	}

	so.metrics.AddMutation()
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) { //nolint:dupl
	if !so.canForward(deletion.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: &deletion,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
				EventTime:      time.Unix(int64(deletion.Cas/1000000000), 0),
			},
		})
	}

	so.metrics.AddDeletion()
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) { //nolint:dupl
	if !so.canForward(expiration.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: &expiration,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
				EventTime:      time.Unix(int64(expiration.Cas/1000000000), 0),
			},
		})
	}

	so.metrics.AddExpiration()
}

// nolint:staticcheck
func (so *observer) End(event models.DcpStreamEnd, err error) {
	defer func() {
		if r := recover(); r != nil {
			// listenerEndCh channel is closed
		}
	}()

	so.listenerEndCh <- models.DcpStreamEndContext{
		Event: event,
		Err:   err,
	}
}

func (so *observer) CreateCollection(event gocbcore.DcpCollectionCreation) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionCreation{
				DcpCollectionCreation: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
			},
		})
	}
}

func (so *observer) DeleteCollection(event gocbcore.DcpCollectionDeletion) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionDeletion{
				DcpCollectionDeletion: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
			},
		})
	}
}

func (so *observer) FlushCollection(event gocbcore.DcpCollectionFlush) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionFlush{
				DcpCollectionFlush: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
			},
		})
	}
}

func (so *observer) CreateScope(event gocbcore.DcpScopeCreation) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpScopeCreation{
				DcpScopeCreation: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
			},
		})
	}
}

func (so *observer) DeleteScope(event gocbcore.DcpScopeDeletion) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpScopeDeletion{
				DcpScopeDeletion: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
			},
		})
	}
}

func (so *observer) ModifyCollection(event gocbcore.DcpCollectionModification) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionModification{
				DcpCollectionModification: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         *so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
			},
		})
	}
}

func (so *observer) OSOSnapshot(event gocbcore.DcpOSOSnapshot) {
	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) SeqNoAdvanced(advanced gocbcore.DcpSeqNoAdvanced) {
	if !so.canForward(advanced.SeqNo) {
		return
	}

	snapshot := &models.SnapshotMarker{
		StartSeqNo: advanced.SeqNo,
		EndSeqNo:   advanced.SeqNo,
	}

	so.currentSnapshot = snapshot

	so.sendOrSkip(models.ListenerArgs{
		Event: models.InternalDcpSeqNoAdvance{
			DcpSeqNoAdvanced: &advanced,
			Offset: &models.Offset{
				SnapshotMarker: snapshot,
				VbUUID:         *so.vbUUID,
				SeqNo:          advanced.SeqNo,
			},
		},
	})
}

func (so *observer) GetMetrics() *ObserverMetric {
	return so.metrics
}

func (so *observer) GetPersistSeqNo() gocbcore.SeqNo {
	return so.persistSeqNo
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

	logger.Log.Debug("observer closing")

	err := so.bus.Unsubscribe(helpers.PersistSeqNoChangedBusEventName, so.persistSeqNoChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}

	so.closed = true
	close(so.listenerCh)

	// to drain buffered channel
	closedListenerCh := make(models.ListenerCh)
	close(closedListenerCh)
	so.listenerCh = closedListenerCh

	logger.Log.Debug("observer closed")
}

func (so *observer) SetVbUUID(vbUUID gocbcore.VbUUID) {
	so.vbUUID = &vbUUID
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
	vbID uint16,
	collectionIDs map[uint32]string,
	bus EventBus.Bus,
) Observer {
	observer := &observer{
		vbID:          vbID,
		metrics:       &ObserverMetric{},
		collectionIDs: collectionIDs,
		listenerCh:    make(models.ListenerCh),
		listenerEndCh: make(models.ListenerEndCh, 1),
		bus:           bus,
		config:        config,
	}

	err := observer.bus.Subscribe(helpers.PersistSeqNoChangedBusEventName, observer.persistSeqNoChangedListener)
	if err != nil {
		logger.Log.Error("error while subscribe to persistSeqNo changed event, err: %v", err)
		panic(err)
	}

	return observer
}
