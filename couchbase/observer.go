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
	Close()
	CloseEnd()
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
	config          *dcp.Dcp
	currentSnapshot *models.SnapshotMarker
	collectionIDs   map[uint32]string
	metrics         *ObserverMetric
	listener        func(args models.ListenerArgs)
	endListener     func(context models.DcpStreamEndContext)
	vbUUID          gocbcore.VbUUID
	catchupSeqNo    uint64
	persistSeqNo    gocbcore.SeqNo
	vbID            uint16
	isCatchupNeed   bool
	closed          bool
	endClosed       bool
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
		logger.Log.Trace("persistSeqNo: %v on vbID: %v", persistSeqNo.SeqNo, persistSeqNo.VbID)
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

func (so *observer) isBeforeSkipWindow(eventTime time.Time) bool {
	if so.config.Dcp.Listener.SkipUntil == nil {
		return false
	}
	return so.config.Dcp.Listener.SkipUntil.After(eventTime)
}

func (so *observer) convertToCollectionName(collectionID uint32) string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return name
	}

	return DefaultCollectionName
}

// nolint:staticcheck
func (so *observer) sendOrSkip(args models.ListenerArgs) {
	if so.closed {
		return
	}

	so.listener(args)
}

func (so *observer) SnapshotMarker(event models.DcpSnapshotMarker) {
	if !so.canForward(event.StartSeqNo) {
		return
	}

	so.currentSnapshot = &models.SnapshotMarker{
		StartSeqNo: event.StartSeqNo,
		EndSeqNo:   event.EndSeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: event,
	})
}

func (so *observer) IsInSnapshotMarker(seqNo uint64) bool {
	isIn := so.currentSnapshot != nil &&
		seqNo >= so.currentSnapshot.StartSeqNo && seqNo <= so.currentSnapshot.EndSeqNo

	if !isIn {
		logger.Log.Warn("seqNo not in snapshot: %v", seqNo)
	}

	return isIn
}

func (so *observer) Mutation(event gocbcore.DcpMutation) { //nolint:dupl
	if !so.canForward(event.SeqNo) {
		return
	}

	eventTime := time.Unix(int64(event.Cas/1000000000), 0)
	if so.isBeforeSkipWindow(eventTime) {
		return
	}

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
				EventTime:      eventTime,
			},
		})

		so.metrics.AddMutation()
	}
}

func (so *observer) Deletion(event gocbcore.DcpDeletion) { //nolint:dupl
	if !so.canForward(event.SeqNo) {
		return
	}

	eventTime := time.Unix(int64(event.Cas/1000000000), 0)
	if so.isBeforeSkipWindow(eventTime) {
		return
	}

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
				EventTime:      eventTime,
			},
		})

		so.metrics.AddDeletion()
	}
}

func (so *observer) Expiration(event gocbcore.DcpExpiration) { //nolint:dupl
	if !so.canForward(event.SeqNo) {
		return
	}

	eventTime := time.Unix(int64(event.Cas/1000000000), 0)
	if so.isBeforeSkipWindow(eventTime) {
		return
	}

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
					SeqNo:          event.SeqNo,
				},
				CollectionName: so.convertToCollectionName(event.CollectionID),
				EventTime:      eventTime,
			},
		})

		so.metrics.AddExpiration()
	}
}

// nolint:staticcheck
func (so *observer) End(event models.DcpStreamEnd, err error) {
	if so.endClosed {
		return
	}

	so.endListener(models.DcpStreamEndContext{
		Event: event,
		Err:   err,
	})
}

func (so *observer) CreateCollection(event gocbcore.DcpCollectionCreation) {
	if !so.canForward(event.SeqNo) {
		return
	}

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionCreation{
				DcpCollectionCreation: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionDeletion{
				DcpCollectionDeletion: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionFlush{
				DcpCollectionFlush: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpScopeCreation{
				DcpScopeCreation: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpScopeDeletion{
				DcpScopeDeletion: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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

	if so.IsInSnapshotMarker(event.SeqNo) {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpCollectionModification{
				DcpCollectionModification: &event,
				Offset: &models.Offset{
					SnapshotMarker: so.currentSnapshot,
					VbUUID:         so.vbUUID,
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
				VbUUID:         so.vbUUID,
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

// nolint:staticcheck
func (so *observer) Close() {
	logger.Log.Debug("observer closing")

	err := so.bus.Unsubscribe(helpers.PersistSeqNoChangedBusEventName, so.persistSeqNoChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}

	so.closed = true

	logger.Log.Debug("observer closed")
}

func (so *observer) SetVbUUID(vbUUID gocbcore.VbUUID) {
	so.vbUUID = vbUUID
}

// nolint:staticcheck
func (so *observer) CloseEnd() {
	so.endClosed = true
}

func NewObserver(
	config *dcp.Dcp,
	vbID uint16,
	listener func(args models.ListenerArgs),
	endListener func(context models.DcpStreamEndContext),
	collectionIDs map[uint32]string,
	bus EventBus.Bus,
) Observer {
	observer := &observer{
		vbID:          vbID,
		metrics:       &ObserverMetric{},
		collectionIDs: collectionIDs,
		listener:      listener,
		endListener:   endListener,
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
