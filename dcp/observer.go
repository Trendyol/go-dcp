package dcp

import (
	"reflect"
	"sync"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"

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
	SeqNoAdvanced(advanced models.DcpSeqNoAdvanced)
	GetMetric() ObserverMetric
	Listen() models.ListenerCh
	Close()
	ListenEnd() chan models.DcpStreamEnd
}

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

type observer struct {
	currentSnapshot *models.SnapshotMarker
	uuID            gocbcore.VbUUID
	collectionIDs   map[uint32]string
	metric          ObserverMetric
	metricLock      sync.Mutex
	listenerCh      models.ListenerCh
	endCh           chan models.DcpStreamEnd
}

func (so *observer) convertToCollectionName(collectionID uint32) *string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return &name
	}

	return nil
}

func (so *observer) sendOrSkip(args models.ListenerArgs) {
	defer func() {
		if r := recover(); r != nil {
			logger.Debug("%v, skipping event: %s", r, reflect.TypeOf(args.Event))
		}
	}()

	so.listenerCh <- args
}

func (so *observer) SnapshotMarker(marker models.DcpSnapshotMarker) {
	so.currentSnapshot = &models.SnapshotMarker{
		StartSeqNo: marker.StartSeqNo,
		EndSeqNo:   marker.EndSeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: marker,
	})
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) {
	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: mutation,
				Offset: models.Offset{
					SnapshotMarker: *so.currentSnapshot,
					VbUUID:         so.uuID,
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
			},
		})
	}

	so.metricLock.Lock()

	so.metric.TotalMutations++

	so.metricLock.Unlock()
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) {
	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: deletion,
				Offset: models.Offset{
					SnapshotMarker: *so.currentSnapshot,
					VbUUID:         so.uuID,
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
			},
		})
	}

	so.metricLock.Lock()

	so.metric.TotalDeletions++

	so.metricLock.Unlock()
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) {
	if so.currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: expiration,
				Offset: models.Offset{
					SnapshotMarker: *so.currentSnapshot,
					VbUUID:         so.uuID,
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
			},
		})
	}

	so.metricLock.Lock()

	so.metric.TotalExpirations++

	so.metricLock.Unlock()
}

func (so *observer) End(end models.DcpStreamEnd, _ error) {
	so.endCh <- end
}

func (so *observer) CreateCollection(creation models.DcpCollectionCreation) {
	so.sendOrSkip(models.ListenerArgs{
		Event: creation,
	})
}

func (so *observer) DeleteCollection(deletion models.DcpCollectionDeletion) {
	so.sendOrSkip(models.ListenerArgs{
		Event: deletion,
	})
}

func (so *observer) FlushCollection(flush models.DcpCollectionFlush) {
	so.sendOrSkip(models.ListenerArgs{
		Event: flush,
	})
}

func (so *observer) CreateScope(creation models.DcpScopeCreation) {
	so.sendOrSkip(models.ListenerArgs{
		Event: creation,
	})
}

func (so *observer) DeleteScope(deletion models.DcpScopeDeletion) {
	so.sendOrSkip(models.ListenerArgs{
		Event: deletion,
	})
}

func (so *observer) ModifyCollection(modification models.DcpCollectionModification) {
	so.sendOrSkip(models.ListenerArgs{
		Event: modification,
	})
}

func (so *observer) OSOSnapshot(snapshot models.DcpOSOSnapshot) {
	so.sendOrSkip(models.ListenerArgs{
		Event: snapshot,
	})
}

func (so *observer) SeqNoAdvanced(advanced models.DcpSeqNoAdvanced) {
	so.currentSnapshot = &models.SnapshotMarker{
		StartSeqNo: advanced.SeqNo,
		EndSeqNo:   advanced.SeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: advanced,
	})
}

func (so *observer) GetMetric() ObserverMetric {
	so.metricLock.Lock()
	defer so.metricLock.Unlock()

	return so.metric
}

func (so *observer) Listen() models.ListenerCh {
	return so.listenerCh
}

func (so *observer) ListenEnd() chan models.DcpStreamEnd {
	return so.endCh
}

func (so *observer) Close() {
	defer func() {
		if r := recover(); r != nil {
			logger.Debug("panic ignored: %v", r)
		}
	}()

	close(so.listenerCh)
	close(so.endCh)
}

func NewObserver(
	collectionIDs map[uint32]string,
	uuID gocbcore.VbUUID,
) Observer {
	return &observer{
		currentSnapshot: nil,
		uuID:            uuID,

		metricLock: sync.Mutex{},
		metric:     ObserverMetric{},

		collectionIDs: collectionIDs,

		listenerCh: make(models.ListenerCh, 1),
		endCh:      make(chan models.DcpStreamEnd, 1),
	}
}
