package dcp

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
	SeqNoAdvanced(advanced models.DcpSeqNoAdvanced)
	GetMetrics() map[uint16]ObserverMetric
	Listen() models.ListenerCh
	Close()
	CloseEnd()
	ListenEnd() chan models.DcpStreamEnd
}

type ObserverMetric struct {
	TotalMutations   float64
	TotalDeletions   float64
	TotalExpirations float64
}

type observer struct {
	currentSnapshots     map[uint16]*models.SnapshotMarker
	currentSnapshotsLock sync.Mutex

	uuIDs     map[uint16]gocbcore.VbUUID
	uuIDsLock sync.Mutex

	metrics     map[uint16]ObserverMetric
	metricsLock sync.Mutex

	collectionIDs map[uint32]string

	listenerCh models.ListenerCh
	endCh      chan models.DcpStreamEnd
}

func (so *observer) convertToCollectionName(collectionID uint32) *string {
	if name, ok := so.collectionIDs[collectionID]; ok {
		return &name
	}

	return nil
}

//nolint:staticcheck
func (so *observer) sendOrSkip(args models.ListenerArgs) {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	so.listenerCh <- args
}

func (so *observer) SnapshotMarker(marker models.DcpSnapshotMarker) {
	so.currentSnapshots[marker.VbID] = &models.SnapshotMarker{
		StartSeqNo: marker.StartSeqNo,
		EndSeqNo:   marker.EndSeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: marker,
	})
}

func (so *observer) Mutation(mutation gocbcore.DcpMutation) {
	if currentSnapshot, ok := so.currentSnapshots[mutation.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpMutation{
				DcpMutation: mutation,
				Offset: models.Offset{
					SnapshotMarker: *currentSnapshot,
					VbUUID:         so.uuIDs[mutation.VbID],
					SeqNo:          mutation.SeqNo,
				},
				CollectionName: so.convertToCollectionName(mutation.CollectionID),
			},
		})
	}

	so.metricsLock.Lock()
	defer so.metricsLock.Unlock()

	if metric, ok := so.metrics[mutation.VbID]; ok {
		metric.TotalMutations++
	} else {
		so.metrics[mutation.VbID] = ObserverMetric{
			TotalMutations: 1,
		}
	}
}

func (so *observer) Deletion(deletion gocbcore.DcpDeletion) {
	if currentSnapshot, ok := so.currentSnapshots[deletion.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpDeletion{
				DcpDeletion: deletion,
				Offset: models.Offset{
					SnapshotMarker: *currentSnapshot,
					VbUUID:         so.uuIDs[deletion.VbID],
					SeqNo:          deletion.SeqNo,
				},
				CollectionName: so.convertToCollectionName(deletion.CollectionID),
			},
		})
	}

	so.metricsLock.Lock()
	defer so.metricsLock.Unlock()

	if metric, ok := so.metrics[deletion.VbID]; ok {
		metric.TotalDeletions++
	} else {
		so.metrics[deletion.VbID] = ObserverMetric{
			TotalDeletions: 1,
		}
	}
}

func (so *observer) Expiration(expiration gocbcore.DcpExpiration) {
	if currentSnapshot, ok := so.currentSnapshots[expiration.VbID]; ok && currentSnapshot != nil {
		so.sendOrSkip(models.ListenerArgs{
			Event: models.InternalDcpExpiration{
				DcpExpiration: expiration,
				Offset: models.Offset{
					SnapshotMarker: *currentSnapshot,
					VbUUID:         so.uuIDs[expiration.VbID],
					SeqNo:          expiration.SeqNo,
				},
				CollectionName: so.convertToCollectionName(expiration.CollectionID),
			},
		})
	}

	so.metricsLock.Lock()
	defer so.metricsLock.Unlock()

	if metric, ok := so.metrics[expiration.VbID]; ok {
		metric.TotalExpirations++
	} else {
		so.metrics[expiration.VbID] = ObserverMetric{
			TotalExpirations: 1,
		}
	}
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
	so.currentSnapshots[advanced.VbID] = &models.SnapshotMarker{
		StartSeqNo: advanced.SeqNo,
		EndSeqNo:   advanced.SeqNo,
	}

	so.sendOrSkip(models.ListenerArgs{
		Event: advanced,
	})
}

func (so *observer) GetMetrics() map[uint16]ObserverMetric {
	so.metricsLock.Lock()
	defer so.metricsLock.Unlock()

	return so.metrics
}

func (so *observer) Listen() models.ListenerCh {
	return so.listenerCh
}

func (so *observer) ListenEnd() chan models.DcpStreamEnd {
	return so.endCh
}

//nolint:staticcheck
func (so *observer) Close() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	close(so.listenerCh)
}

//nolint:staticcheck
func (so *observer) CloseEnd() {
	defer func() {
		if r := recover(); r != nil {
			// listener channel is closed
		}
	}()

	close(so.endCh)
}

func NewObserver(
	config helpers.Config,
	collectionIDs map[uint32]string,
	uuIDMap map[uint16]gocbcore.VbUUID,
) Observer {
	return &observer{
		currentSnapshots:     map[uint16]*models.SnapshotMarker{},
		currentSnapshotsLock: sync.Mutex{},

		uuIDs:     uuIDMap,
		uuIDsLock: sync.Mutex{},

		metrics:     map[uint16]ObserverMetric{},
		metricsLock: sync.Mutex{},

		collectionIDs: collectionIDs,

		listenerCh: make(models.ListenerCh, config.Dcp.Listener.BufferSize),
		endCh:      make(chan models.DcpStreamEnd, 1),
	}
}
