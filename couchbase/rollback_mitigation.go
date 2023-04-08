package couchbase

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/couchbase/gocbcore/v10"
)

type RollbackMitigation interface {
	Start()
	Stop()
}

type vbUUIDAndSeqNo struct {
	vbUUID gocbcore.VbUUID
	seqNo  gocbcore.SeqNo
	absent bool
}

type rollbackMitigation struct {
	client           Client
	bus              helpers.Bus
	configSnapshot   *gocbcore.ConfigSnapshot
	persistedSeqNos  map[uint16][]*vbUUIDAndSeqNo
	configWatchTimer *time.Ticker
	vbIds            []uint16
	activeGroupID    int
}

func (r *rollbackMitigation) getRevEpochAndID(snapshot *gocbcore.ConfigSnapshot) (int64, int64) {
	state := reflect.ValueOf(snapshot).Elem().FieldByName("state")
	routeCfg := state.Elem().FieldByName("routeCfg")

	revEpoch := routeCfg.FieldByName("revEpoch").Int()
	revID := routeCfg.FieldByName("revID").Int()

	return revEpoch, revID
}

func (r *rollbackMitigation) configWatch() {
	snapshot, err := r.client.GetConfigSnapshot()

	if err == nil && (r.configSnapshot == nil || r.isConfigSnapshotNewerThan(snapshot)) {
		r.configSnapshot = snapshot
		r.reconfigure()
	}
}

func (r *rollbackMitigation) isConfigSnapshotNewerThan(newConfigSnapshot *gocbcore.ConfigSnapshot) bool {
	oldEpoch, oldRevID := r.getRevEpochAndID(r.configSnapshot)
	newEpoch, newRevID := r.getRevEpochAndID(newConfigSnapshot)

	if newEpoch < oldEpoch {
		return false
	} else if newEpoch == oldEpoch {
		if newRevID == oldRevID {
			return false
		} else if newRevID < oldRevID {
			return false
		}
	}

	return true
}

func (r *rollbackMitigation) observeVbID(vbID uint16, vbUUID gocbcore.VbUUID) (*gocbcore.ObserveVbResult, error) { //nolint:unused
	opm := NewAsyncOp(context.Background())
	ch := make(chan error)

	var response *gocbcore.ObserveVbResult

	op, err := r.client.GetAgent().ObserveVb(gocbcore.ObserveVbOptions{
		VbID:   vbID,
		VbUUID: vbUUID,
	}, func(result *gocbcore.ObserveVbResult, err error) {
		opm.Resolve()

		response = result

		ch <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	return response, <-ch
}

func (r *rollbackMitigation) getMinSeqNo(vbID uint16) gocbcore.SeqNo { //nolint:unused
	startIndex := -1
	replicas := r.persistedSeqNos[vbID]

	for idx, replica := range replicas {
		if !replica.absent {
			startIndex = idx
			break
		}
	}

	if startIndex == -1 {
		return 0
	}

	vbUUID := replicas[startIndex].vbUUID
	minSeqNo := replicas[startIndex].seqNo

	for idx := startIndex + 1; idx < len(replicas); idx++ {
		replica := replicas[idx]
		if replica.absent {
			continue
		}

		if vbUUID != replica.vbUUID {
			return 0
		}

		if minSeqNo > replica.seqNo {
			minSeqNo = replica.seqNo
		}
	}

	return minSeqNo
}

func (r *rollbackMitigation) markAbsentInstances() error { //nolint:unused
	for vbID, replicas := range r.persistedSeqNos {
		for idx, replica := range replicas {
			serverIndex, err := r.configSnapshot.VbucketToServer(vbID, uint32(idx))
			if err != nil {
				if errors.Is(err, gocbcore.ErrInvalidReplica) {
					replica.absent = true
				} else {
					return err
				}
			} else {
				if serverIndex < 0 {
					replica.absent = true
				}
			}
		}
	}

	return nil
}

func (r *rollbackMitigation) reconfigure() {
	r.activeGroupID++
	groupID := r.activeGroupID

	r.reset()

	logger.Log.Printf("new cluster config received, groupId = %v", groupID)

	err := r.markAbsentInstances()
	if err != nil {
		panic(err)
	}

	for vbID := range r.persistedSeqNos {
		go func(innerVbId uint16) {
			failoverLogs, err := r.client.GetFailoverLogs(innerVbId)
			if err != nil {
				panic(err)
			}

			observeTimer := time.NewTicker(100 * time.Millisecond)

			for range observeTimer.C {
				if stop := r.observe(innerVbId, failoverLogs[0].VbUUID, groupID); stop {
					break
				}
			}
		}(vbID)
	}
}

func (r *rollbackMitigation) observe(vbID uint16, vbUUID gocbcore.VbUUID, groupID int) bool {
	if r.activeGroupID != groupID {
		return true
	}

	result, err := r.observeVbID(vbID, vbUUID)
	if err != nil {
		if errors.Is(err, gocbcore.ErrTemporaryFailure) {
			return false
		} else {
			panic(err)
		}
	}

	for _, replica := range r.persistedSeqNos[vbID] {
		replica.seqNo = result.PersistSeqNo
		replica.vbUUID = result.VbUUID
	}

	seqNo := r.getMinSeqNo(vbID)

	r.bus.Emit(helpers.PersistSeqNoChangedBusEventName, []interface{}{vbID, seqNo})

	return false
}

func (r *rollbackMitigation) reset() {
	replicas, err := r.configSnapshot.NumReplicas()
	if err != nil {
		panic(err)
	}

	r.persistedSeqNos = map[uint16][]*vbUUIDAndSeqNo{}

	for _, vbID := range r.vbIds {
		r.persistedSeqNos[vbID] = make([]*vbUUIDAndSeqNo, replicas+1)
		for j := 0; j <= replicas; j++ {
			r.persistedSeqNos[vbID][j] = &vbUUIDAndSeqNo{}
		}
	}
}

func (r *rollbackMitigation) Start() {
	r.configWatchTimer = time.NewTicker(time.Second * 2)

	go func() {
		for range r.configWatchTimer.C {
			r.configWatch()
		}
	}()
}

func (r *rollbackMitigation) Stop() {
	r.configWatchTimer.Stop()
}

func NewRollbackMitigation(client Client, vbIds []uint16, bus helpers.Bus) RollbackMitigation {
	return &rollbackMitigation{
		client: client,
		vbIds:  vbIds,
		bus:    bus,
	}
}
