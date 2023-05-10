package couchbase

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/config"

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
	client             Client
	config             *config.Dcp
	bus                helpers.Bus
	configSnapshot     *gocbcore.ConfigSnapshot
	persistedSeqNos    map[uint16][]*vbUUIDAndSeqNo
	configWatchTimer   *time.Ticker
	observeTimer       *time.Ticker
	observeCloseCh     chan struct{}
	observeCloseDoneCh chan struct{}
	vbIds              []uint16
	activeGroupID      int
	closed             bool
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

func (r *rollbackMitigation) observeVbID(
	vbID uint16,
	vbUUID gocbcore.VbUUID,
	callback func(*gocbcore.ObserveVbResult, error),
) { //nolint:unused
	_, err := r.client.GetAgent().ObserveVb(gocbcore.ObserveVbOptions{
		VbID:   vbID,
		VbUUID: vbUUID,
	}, callback)
	if err != nil {
		callback(nil, err)
	}
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

func (r *rollbackMitigation) startObserve(groupID int) {
	uuIDMap := map[uint16]gocbcore.VbUUID{}

	for vbID := range r.persistedSeqNos {
		failoverLogs, err := r.client.GetFailoverLogs(vbID)
		if err != nil {
			panic(err)
		}

		uuIDMap[vbID] = failoverLogs[0].VbUUID
	}

	r.observeTimer = time.NewTicker(r.config.RollbackMitigation.Interval)
	for {
		select {
		case <-r.observeTimer.C:
			for vbID := range r.persistedSeqNos {
				if r.closed || r.activeGroupID != groupID {
					break
				}

				r.observe(vbID, uuIDMap[vbID])
			}
		case <-r.observeCloseCh:
			r.observeCloseDoneCh <- struct{}{}
			return
		}
	}
}

func (r *rollbackMitigation) reconfigure() {
	if r.observeTimer != nil {
		r.observeTimer.Stop()
		r.observeCloseCh <- struct{}{}
		<-r.observeCloseDoneCh
	}

	r.activeGroupID++
	logger.Log.Printf("new cluster config received, groupId = %v", r.activeGroupID)

	r.reset()
	err := r.markAbsentInstances()
	if err != nil {
		panic(err)
	}

	go r.startObserve(r.activeGroupID)
}

func (r *rollbackMitigation) observe(vbID uint16, vbUUID gocbcore.VbUUID) {
	r.observeVbID(vbID, vbUUID, func(result *gocbcore.ObserveVbResult, err error) {
		if r.closed {
			return
		}

		if err != nil {
			if errors.Is(err, gocbcore.ErrTemporaryFailure) {
				// skip
				return
			} else {
				panic(err)
			}
		}

		for _, replica := range r.persistedSeqNos[vbID] {
			replica.seqNo = result.PersistSeqNo
			replica.vbUUID = result.VbUUID
		}

		r.bus.Emit(helpers.PersistSeqNoChangedBusEventName, models.PersistSeqNo{
			VbID:  vbID,
			SeqNo: r.getMinSeqNo(vbID),
		})
	})
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

func (r *rollbackMitigation) waitFirstConfig() error {
	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := r.client.GetAgent().WaitForConfigSnapshot(
		time.Now().Add(r.config.ConnectionTimeout),
		gocbcore.WaitForConfigSnapshotOptions{},
		func(result *gocbcore.WaitForConfigSnapshotResult, err error) {
			r.configSnapshot = result.Snapshot

			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func (r *rollbackMitigation) Start() {
	logger.Log.Printf("rollback mitigation will start with %v interval", r.config.RollbackMitigation.Interval)

	err := r.waitFirstConfig()
	if err != nil {
		logger.ErrorLog.Printf("cannot get first config: %v", err)
		panic(err)
	}

	r.reconfigure()

	go func() {
		r.configWatchTimer = time.NewTicker(time.Second * 2)
		for range r.configWatchTimer.C {
			r.configWatch()
		}
	}()
}

func (r *rollbackMitigation) Stop() {
	r.closed = true
	r.configWatchTimer.Stop()

	logger.Log.Printf("rollback mitigation stopped")
}

func NewRollbackMitigation(client Client, config *config.Dcp, vbIds []uint16, bus helpers.Bus) RollbackMitigation {
	return &rollbackMitigation{
		client:             client,
		config:             config,
		vbIds:              vbIds,
		bus:                bus,
		observeCloseCh:     make(chan struct{}, 1),
		observeCloseDoneCh: make(chan struct{}, 1),
	}
}
