package couchbase

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/asaskevich/EventBus"

	"github.com/couchbase/gocbcore/v10"

	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
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

func (v *vbUUIDAndSeqNo) SetAbsent() {
	v.absent = true
}

func (v *vbUUIDAndSeqNo) IsAbsent() bool {
	return v.absent
}

func (v *vbUUIDAndSeqNo) SetSeqNo(seqNo gocbcore.SeqNo) {
	v.seqNo = seqNo
}

func (v *vbUUIDAndSeqNo) SetVbUUID(vbUUID gocbcore.VbUUID) {
	v.vbUUID = vbUUID
}

// IsOutdated verify if a known state is outdated comparing to lastly received from couchbase
func (v *vbUUIDAndSeqNo) IsOutdated(last *gocbcore.ObserveVbResult) bool {
	if !v.absent {
		return v.vbUUID != last.VbUUID || v.seqNo != last.PersistSeqNo
	}
	return false
}

type rollbackMitigation struct {
	bus                EventBus.Bus
	client             Client
	vbUUIDMap          *wrapper.ConcurrentSwissMap[uint16, gocbcore.VbUUID]
	configSnapshot     *gocbcore.ConfigSnapshot
	persistedSeqNos    *wrapper.ConcurrentSwissMap[uint16, []*vbUUIDAndSeqNo]
	observeCount       *atomic.Uint32
	config             *config.Dcp
	observeTimer       *time.Ticker
	observeCloseCh     chan struct{}
	observeCloseDoneCh chan struct{}
	vbIds              []uint16
	activeGroupID      int
	configWatchRunning bool
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
	snapshot, err := r.client.GetDcpAgentConfigSnapshot()

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
	replica int,
	vbUUID gocbcore.VbUUID,
	callback func(*gocbcore.ObserveVbResult, error),
) { //nolint:unused
	_, err := r.client.GetAgent().ObserveVb(gocbcore.ObserveVbOptions{
		VbID:          vbID,
		ReplicaIdx:    replica,
		VbUUID:        vbUUID,
		Deadline:      time.Now().Add(time.Second * 5),
		RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
	}, callback)
	if err != nil {
		logger.Log.Error("observeVBID error for vbID: %v, replica:%v, vbUUID: %v, err: %v", vbID, replica, vbUUID, err)
		callback(nil, err)
	}
}

func (r *rollbackMitigation) getMinSeqNo(vbID uint16) gocbcore.SeqNo { //nolint:unused
	startIndex := -1
	replicas, _ := r.persistedSeqNos.Load(vbID)

	for idx, replica := range replicas {
		if !replica.IsAbsent() {
			startIndex = idx
			break
		}
	}

	if startIndex == -1 {
		logger.Log.Error("all replicas absent")
		return 0
	}

	vbUUID := replicas[startIndex].vbUUID
	minSeqNo := replicas[startIndex].seqNo

	for idx := startIndex + 1; idx < len(replicas); idx++ {
		replica := replicas[idx]
		if replica.IsAbsent() {
			continue
		}

		if vbUUID != replica.vbUUID {
			logger.Log.Trace("vbUUID mismatch %v != %v for %v index of %v", vbUUID, replica.vbUUID, idx, len(replicas))
			return 0
		}

		if minSeqNo > replica.seqNo {
			minSeqNo = replica.seqNo
		}
	}

	return minSeqNo
}

func (r *rollbackMitigation) markAbsentInstances() error { //nolint:unused
	var outerError error

	r.persistedSeqNos.Range(func(vbID uint16, replicas []*vbUUIDAndSeqNo) bool {
		for idx, replica := range replicas {
			serverIndex, err := r.configSnapshot.VbucketToServer(vbID, uint32(idx))
			if err != nil {
				if errors.Is(err, gocbcore.ErrInvalidReplica) {
					logger.Log.Debug("invalid replica of vbID: %v, replica: %v, err: %v", vbID, idx, err)
					replica.SetAbsent()
				} else {
					outerError = err
					return false
				}
			} else {
				if serverIndex < 0 {
					logger.Log.Debug("invalid server index of vbID: %v, replica: %v, serverIndex: %v", vbID, idx, serverIndex)
					replica.SetAbsent()
				}
			}
		}

		return true
	})

	return outerError
}

func (r *rollbackMitigation) startObserve(groupID int) {
	r.vbUUIDMap = wrapper.CreateConcurrentSwissMap[uint16, gocbcore.VbUUID](1024)

	r.loadVbUUIDMap()

	r.observeTimer = time.NewTicker(r.config.RollbackMitigation.Interval)
	for {
		select {
		case <-r.observeTimer.C:
			wg := &sync.WaitGroup{}
			wg.Add(int(r.observeCount.Load()))
			r.persistedSeqNos.Range(func(vbID uint16, replicas []*vbUUIDAndSeqNo) bool {
				for idx, replica := range replicas {
					if replica.IsAbsent() {
						wg.Done()
						continue
					}

					if r.closed || r.activeGroupID != groupID {
						logger.Log.Debug("closed(%v) or groupID(%v!=%v) changed on startObserve", r.closed, r.activeGroupID, groupID)
						wg.Done()
					} else {
						vbUUID, _ := r.vbUUIDMap.Load(vbID)
						r.observe(vbID, idx, groupID, vbUUID, wg)
					}
				}

				return true
			})

			wg.Wait()
		case <-r.observeCloseCh:
			logger.Log.Debug("observe close trigger received")
			r.observeCloseDoneCh <- struct{}{}
			return
		}
	}
}

func (r *rollbackMitigation) loadVbUUID(vbID uint16) error {
	failOverLogs, err := r.client.GetFailOverLogs(vbID)
	if err != nil {
		return err
	}

	r.vbUUIDMap.Store(vbID, failOverLogs[0].VbUUID)

	failOverInfos := make([]string, 0, len(failOverLogs))
	for index, failOverLog := range failOverLogs {
		failOverInfos = append(
			failOverInfos,
			fmt.Sprintf("index: %v, vbUUID: %v, seqNo: %v", index, failOverLog.VbUUID, failOverLog.SeqNo),
		)
	}

	logger.Log.Trace(
		"observing vbID: %v, vbUUID: %v, failOverInfo: %v",
		vbID, failOverLogs[0].VbUUID, strings.Join(failOverInfos, ", "),
	)

	return nil
}

func (r *rollbackMitigation) loadVbUUIDMap() {
	eg, _ := errgroup.WithContext(context.Background())

	r.persistedSeqNos.Range(func(vbID uint16, _ []*vbUUIDAndSeqNo) bool {
		eg.Go(func() error {
			return r.loadVbUUID(vbID)
		})

		return true
	})

	err := eg.Wait()
	if err != nil {
		logger.Log.Error("error while get load vbuuid map, err: %v", err)
		panic(err)
	}
}

func (r *rollbackMitigation) reconfigure() {
	logger.Log.Debug("reconfigure triggerred")

	if r.observeTimer != nil {
		r.observeTimer.Stop()
		logger.Log.Debug("observe close triggered from reconfigure")
		r.observeCloseCh <- struct{}{}
		<-r.observeCloseDoneCh
		logger.Log.Debug("observe close done from reconfigure")
	}

	r.activeGroupID++
	logger.Log.Info("new cluster config received, groupId = %v", r.activeGroupID)

	r.reset()
	err := r.markAbsentInstances()
	if err != nil {
		logger.Log.Error("error while mark absent instances, err: %v", err)
		panic(err)
	}

	go r.startObserve(r.activeGroupID)
}

func (r *rollbackMitigation) observe(vbID uint16, replica int, groupID int, vbUUID gocbcore.VbUUID, wg *sync.WaitGroup) {
	r.observeVbID(vbID, replica, vbUUID, func(result *gocbcore.ObserveVbResult, err error) {
		wg.Done()

		if r.closed || r.activeGroupID != groupID {
			logger.Log.Debug("closed(%v) or groupID(%v!=%v) changed on observe", r.closed, r.activeGroupID, groupID)
			return
		}

		if err != nil {
			if errors.Is(err, gocbcore.ErrUnambiguousTimeout) {
				logger.Log.Debug("timeout while observe: %v", err)
				return
			}

			logger.Log.Error("error while observe, err: %v", err)

			if errors.Is(err, gocbcore.ErrTemporaryFailure) ||
				errors.Is(err, gocbcore.ErrBusy) {
				return
			} else {
				panic(err)
			}
		}

		replicas, ok := r.persistedSeqNos.Load(vbID)
		if !ok {
			logger.Log.Error("replicas of vbID: %v not found", vbID)
		}

		if len(replicas) > replica {
			// publish events only if the state is outdated, to not generate unnecessary events
			if replicas[replica].IsOutdated(result) {
				replicas[replica].SetSeqNo(result.PersistSeqNo)
				replicas[replica].SetVbUUID(result.VbUUID)

				r.bus.Publish(helpers.PersistSeqNoChangedBusEventName, models.PersistSeqNo{
					VbID:  vbID,
					SeqNo: r.getMinSeqNo(vbID),
				})
			}

			if vbUUID != result.VbUUID {
				r.vbUUIDMap.Store(vbID, result.VbUUID)
			}
		} else {
			logger.Log.Error("replica: %v not found", replica)
		}
	})
}

func (r *rollbackMitigation) reset() {
	replicas, err := r.configSnapshot.NumReplicas()
	if err != nil {
		logger.Log.Error("error while reset rollback mitigation, err: %v", err)
		panic(err)
	}

	r.persistedSeqNos = wrapper.CreateConcurrentSwissMap[uint16, []*vbUUIDAndSeqNo](1024)

	var observeCount int
	for _, vbID := range r.vbIds {
		arrLen := replicas + 1
		replicaArr := make([]*vbUUIDAndSeqNo, arrLen)
		for j := 0; j <= replicas; j++ {
			replicaArr[j] = &vbUUIDAndSeqNo{}
		}

		observeCount += arrLen
		r.persistedSeqNos.Store(vbID, replicaArr)
	}

	r.observeCount.Swap(uint32(observeCount))
}

func (r *rollbackMitigation) waitFirstConfig() error {
	opm := NewAsyncOp(context.Background())

	ch := make(chan error, 1)

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
	logger.Log.Info("rollback mitigation will start with %v interval", r.config.RollbackMitigation.Interval)

	err := r.waitFirstConfig()
	if err != nil {
		logger.Log.Error("error while get first config, err: %v", err)
		panic(err)
	}

	r.reconfigure()

	go func() {
		r.configWatchRunning = true
		for r.configWatchRunning {
			time.Sleep(r.config.RollbackMitigation.ConfigWatchInterval)
			r.configWatch()
		}
	}()
}

func (r *rollbackMitigation) Stop() {
	r.closed = true

	r.configWatchRunning = false

	if r.observeTimer != nil {
		r.observeTimer.Stop()
		logger.Log.Debug("observe close triggered from stop")
		r.observeCloseCh <- struct{}{}
		<-r.observeCloseDoneCh
		logger.Log.Debug("observe close done from stop")
	}

	logger.Log.Info("rollback mitigation stopped")
}

func NewRollbackMitigation(client Client, config *config.Dcp, vbIds []uint16, bus EventBus.Bus) RollbackMitigation {
	return &rollbackMitigation{
		client:             client,
		config:             config,
		vbIds:              vbIds,
		bus:                bus,
		observeCount:       &atomic.Uint32{},
		observeCloseCh:     make(chan struct{}, 1),
		observeCloseDoneCh: make(chan struct{}, 1),
	}
}
