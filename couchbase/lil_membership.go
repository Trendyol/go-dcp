package couchbase

import (
	"context"
	"errors"
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/membership"
	"github.com/asaskevich/EventBus"
	"github.com/bytedance/sonic"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/google/uuid"
	"sort"
	"time"
)

type lilCbMembership struct {
	client              Client
	bus                 EventBus.Bus
	membershipConfig    *config.CouchbaseMembership
	infoChan            chan *membership.Model
	config              *config.Dcp
	info                *membership.Model
	scopeName           string
	collectionName      string
	lastActiveInstances []Instance
	instanceAll         []byte
	id                  []byte
	clusterJoinTime     int64
	heartbeatRunning    bool
	monitorRunning      bool
}

const (
	_heartbeatPath = "heartbeatTime"
)

func (h *lilCbMembership) GetInfo() *membership.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func (h *lilCbMembership) register() {
	ctx, cancel := context.WithTimeout(context.Background(), h.membershipConfig.Timeout)
	defer cancel()

	now := time.Now().UnixNano()

	err := h.createIndex(ctx, now)
	if err != nil {
		logger.Log.Error("error register while create index, err: %v", err)
		panic(err)
	}
}

func (h *lilCbMembership) createIndex(ctx context.Context, clusterJoinTime int64) error {
	var id = string(h.id)

	var instance = Instance{
		ID:              &id,
		Type:            _type,
		HeartbeatTime:   clusterJoinTime,
		ClusterJoinTime: clusterJoinTime,
	}
	payload, _ := sonic.Marshal(instance)

	return CreatePath(ctx, h.client.GetMetaAgent(), h.scopeName, h.collectionName, h.instanceAll, h.id, payload, memd.SubdocDocFlagMkDoc)
}

func (h *lilCbMembership) Close() {
	err := h.bus.Unsubscribe(helpers.MembershipChangedBusEventName, h.membershipChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}

	h.monitorRunning = false
	h.heartbeatRunning = false
}

func (h *lilCbMembership) membershipChangedListener(model *membership.Model) {
	shouldSendMessage := h.info == nil
	h.info = model
	if shouldSendMessage {
		go func() {
			h.infoChan <- model
		}()
	}
}

func (h *lilCbMembership) startHeartbeat() {
	h.heartbeatRunning = true

	go func() {
		for h.heartbeatRunning {
			time.Sleep(h.membershipConfig.HeartbeatInterval)
			h.heartbeat()
		}
	}()
}

func (h *lilCbMembership) heartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), h.membershipConfig.Timeout)
	defer cancel()

	var now = time.Now().UnixNano()

	payload, _ := sonic.Marshal(now)

	var heartbeatPath = append(append(h.id, '.'), []byte(_heartbeatPath)...)

	err := CreatePath(ctx, h.client.GetMetaAgent(), h.scopeName, h.collectionName, h.instanceAll, heartbeatPath, payload, memd.SubdocDocFlagMkDoc)
	if err != nil {
		logger.Log.Error("error while heartbeat: %v", err)
		return
	}
}

func (h *lilCbMembership) startMonitor() {
	h.monitorRunning = true

	go func() {
		logger.Log.Info("lil couchbase membership will start after %v", h.config.Dcp.Group.Membership.RebalanceDelay)
		time.Sleep(h.config.Dcp.Group.Membership.RebalanceDelay)

		for h.monitorRunning {
			h.monitor()
			time.Sleep(h.membershipConfig.MonitorInterval)
		}
	}()
}

//nolint:funlen
func (h *lilCbMembership) monitor() {
	ctx, cancel := context.WithTimeout(context.Background(), h.membershipConfig.Timeout)
	defer cancel()

	data, err := Get(ctx, h.client.GetMetaAgent(), h.scopeName, h.collectionName, h.instanceAll)
	if err != nil {
		logger.Log.Error("error while monitor try to get index: %v", err)
		return
	}

	all := map[string]Instance{}

	err = sonic.Unmarshal(data.Value, &all)
	if err != nil {
		logger.Log.Error("error while monitor try to unmarshal index: %v", err)
		return
	}

	ids := make([]string, 0, len(all))

	for k := range all {
		ids = append(ids, k)
	}
	sort.SliceStable(ids, func(i, j int) bool {
		return all[ids[i]].ClusterJoinTime < all[ids[j]].ClusterJoinTime
	})

	instances := make([]*Instance, len(ids))

	for i, id := range ids {
		instance, ok := all[id]
		if !ok {
			logger.Log.Error("error while monitor instance: %v, err: %v", id, err)
			panic(err)
		}

		if h.isAlive(instance.HeartbeatTime) {
			instances[i] = &instance
		} else {
			logger.Log.Info("instance %v is not alive", instance.ID)
		}
	}

	var filteredInstances []Instance
	for _, instance := range instances {
		if instance != nil {
			filteredInstances = append(filteredInstances, *instance)
		}
	}

	if h.isClusterChanged(filteredInstances) {
		err = h.updateIndex(ctx, filteredInstances, data.Cas)
		if err == nil {
			h.rebalance(filteredInstances)
		} else {
			if errors.Is(err, gocbcore.ErrCasMismatch) {
				logger.Log.Debug("cannot update instances: cas mismatch")
				h.monitor()
			} else {
				logger.Log.Error("error while update instances: %v", err)
			}
		}
	}
}

func (h *lilCbMembership) updateIndex(ctx context.Context, instances []Instance, cas gocbcore.Cas) error {
	all := map[string]Instance{}

	for _, instance := range instances {
		all[*instance.ID] = instance
	}

	payload, _ := sonic.Marshal(all)

	err := UpdateDocument(ctx, h.client.GetMetaAgent(), h.scopeName, h.collectionName, h.instanceAll, payload, 0, &cas)
	if err != nil {
		return err
	}
	return nil
}

func (h *lilCbMembership) rebalance(instances []Instance) {
	selfOrder := 0

	for index, instance := range instances {
		if *instance.ID == string(h.id) {
			selfOrder = index + 1
			break
		}
	}

	if selfOrder == 0 {
		err := errors.New("cant find self in cluster")
		logger.Log.Error("error while rebalance, self = %v, err: %v", string(h.id), err)
		panic(err)
	} else {
		newInfo := &membership.Model{
			MemberNumber: selfOrder,
			TotalMembers: len(instances),
		}

		if newInfo.IsChanged(h.info) {
			logger.Log.Debug("new info arrived for member: %v/%v", newInfo.MemberNumber, newInfo.TotalMembers)

			h.bus.Publish(helpers.MembershipChangedBusEventName, newInfo)
		}

		h.lastActiveInstances = instances
	}
}

func (h *lilCbMembership) isClusterChanged(currentActiveInstances []Instance) bool {
	if len(h.lastActiveInstances) != len(currentActiveInstances) {
		return true
	}

	for i := range h.lastActiveInstances {
		if *h.lastActiveInstances[i].ID != *currentActiveInstances[i].ID {
			return true
		}
	}

	return false
}

func (h *lilCbMembership) isAlive(heartbeatTime int64) bool {
	upperWaitLimit := h.membershipConfig.HeartbeatInterval.Nanoseconds() + h.membershipConfig.HeartbeatToleranceDuration.Nanoseconds()
	passedTimeSinceLastHeartbeat := time.Now().UnixNano() - heartbeatTime
	logger.Log.Debug(
		"passed seconds since last heartbeat: %v, upper wait limit second: %v",
		passedTimeSinceLastHeartbeat/1000000000, upperWaitLimit/1000000000,
	)
	return passedTimeSinceLastHeartbeat < upperWaitLimit
}

func NewLilCBMembership(config *config.Dcp, client Client, bus EventBus.Bus) membership.Membership {
	if !config.IsCouchbaseMetadata() {
		err := errors.New("unsupported metadata type")
		logger.Log.Error("error while initialize lil couchbase membership, err: %v", err)
		panic(err)
	}

	couchbaseMetadataConfig := config.GetCouchbaseMetadata()

	lcbm := &lilCbMembership{
		infoChan:         make(chan *membership.Model),
		client:           client,
		id:               []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":" + uuid.New().String()),
		instanceAll:      []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":lil"),
		bus:              bus,
		scopeName:        couchbaseMetadataConfig.Scope,
		collectionName:   couchbaseMetadataConfig.Collection,
		membershipConfig: config.GetCouchbaseMembership(),
		config:           config,
	}

	lcbm.register()

	lcbm.startHeartbeat()
	lcbm.startMonitor()

	err := bus.SubscribeAsync(helpers.MembershipChangedBusEventName, lcbm.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("error while subscribe membership changed event, err: %v", err)
		panic(err)
	}

	return lcbm
}
