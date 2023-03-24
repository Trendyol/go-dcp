package membership

import (
	"context"
	"errors"
	"sort"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/Trendyol/go-dcp-client/dcp"

	"github.com/google/uuid"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/membership/info"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMembership struct {
	client              dcp.Client
	handler             info.Handler
	info                *info.Model
	infoChan            chan *info.Model
	id                  []byte
	lastActiveInstances []*Instance
	clusterJoinTime     int64
	scopeName           string
	collectionName      string
	config              *helpers.Config
	instanceAll         []byte
	heartbeatTicker     *time.Ticker
	monitorTicker       *time.Ticker
}

type Instance struct {
	ID              *string `json:"id,omitempty"`
	Type            string  `json:"type"`
	HeartbeatTime   int64   `json:"heartbeatTime"`
	ClusterJoinTime int64   `json:"clusterJoinTime"`
}

const (
	_type                  = "instance"
	_expirySec             = 2
	_heartbeatIntervalSec  = 1
	_heartbeatToleranceSec = 2
	_monitorIntervalMs     = 500
	_timeoutSec            = 10
)

func (h *cbMembership) GetInfo() *info.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func (h *cbMembership) register() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	now := time.Now().UnixNano()

	err := h.createIndex(ctx, now)
	if err != nil {
		logger.Panic(err, "error while create index")
		return
	}

	h.clusterJoinTime = now

	instance := Instance{
		Type:            _type,
		HeartbeatTime:   now,
		ClusterJoinTime: now,
	}

	err = h.client.UpdateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)

	var kvErr *gocbcore.KeyValueError
	if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
		err = h.client.CreateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)

		if err == nil {
			err = h.client.UpdateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)
		}
	}

	if err != nil {
		logger.Panic(err, "error while register")
		return
	}
}

func (h *cbMembership) createIndex(ctx context.Context, clusterJoinTime int64) error {
	err := h.client.CreatePath(ctx, h.scopeName, h.collectionName, h.instanceAll, h.id, clusterJoinTime)

	var kvErr *gocbcore.KeyValueError
	if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
		err = h.client.CreateDocument(ctx, h.scopeName, h.collectionName, h.instanceAll, struct{}{}, 0)

		if err == nil {
			err = h.client.CreatePath(ctx, h.scopeName, h.collectionName, h.instanceAll, h.id, clusterJoinTime)
		}
	}

	return err
}

func (h *cbMembership) isClusterChanged(currentActiveInstances []*Instance) bool {
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

func (h *cbMembership) heartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	instance := &Instance{
		Type:            _type,
		HeartbeatTime:   time.Now().UnixNano(),
		ClusterJoinTime: h.clusterJoinTime,
	}

	err := h.client.UpdateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)
	if err != nil {
		logger.Error(err, "error while heartbeat")
		return
	}
}

func (h *cbMembership) isAlive(heartbeatTime int64) bool {
	return (time.Now().UnixNano() - heartbeatTime) < heartbeatTime+(_heartbeatToleranceSec*1000*1000*1000)
}

func (h *cbMembership) monitor() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	data, err := h.client.Get(ctx, h.scopeName, h.collectionName, h.instanceAll)
	if err != nil {
		logger.Error(err, "error while monitor try to get index")
		return
	}

	all := map[string]int64{}

	err = jsoniter.Unmarshal(data, &all)
	if err != nil {
		logger.Error(err, "error while monitor try to unmarshal index")
		return
	}

	ids := make([]string, 0, len(all))

	for k := range all {
		ids = append(ids, k)
	}
	sort.SliceStable(ids, func(i, j int) bool {
		return all[ids[i]] < all[ids[j]]
	})

	var instances []*Instance

	for _, id := range ids {
		doc, err := h.client.Get(ctx, h.scopeName, h.collectionName, []byte(id))
		var kvErr *gocbcore.KeyValueError
		if err != nil {
			if errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
				continue
			} else {
				logger.Panic(err, "error while monitor try to get instance")
			}
		}

		copyID := id
		instance := &Instance{ID: &copyID}
		err = jsoniter.Unmarshal(doc, instance)

		if err != nil {
			logger.Panic(err, "error while monitor try to unmarshal instance %v", string(doc))
		}

		if h.isAlive(instance.HeartbeatTime) {
			instances = append(instances, instance)
		} else {
			logger.Debug("instance %v is not alive", instance.ID)
		}
	}

	if h.isClusterChanged(instances) {
		h.rebalance(instances)
		h.updateIndex(ctx)
	}
}

func (h *cbMembership) updateIndex(ctx context.Context) {
	all := map[string]int64{}

	for _, instance := range h.lastActiveInstances {
		all[*instance.ID] = instance.ClusterJoinTime
	}

	err := h.client.UpdateDocument(ctx, h.scopeName, h.collectionName, h.instanceAll, all, 0)
	if err != nil {
		logger.Error(err, "error while update instances")
		return
	}
}

func (h *cbMembership) rebalance(instances []*Instance) {
	selfOrder := 0

	for index, instance := range instances {
		if *instance.ID == string(h.id) {
			selfOrder = index + 1
			break
		}
	}

	if selfOrder == 0 {
		logger.Panic(errors.New("cant find self in cluster"), "error while rebalance, self = %v", string(h.id))
	} else {
		h.handler.OnModelChange(&info.Model{
			MemberNumber: selfOrder,
			TotalMembers: len(instances),
		})

		h.lastActiveInstances = instances
	}
}

func (h *cbMembership) startHeartbeat() {
	h.heartbeatTicker = time.NewTicker(_heartbeatIntervalSec * time.Second)

	go func() {
		for range h.heartbeatTicker.C {
			h.heartbeat()
		}
	}()
}

func (h *cbMembership) startMonitor() {
	h.monitorTicker = time.NewTicker(_monitorIntervalMs * time.Millisecond)

	go func() {
		logger.Info("couchbase membership will start after %v", h.config.Dcp.Group.Membership.RebalanceDelay)
		time.Sleep(h.config.Dcp.Group.Membership.RebalanceDelay)

		for range h.monitorTicker.C {
			h.monitor()
		}
	}()
}

func (h *cbMembership) Close() {
	close(h.infoChan)
	h.monitorTicker.Stop()
	h.heartbeatTicker.Stop()
}

func NewCBMembership(config *helpers.Config, client dcp.Client, handler info.Handler) Membership {
	if !config.IsCouchbaseMetadata() {
		logger.Panic(
			errors.New("unsupported metadata type"),
			"cannot initialize couchbase membership",
		)
	}

	_, scope, collection := config.GetCouchbaseMetadata()

	cbm := &cbMembership{
		infoChan:       make(chan *info.Model),
		client:         client,
		id:             []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":" + uuid.New().String()),
		instanceAll:    []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":all"),
		handler:        handler,
		scopeName:      scope,
		collectionName: collection,
		config:         config,
	}

	cbm.register()

	cbm.startHeartbeat()
	cbm.startMonitor()

	handler.Subscribe(func(new *info.Model) {
		cbm.info = new
		go func() {
			cbm.infoChan <- new
		}()
	})

	return cbm
}
