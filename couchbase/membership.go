package couchbase

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/membership"

	"github.com/json-iterator/go"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type cbMembership struct {
	client              Client
	bus                 helpers.Bus
	info                *membership.Model
	infoChan            chan *membership.Model
	heartbeatTicker     *time.Ticker
	config              *config.Dcp
	monitorTicker       *time.Ticker
	scopeName           string
	collectionName      string
	lastActiveInstances []*Instance
	instanceAll         []byte
	id                  []byte
	clusterJoinTime     int64
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

func (h *cbMembership) GetInfo() *membership.Model {
	if h.info != nil {
		return h.info
	}

	return <-h.infoChan
}

func (h *cbMembership) get(ctx context.Context, scopeName string, collectionName string, id []byte) ([]byte, error) {
	opm := NewAsyncOp(context.Background())

	deadline, _ := ctx.Deadline()

	errorCh := make(chan error)
	documentCh := make(chan []byte)

	op, err := h.client.GetMetaAgent().Get(gocbcore.GetOptions{
		Key:            id,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.GetResult, err error) {
		opm.Resolve()

		if err == nil {
			documentCh <- result.Value
		} else {
			documentCh <- nil
		}

		errorCh <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return nil, err
	}

	document := <-documentCh
	err = <-errorCh

	return document, err
}

func (h *cbMembership) createPath(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	path []byte,
	value interface{},
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := h.client.GetMetaAgent().MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpDictSet,
				Value: payload,
				Path:  string(path),
			},
		},
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (h *cbMembership) register() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	now := time.Now().UnixNano()

	err := h.createIndex(ctx, now)
	if err != nil {
		logger.ErrorLog.Printf("error while create index: %v", err)
		panic(err)
	}

	h.clusterJoinTime = now

	instance := Instance{
		Type:            _type,
		HeartbeatTime:   now,
		ClusterJoinTime: now,
	}

	err = h.updateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)

	var kvErr *gocbcore.KeyValueError
	if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
		err = h.client.CreateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)

		if err == nil {
			err = h.updateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)
		}
	}

	if err != nil {
		logger.ErrorLog.Printf("error while register: %v", err)
		panic(err)
	}
}

func (h *cbMembership) updateDocument(ctx context.Context,
	scopeName string,
	collectionName string,
	id []byte,
	value interface{},
	expiry uint32,
) error {
	opm := NewAsyncOp(ctx)

	deadline, _ := ctx.Deadline()

	payload, _ := jsoniter.Marshal(value)

	ch := make(chan error)

	op, err := h.client.GetMetaAgent().MutateIn(gocbcore.MutateInOptions{
		Key: id,
		Ops: []gocbcore.SubDocOp{
			{
				Op:    memd.SubDocOpSetDoc,
				Value: payload,
			},
		},
		Expiry:         expiry,
		Deadline:       deadline,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, func(result *gocbcore.MutateInResult, err error) {
		opm.Resolve()

		ch <- err
	})

	err = opm.Wait(op, err)

	if err != nil {
		return err
	}

	err = <-ch

	return err
}

func (h *cbMembership) createIndex(ctx context.Context, clusterJoinTime int64) error {
	err := h.createPath(ctx, h.scopeName, h.collectionName, h.instanceAll, h.id, clusterJoinTime)

	var kvErr *gocbcore.KeyValueError
	if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
		err = h.client.CreateDocument(ctx, h.scopeName, h.collectionName, h.instanceAll, struct{}{}, 0)

		if err == nil {
			err = h.createPath(ctx, h.scopeName, h.collectionName, h.instanceAll, h.id, clusterJoinTime)
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

	err := h.updateDocument(ctx, h.scopeName, h.collectionName, h.id, instance, _expirySec)
	if err != nil {
		logger.ErrorLog.Printf("error while heartbeat: %v", err)
		return
	}
}

func (h *cbMembership) isAlive(heartbeatTime int64) bool {
	return (time.Now().UnixNano() - heartbeatTime) < heartbeatTime+(_heartbeatToleranceSec*1000*1000*1000)
}

func (h *cbMembership) monitor() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	data, err := h.get(ctx, h.scopeName, h.collectionName, h.instanceAll)
	if err != nil {
		logger.ErrorLog.Printf("error while monitor try to get index: %v", err)
		return
	}

	all := map[string]int64{}

	err = jsoniter.Unmarshal(data, &all)
	if err != nil {
		logger.ErrorLog.Printf("error while monitor try to unmarshal index: %v", err)
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
		doc, err := h.get(ctx, h.scopeName, h.collectionName, []byte(id))
		var kvErr *gocbcore.KeyValueError
		if err != nil {
			if errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
				continue
			} else {
				logger.ErrorLog.Printf("error while monitor try to get instance: %v", err)
				panic(err)
			}
		}

		copyID := id
		instance := &Instance{ID: &copyID}
		err = jsoniter.Unmarshal(doc, instance)

		if err != nil {
			logger.ErrorLog.Printf("error while monitor try to unmarshal instance %v, err: %v", string(doc), err)
			panic(err)
		}

		if h.isAlive(instance.HeartbeatTime) {
			instances = append(instances, instance)
		} else {
			logger.Log.Printf("instance %v is not alive", instance.ID)
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

	err := h.updateDocument(ctx, h.scopeName, h.collectionName, h.instanceAll, all, 0)
	if err != nil {
		logger.ErrorLog.Printf("error while update instances: %v", err)
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
		err := errors.New("cant find self in cluster")
		logger.ErrorLog.Printf("error while rebalance, self = %v, err: %v", string(h.id), err)
		panic(err)
	} else {
		h.bus.Emit(helpers.MembershipChangedBusEventName, &membership.Model{
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
		logger.Log.Printf("couchbase membership will start after %v", h.config.Dcp.Group.Membership.RebalanceDelay)
		time.Sleep(h.config.Dcp.Group.Membership.RebalanceDelay)

		for range h.monitorTicker.C {
			h.monitor()
		}
	}()
}

func (h *cbMembership) Close() {
	h.monitorTicker.Stop()
	h.heartbeatTicker.Stop()
}

func (h *cbMembership) membershipChangedListener(event interface{}) {
	model := event.(*membership.Model)

	h.info = model
	go func() {
		h.infoChan <- model
	}()
}

func NewCBMembership(config *config.Dcp, client Client, bus helpers.Bus) membership.Membership {
	if !config.IsCouchbaseMetadata() {
		err := errors.New("unsupported metadata type")
		logger.ErrorLog.Printf("cannot initialize couchbase membership, err: %v", err)
		panic(err)
	}

	_, scope, collection, _, _ := config.GetCouchbaseMetadata()

	cbm := &cbMembership{
		infoChan:       make(chan *membership.Model),
		client:         client,
		id:             []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":" + uuid.New().String()),
		instanceAll:    []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":all"),
		bus:            bus,
		scopeName:      scope,
		collectionName: collection,
		config:         config,
	}

	cbm.register()

	cbm.startHeartbeat()
	cbm.startMonitor()

	bus.Subscribe(helpers.MembershipChangedBusEventName, cbm.membershipChangedListener)

	return cbm
}
