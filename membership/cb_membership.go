package membership

import (
	"context"
	"errors"
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
	info                *info.Model
	infoChan            chan *info.Model
	client              dcp.Client
	id                  []byte
	clusterJoinTime     int64
	lastActiveInstances []Instance
	handler             info.Handler
	monitorQuery        []byte
	indexQuery          []byte
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

	h.clusterJoinTime = now

	instance := Instance{
		Type:            _type,
		HeartbeatTime:   now,
		ClusterJoinTime: now,
	}

	err := h.client.UpdateDocument(ctx, h.id, instance, _expirySec)

	var kvErr *gocbcore.KeyValueError
	if err != nil && errors.As(err, &kvErr) && kvErr.StatusCode == memd.StatusKeyNotFound {
		err = h.client.CreateDocument(ctx, h.id, instance, _expirySec)

		if err == nil {
			err = h.client.UpdateDocument(ctx, h.id, instance, _expirySec)
		}
	}

	if err != nil {
		logger.Panic(err, "error while register")
		return
	}
}

func (h *cbMembership) isClusterChanged(currentActiveInstances []Instance) bool {
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

	instance := Instance{
		Type:            _type,
		HeartbeatTime:   time.Now().UnixNano(),
		ClusterJoinTime: h.clusterJoinTime,
	}

	err := h.client.UpdateDocument(ctx, h.id, instance, _expirySec)
	if err != nil {
		logger.Error(err, "error while heartbeat")
		return
	}
}

func (h *cbMembership) createIndex() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	_, err := h.client.ExecuteQuery(ctx, h.indexQuery)
	if err != nil {
		logger.Panic(err, "error while create index")
	}
}

func (h *cbMembership) isAlive(heartbeatTime int64) bool {
	return (time.Now().UnixNano() - heartbeatTime) < heartbeatTime+(_heartbeatToleranceSec*1000*1000*1000)
}

func (h *cbMembership) monitor() {
	ctx, cancel := context.WithTimeout(context.Background(), _timeoutSec*time.Second)
	defer cancel()

	rows, err := h.client.ExecuteQuery(ctx, h.monitorQuery)
	if err != nil {
		logger.Debug("error while monitor %v", err)
	}

	var instances []Instance

	for _, row := range rows {
		var instance Instance
		err = jsoniter.Unmarshal(row, &instance)

		if err != nil {
			logger.Error(err, "cannot unmarshal %v", string(row))
			continue
		}

		if h.isAlive(instance.HeartbeatTime) {
			instances = append(instances, instance)
		} else {
			logger.Debug("instance %v is not alive", instance.ID)
		}
	}

	if h.isClusterChanged(instances) {
		h.rebalance(instances)
	}
}

func getMonitorQuery(metadataBucket string) []byte {
	var query []byte

	query = append(query, []byte("SELECT meta().id, type, heartbeatTime, clusterJoinTime FROM ")...)
	query = append(query, []byte("`")...)
	query = append(query, []byte(metadataBucket)...)
	query = append(query, []byte("`")...)
	query = append(query, []byte(" WHERE type = '")...)
	query = append(query, []byte(_type)...)
	query = append(query, []byte("' ")...)
	query = append(query, []byte("order by clusterJoinTime")...)

	return query
}

func getIndexQuery(metadataBucket string) []byte {
	var query []byte

	query = append(query, []byte("CREATE INDEX ")...)
	query = append(query, []byte("ids_metadata_instance IF NOT EXISTS on ")...)
	query = append(query, []byte("`")...)
	query = append(query, []byte(metadataBucket)...)
	query = append(query, []byte("`(`type`)")...)
	query = append(query, []byte(" where type = '")...)
	query = append(query, []byte(_type)...)
	query = append(query, []byte("'")...)

	return query
}

func (h *cbMembership) rebalance(instances []Instance) {
	selfOrder := 0

	for index, instance := range instances {
		if *instance.ID == string(h.id) {
			selfOrder = index + 1
			break
		}
	}

	if selfOrder == 0 {
		logger.Panic(errors.New("cant find self in cluster"), "error while rebalance")
	} else {
		h.handler.OnModelChange(&info.Model{
			MemberNumber: selfOrder,
			TotalMembers: len(instances),
		})

		h.lastActiveInstances = instances
	}
}

func (h *cbMembership) startHeartbeat() {
	heartbeatTicker := time.NewTicker(_heartbeatIntervalSec * time.Second)

	go func() {
		for range heartbeatTicker.C {
			h.heartbeat()
		}
	}()
}

func (h *cbMembership) startMonitor() {
	monitorTicker := time.NewTicker(_monitorIntervalMs * time.Millisecond)

	go func() {
		for range monitorTicker.C {
			h.monitor()
		}
	}()
}

func NewCBMembership(config helpers.Config, client dcp.Client, handler info.Handler) Membership {
	cbm := &cbMembership{
		infoChan:     make(chan *info.Model),
		client:       client,
		id:           []byte(helpers.Prefix + config.Dcp.Group.Name + ":" + _type + ":" + uuid.New().String()),
		handler:      handler,
		monitorQuery: getMonitorQuery(config.MetadataBucket),
		indexQuery:   getIndexQuery(config.MetadataBucket),
	}

	cbm.createIndex()
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
