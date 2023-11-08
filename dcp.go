package dcp

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	jsoniter "github.com/json-iterator/go"

	"gopkg.in/yaml.v3"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Trendyol/go-dcp/api"
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/metadata"
	"github.com/Trendyol/go-dcp/metric"
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/servicediscovery"
	"github.com/Trendyol/go-dcp/stream"
)

type Dcp interface {
	WaitUntilReady() chan struct{}
	Start()
	Close()
	Commit()
	GetConfig() *config.Dcp
	SetMetadata(metadata metadata.Metadata)
	SetMetricCollectors(collectors ...prometheus.Collector)
	SetEventHandler(handler models.EventHandler)
}

type dcp struct {
	client            couchbase.Client
	stream            stream.Stream
	api               api.API
	leaderElection    stream.LeaderElection
	vBucketDiscovery  stream.VBucketDiscovery
	serviceDiscovery  servicediscovery.ServiceDiscovery
	metadata          metadata.Metadata
	eventHandler      models.EventHandler
	apiShutdown       chan struct{}
	stopCh            chan struct{}
	healCheckFailedCh chan struct{}
	config            *config.Dcp
	healthCheckTicker *time.Ticker
	listener          models.Listener
	readyCh           chan struct{}
	cancelCh          chan os.Signal
	metricCollectors  []prometheus.Collector
	closeWithCancel   bool
}

func (s *dcp) startHealthCheck() {
	s.healthCheckTicker = time.NewTicker(s.config.HealthCheck.Interval)

	go func() {
		for range s.healthCheckTicker.C {
			if err := s.client.Ping(); err != nil {
				logger.Log.Error("health check failed: %v", err)
				s.healthCheckTicker.Stop()
				s.healCheckFailedCh <- struct{}{}
				break
			}
		}
	}()
}

func (s *dcp) stopHealthCheck() {
	s.healthCheckTicker.Stop()
}

func (s *dcp) SetMetadata(metadata metadata.Metadata) {
	s.metadata = metadata
}

func (s *dcp) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	s.metricCollectors = append(s.metricCollectors, metricCollectors...)
}

func (s *dcp) SetEventHandler(eventHandler models.EventHandler) {
	s.eventHandler = eventHandler
}

func (s *dcp) membershipChangedListener(_ interface{}) {
	s.stream.Rebalance()
}

//nolint:funlen
func (s *dcp) Start() {
	if s.metadata == nil {
		switch {
		case s.config.IsCouchbaseMetadata():
			s.metadata = couchbase.NewCBMetadata(s.client, s.config)
		case s.config.IsFileMetadata():
			s.metadata = metadata.NewFSMetadata(s.config)
		default:
			panic(errors.New("invalid metadata type"))
		}
	}

	if s.config.Metadata.ReadOnly {
		s.metadata = metadata.NewReadMetadata(s.metadata)
	}

	logger.Log.Info("using %v metadata", reflect.TypeOf(s.metadata))

	bus := helpers.NewBus()

	vBuckets := s.client.GetNumVBuckets()

	s.vBucketDiscovery = stream.NewVBucketDiscovery(s.client, s.config, vBuckets, bus)

	s.stream = stream.NewStream(
		s.client, s.metadata, s.config, s.vBucketDiscovery,
		s.listener, s.client.GetCollectionIDs(s.config.ScopeName, s.config.CollectionNames), s.stopCh, bus, s.eventHandler,
	)

	if s.config.LeaderElection.Enabled {
		s.serviceDiscovery = servicediscovery.NewServiceDiscovery(s.config, bus)
		s.serviceDiscovery.StartHeartbeat()
		s.serviceDiscovery.StartMonitor()

		s.leaderElection = stream.NewLeaderElection(s.config, s.serviceDiscovery, bus)
		s.leaderElection.Start()
	}

	s.stream.Open()

	bus.Subscribe(helpers.MembershipChangedBusEventName, s.membershipChangedListener)

	if !s.config.API.Disabled {
		go func() {
			go func() {
				<-s.apiShutdown
				s.api.Shutdown()
			}()

			s.metricCollectors = append(s.metricCollectors, metric.NewMetricCollector(s.client, s.stream, s.vBucketDiscovery))
			s.api = api.NewAPI(s.config, s.client, s.stream, s.serviceDiscovery, s.metricCollectors)
			s.api.Listen()
		}()
	}

	signal.Notify(s.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	if !s.config.HealthCheck.Disabled {
		s.startHealthCheck()
	}

	logger.Log.Info("dcp stream started")

	s.readyCh <- struct{}{}

	select {
	case <-s.stopCh:
	case <-s.cancelCh:
		s.closeWithCancel = true
	case <-s.healCheckFailedCh:
	}
}

func (s *dcp) WaitUntilReady() chan struct{} {
	return s.readyCh
}

func (s *dcp) Close() {
	if !s.config.HealthCheck.Disabled {
		s.stopHealthCheck()
	}
	s.vBucketDiscovery.Close()

	if s.config.Checkpoint.Type == stream.CheckpointTypeAuto {
		s.stream.Save()
	}
	s.stream.Close(s.closeWithCancel)

	if s.config.LeaderElection.Enabled {
		s.leaderElection.Stop()

		s.serviceDiscovery.StopMonitor()
		s.serviceDiscovery.StopHeartbeat()
	}

	if s.api != nil && !s.config.API.Disabled {
		s.apiShutdown <- struct{}{}
	}

	s.client.DcpClose()
	s.client.Close()

	s.api.UnregisterMetricCollectors()
	s.metricCollectors = []prometheus.Collector{}

	logger.Log.Info("dcp stream closed")
}

func (s *dcp) Commit() {
	s.stream.Save()
}

func (s *dcp) GetConfig() *config.Dcp {
	return s.config
}

func newDcp(config *config.Dcp, listener models.Listener) (Dcp, error) {
	config.ApplyDefaults()
	copyOfConfig := config
	printConfiguration(*copyOfConfig)

	client := couchbase.NewClient(config)

	err := client.Connect()
	if err != nil {
		return nil, err
	}

	err = client.DcpConnect()

	if err != nil {
		return nil, err
	}

	return &dcp{
		client:            client,
		listener:          listener,
		config:            config,
		apiShutdown:       make(chan struct{}, 1),
		cancelCh:          make(chan os.Signal, 1),
		stopCh:            make(chan struct{}, 1),
		healCheckFailedCh: make(chan struct{}, 1),
		readyCh:           make(chan struct{}, 1),
		metricCollectors:  []prometheus.Collector{},
		eventHandler:      models.DefaultEventHandler,
	}, nil
}

// NewDcp creates a new Dcp client
//
// config: path to a configuration file or a configuration struct
// listener is a callback function that will be called when a mutation, deletion or expiration event occurs
func NewDcp(cfg any, listener models.Listener) (Dcp, error) {
	switch v := cfg.(type) {
	case *config.Dcp:
		return newDcp(v, listener)
	case config.Dcp:
		return newDcp(&v, listener)
	case string:
		return newDcpWithPath(v, listener)
	default:
		return nil, errors.New("invalid config")
	}
}

func newDcpWithPath(path string, listener models.Listener) (Dcp, error) {
	c, err := newDcpConfig(path)
	if err != nil {
		return nil, err
	}
	return newDcp(&c, listener)
}

func newDcpConfig(path string) (config.Dcp, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return config.Dcp{}, err
	}
	var c config.Dcp
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return config.Dcp{}, err
	}
	return c, nil
}

func NewDcpWithLogger(cfg any, listener models.Listener, logrus *logrus.Logger) (Dcp, error) {
	logger.Log = &logger.Loggers{
		Logrus: logrus,
	}
	return NewDcp(cfg, listener)
}

func printConfiguration(config config.Dcp) {
	config.Password = "*****"
	configJSON, _ := jsoniter.MarshalIndent(config, "", "  ")
	fmt.Printf("using config: %v", string(configJSON))
}
