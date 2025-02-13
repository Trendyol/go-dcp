package dcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"strings"
	"syscall"

	"github.com/Trendyol/go-dcp/tracing"

	"github.com/Trendyol/go-dcp/metric"

	"github.com/asaskevich/EventBus"

	"github.com/Trendyol/go-dcp/membership"

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
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/servicediscovery"
	"github.com/Trendyol/go-dcp/stream"
)

type Dcp interface {
	WaitUntilReady() chan struct{}
	Start()
	Close()
	Commit()
	GetClient() couchbase.Client
	GetConfig() *config.Dcp
	GetVersion() *couchbase.Version
	SetMetadata(metadata metadata.Metadata)
	SetMetricCollectors(collectors ...prometheus.Collector)
	SetEventHandler(handler models.EventHandler)
}

type dcp struct {
	bus              EventBus.Bus
	stream           stream.Stream
	api              api.API
	leaderElection   stream.LeaderElection
	vBucketDiscovery stream.VBucketDiscovery
	serviceDiscovery servicediscovery.ServiceDiscovery
	metadata         metadata.Metadata
	eventHandler     models.EventHandler
	client           couchbase.Client
	apiShutdown      chan struct{}
	config           *config.Dcp
	version          *couchbase.Version
	bucketInfo       *couchbase.BucketInfo
	healthCheck      couchbase.HealthCheck
	consumer         models.Consumer
	readyCh          chan struct{}
	cancelCh         chan os.Signal
	stopCh           chan struct{}
	metricCollectors []prometheus.Collector
	closeWithCancel  bool
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

func (s *dcp) membershipChangedListener(_ *membership.Model) {
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
			err := errors.New("invalid metadata type")
			logger.Log.Error("error while dcp start, err: %v", err)
			panic(err)
		}
	}

	if s.config.Metadata.ReadOnly {
		s.metadata = metadata.NewReadMetadata(s.metadata)
	}

	logger.Log.Info("using %v metadata", reflect.TypeOf(s.metadata))

	vBuckets := s.client.GetNumVBuckets()

	s.vBucketDiscovery = stream.NewVBucketDiscovery(s.client, s.config, vBuckets, s.bus)

	tc := tracing.NewTracerComponent()

	collectionIDs, err := s.client.GetCollectionIDs(s.config.ScopeName, s.config.CollectionNames)
	if err != nil {
		logger.Log.Error("error while getting vBucket seqNos, err: %v", err)
		panic(err)
	}

	s.stream = stream.NewStream(
		s.client, s.metadata, s.config, s.version, s.bucketInfo, s.vBucketDiscovery,
		s.consumer, collectionIDs, s.stopCh, s.bus, s.eventHandler,
		tc,
	)

	if s.config.LeaderElection.Enabled {
		s.serviceDiscovery = servicediscovery.NewServiceDiscovery(s.config, s.bus)
		s.serviceDiscovery.StartHeartbeat()
		s.serviceDiscovery.StartMonitor()

		s.leaderElection = stream.NewLeaderElection(s.config, s.serviceDiscovery, s.bus)
		s.leaderElection.Start()
	}

	if !s.config.API.Disabled {
		go func() {
			go func() {
				<-s.apiShutdown
				s.api.Shutdown()
			}()

			s.metricCollectors = append(s.metricCollectors, metric.NewMetricCollector(s.client, s.stream, s.vBucketDiscovery))
			s.api = api.NewAPI(s.config, s.client, s.stream, s.serviceDiscovery, s.metricCollectors, s.bus)
			s.api.Listen()
		}()
	}

	s.stream.Open()

	err = s.bus.SubscribeAsync(helpers.MembershipChangedBusEventName, s.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("error while subscribe to membership changed event, err: %v", err)
		panic(err)
	}

	signal.Notify(s.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	if !s.config.HealthCheck.Disabled {
		s.healthCheck = couchbase.NewHealthCheck(&s.config.HealthCheck, s.client)
		s.healthCheck.Start()
	}

	logger.Log.Info("dcp stream started")

	s.readyCh <- struct{}{}

	select {
	case <-s.stopCh:
		logger.Log.Debug("stop channel triggered")
	case <-s.cancelCh:
		logger.Log.Debug("cancel channel triggered")
		s.closeWithCancel = true
	}

	s.close()
}

func (s *dcp) GetClient() couchbase.Client {
	return s.client
}

func (s *dcp) WaitUntilReady() chan struct{} {
	return s.readyCh
}

func (s *dcp) Close() {
	s.cancelCh <- syscall.SIGTERM
}

func (s *dcp) close() {
	if !s.config.HealthCheck.Disabled {
		s.healthCheck.Stop()
	}
	s.vBucketDiscovery.Close()

	if s.config.Checkpoint.Type == stream.CheckpointTypeAuto {
		s.stream.Save()
	}

	err := s.bus.Unsubscribe(helpers.MembershipChangedBusEventName, s.membershipChangedListener)
	if err != nil {
		logger.Log.Error("cannot while unsubscribe: %v", err)
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

	if s.api != nil && !s.config.API.Disabled {
		s.api.UnregisterMetricCollectors()
	}

	s.metricCollectors = []prometheus.Collector{}

	logger.Log.Info("dcp stream closed")
}

func (s *dcp) Commit() {
	s.stream.Save()
}

func (s *dcp) GetConfig() *config.Dcp {
	return s.config
}

func (s *dcp) GetVersion() *couchbase.Version {
	return s.version
}

func newDcp(config *config.Dcp, consumer models.Consumer) (Dcp, error) {
	config.ApplyDefaults()
	copyOfConfig := config
	printConfiguration(*copyOfConfig)

	client := couchbase.NewClient(config)

	err := client.Connect()
	if err != nil {
		return nil, err
	}

	httpClient := couchbase.NewHTTPClient(config, client)

	err = httpClient.Connect()
	if err != nil {
		return nil, err
	}

	version, err := httpClient.GetVersion()
	if err != nil {
		return nil, err
	}

	bucketInfo, err := httpClient.GetBucketInfo()
	if err != nil {
		return nil, err
	}

	var useExpiryOpcode bool
	var useChangeStreams bool

	if version.Higher(couchbase.SrvVer650) || version.Equal(couchbase.SrvVer650) {
		useExpiryOpcode = true
	}

	if bucketInfo.IsMagma() && (version.Higher(couchbase.SrvVer720) || version.Equal(couchbase.SrvVer720)) {
		useChangeStreams = true
	}

	err = client.DcpConnect(useExpiryOpcode, useChangeStreams)
	if err != nil {
		return nil, err
	}

	return &dcp{
		client:           client,
		consumer:         consumer,
		config:           config,
		version:          version,
		bucketInfo:       bucketInfo,
		apiShutdown:      make(chan struct{}, 1),
		cancelCh:         make(chan os.Signal, 1),
		stopCh:           make(chan struct{}, 1),
		readyCh:          make(chan struct{}, 1),
		metricCollectors: []prometheus.Collector{},
		eventHandler:     models.DefaultEventHandler,
		bus:              EventBus.New(),
	}, nil
}

type simplifiedConsumer struct {
	listener models.Listener
}

func NewSimpleConsumer(listener models.Listener) models.Consumer {
	return &simplifiedConsumer{listener: listener}
}

func (s *simplifiedConsumer) ConsumeEvent(ctx *models.ListenerContext) {
	s.listener(ctx)
}

func (s *simplifiedConsumer) TrackOffset(vbID uint16, offset *models.Offset) {}

// NewExtendedDcp creates a new Dcp client
//
// config: path to a configuration file or a configuration struct
// consumer must implement models.Consumer interface containg both ConsumeEvent and TrackOffset methods
func NewExtendedDcp(cfg any, consumer models.Consumer) (Dcp, error) {
	switch v := cfg.(type) {
	case *config.Dcp:
		return newDcp(v, consumer)
	case config.Dcp:
		return newDcp(&v, consumer)
	case string:
		return newDcpWithPath(v, consumer)
	default:
		return nil, errors.New("invalid config")
	}
}

// NewDcp creates a new Dcp client
//
// config: path to a configuration file or a configuration struct
// listener is a callback function that will be called when a mutation, deletion or expiration event occurs
func NewDcp(cfg any, listener models.Listener) (Dcp, error) {
	switch v := cfg.(type) {
	case *config.Dcp:
		return newDcp(v, NewSimpleConsumer(listener))
	case config.Dcp:
		return newDcp(&v, NewSimpleConsumer(listener))
	case string:
		return newDcpWithPath(v, NewSimpleConsumer(listener))
	default:
		return nil, errors.New("invalid config")
	}
}

func newDcpWithPath(path string, consumer models.Consumer) (Dcp, error) {
	c, err := newDcpConfig(path)
	if err != nil {
		return nil, err
	}
	return newDcp(&c, consumer)
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

	envPattern := regexp.MustCompile(`\${([^}]+)}`)
	matches := envPattern.FindAllStringSubmatch(string(file), -1)
	for _, match := range matches {
		envVar := match[1]
		if value, exists := os.LookupEnv(envVar); exists {
			updatedFile := strings.ReplaceAll(string(file), "${"+envVar+"}", value)
			file = []byte(updatedFile)
		}
	}

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
	configJSON, _ := jsoniter.Marshal(config)

	dst := &bytes.Buffer{}
	if err := json.Compact(dst, configJSON); err != nil {
		logger.Log.Error("error while print configuration, err: %v", err)
		panic(err)
	}

	logger.Log.Info("using config: %v", dst.String())
}
