package godcpclient

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/rs/zerolog"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership/info"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
)

type Dcp interface {
	Start()
	Close()
	Commit()
}

type dcp struct {
	client            gDcp.Client
	stream            Stream
	api               API
	leaderElection    LeaderElection
	vBucketDiscovery  VBucketDiscovery
	serviceDiscovery  servicediscovery.ServiceDiscovery
	listener          models.Listener
	apiShutdown       chan struct{}
	cancelCh          chan os.Signal
	stopCh            chan struct{}
	healCheckFailedCh chan struct{}
	config            *helpers.Config
}

func (s *dcp) getCollectionIDs() map[uint32]string {
	collectionIDs := map[uint32]string{}

	if s.config.IsCollectionModeEnabled() {
		ids, err := s.client.GetCollectionIDs(s.config.ScopeName, s.config.CollectionNames)
		if err != nil {
			logger.Panic(err, "cannot get collection ids")
		}

		collectionIDs = ids
	}

	return collectionIDs
}

func (s *dcp) healthCheck() {
	timer := time.NewTicker(s.config.HealthCheck.Interval)

	go func() {
		for range timer.C {
			status, err := s.client.Ping()
			if err != nil || !status {
				timer.Stop()
				logger.Error(err, "health check failed")
				s.healCheckFailedCh <- struct{}{}
				break
			}
		}
	}()
}

func (s *dcp) Start() {
	infoHandler := info.NewHandler()

	vBuckets := s.client.GetNumVBuckets()

	s.vBucketDiscovery = NewVBucketDiscovery(s.client, s.config, vBuckets, infoHandler)

	metadata := NewCBMetadata(s.client, s.config)
	s.stream = NewStream(s.client, metadata, s.config, s.vBucketDiscovery, s.listener, s.getCollectionIDs(), s.stopCh)

	if s.config.LeaderElection.Enabled {
		s.serviceDiscovery = servicediscovery.NewServiceDiscovery(s.config, infoHandler)
		s.serviceDiscovery.StartHealthCheck()
		s.serviceDiscovery.StartRebalance()

		s.leaderElection = NewLeaderElection(s.config, s.serviceDiscovery, infoHandler)
		s.leaderElection.Start()
	}

	s.stream.Open()

	infoHandler.Subscribe(func(new *info.Model) {
		s.stream.Rebalance()
	})

	if s.config.API.Enabled {
		go func() {
			go func() {
				<-s.apiShutdown
				s.api.Shutdown()
			}()

			s.api = NewAPI(s.config, s.client, s.stream, s.serviceDiscovery)
			s.api.Listen()
		}()
	}

	signal.Notify(s.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	s.healthCheck()

	logger.Info("dcp stream started")
	select {
	case <-s.stopCh:
	case <-s.cancelCh:
	case <-s.healCheckFailedCh:
	}
}

func (s *dcp) Close() {
	if s.config.Checkpoint.Type == helpers.CheckpointTypeAuto {
		s.stream.Save()
	}
	s.stream.Close()

	if s.config.LeaderElection.Enabled {
		s.leaderElection.Stop()

		s.serviceDiscovery.StopRebalance()
		s.serviceDiscovery.StopHealthCheck()
	}

	if s.api != nil && s.config.API.Enabled {
		s.apiShutdown <- struct{}{}
	}

	s.client.DcpClose()
	s.client.Close()

	logger.Info("dcp stream closed")
}

func (s *dcp) Commit() {
	s.stream.Save()
}

func newDcp(config *helpers.Config, listener models.Listener) (Dcp, error) {
	client := gDcp.NewClient(config)

	loggingLevel, err := zerolog.ParseLevel(config.Logging.Level)
	if err != nil {
		logger.Panic(err, "invalid logging level")
	}

	logger.SetLevel(loggingLevel)

	err = client.Connect()
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
	}, nil
}

// NewDcp creates a new DCP client
//
//	config: path to a configuration file or a configuration struct
//	listener is a callback function that will be called when a mutation, deletion or expiration event occurs
func NewDcp(configPath string, listener models.Listener) (Dcp, error) {
	config := helpers.NewConfig(helpers.Name, configPath)
	return newDcp(config, listener)
}
