package godcpclient

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
}

type dcp struct {
	client           gDcp.Client
	metadata         Metadata
	stream           Stream
	api              API
	leaderElection   LeaderElection
	vBucketDiscovery VBucketDiscovery
	serviceDiscovery servicediscovery.ServiceDiscovery
	listener         models.Listener
	config           helpers.Config
	apiShutdown      chan struct{}
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

func (s *dcp) Start() {
	infoHandler := info.NewHandler()

	s.serviceDiscovery = servicediscovery.NewServiceDiscovery(infoHandler)
	s.serviceDiscovery.StartHealthCheck()
	s.serviceDiscovery.StartRebalance()

	vBuckets := s.client.GetNumVBuckets()

	s.vBucketDiscovery = NewVBucketDiscovery(s.client, s.config, vBuckets, infoHandler)

	s.stream = NewStream(s.client, s.metadata, s.config, s.vBucketDiscovery, s.listener, s.getCollectionIDs())

	if s.config.LeaderElection.Enabled {
		s.leaderElection = NewLeaderElection(s.config.LeaderElection, s.stream, s.serviceDiscovery, infoHandler)
		s.leaderElection.Start()
	}

	s.stream.Open()

	infoHandler.Subscribe(func(new *info.Model) {
		s.stream.Rebalance()
	})

	if s.config.Metric.Enabled {
		go func() {
			go func() {
				<-s.apiShutdown
				s.api.Shutdown()
			}()

			s.api = NewAPI(s.config, s.client, s.stream, s.serviceDiscovery)
			s.api.Listen()
		}()
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	logger.Info("dcp stream started")

	go func() {
		s.stream.Wait()
		close(cancelCh)
	}()

	<-cancelCh
}

func (s *dcp) Close() {
	s.serviceDiscovery.StopRebalance()
	s.serviceDiscovery.StopHealthCheck()

	if s.config.LeaderElection.Enabled {
		s.leaderElection.Stop()
	}

	if s.api != nil && s.config.Metric.Enabled {
		s.apiShutdown <- struct{}{}
	}

	s.stream.Save()
	s.stream.Close(false)
	s.client.DcpClose()
	s.client.MetaClose()
	s.client.Close()

	logger.Info("dcp stream closed")
}

func newDcp(config helpers.Config, listener models.Listener) (Dcp, error) {
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

	err = client.MetaConnect()
	if err != nil {
		return nil, err
	}

	err = client.DcpConnect()

	if err != nil {
		return nil, err
	}

	metadata := NewCBMetadata(client, config)

	return &dcp{
		client:      client,
		metadata:    metadata,
		listener:    listener,
		config:      config,
		apiShutdown: make(chan struct{}, 1),
	}, nil
}

// NewDcp creates a new DCP client
//
//	config: path to a configuration file or a configuration struct
//	listener is a callback function that will be called when a mutation, deletion or expiration event occurs
func NewDcp(config interface{}, listener models.Listener) (Dcp, error) {
	if path, ok := config.(string); ok {
		return newDcp(helpers.NewConfig(helpers.Name, path), listener)
	} else if config, ok := config.(helpers.Config); ok {
		return newDcp(config, listener)
	} else {
		return nil, fmt.Errorf("invalid config type")
	}
}
