package godcpclient

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Trendyol/go-dcp-client/identity"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/kubernetes"
	"github.com/Trendyol/go-dcp-client/membership/info"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
)

type Dcp interface {
	Start()
	Close()
}

type dcp struct {
	client           Client
	metadata         Metadata
	listener         Listener
	stream           Stream
	config           helpers.Config
	api              API
	leaderElection   LeaderElection
	vBucketDiscovery VBucketDiscovery
	serviceDiscovery servicediscovery.ServiceDiscovery
	kubernetesClient kubernetes.Client
	myIdentity       *identity.Identity
}

func (s *dcp) Start() {
	infoHandler := info.NewHandler()

	if s.config.LeaderElection.Enabled {
		if s.config.LeaderElection.Type == helpers.KubernetesLeaderElectionType {
			s.myIdentity = identity.NewIdentityFromEnv()

			if namespace, exist := s.config.LeaderElection.Config["leaseLockNamespace"]; exist {
				s.kubernetesClient = kubernetes.NewClient(s.myIdentity, namespace)
				infoHandler.Subscribe(func(new *info.Model) {
					s.kubernetesClient.AddLabel("member", fmt.Sprintf("%v_%v", new.MemberNumber, new.TotalMembers))
				})
			}
		}
	}

	s.serviceDiscovery = servicediscovery.NewServiceDiscovery(infoHandler)
	s.serviceDiscovery.StartHealthCheck()
	s.serviceDiscovery.StartRebalance()

	vBuckets := s.client.GetNumVBuckets()

	s.vBucketDiscovery = NewVBucketDiscovery(s.config.Dcp.Group.Membership, vBuckets, infoHandler)

	s.stream = NewStream(s.client, s.metadata, s.config, s.vBucketDiscovery, s.listener)

	if s.config.LeaderElection.Enabled {
		s.leaderElection = NewLeaderElection(s.config.LeaderElection, s.stream, s.serviceDiscovery, s.myIdentity, s.kubernetesClient)
		s.leaderElection.Start()
	}

	s.stream.Open()

	infoHandler.Subscribe(func(new *info.Model) {
		s.stream.Rebalance()
	})

	if s.config.Metric.Enabled {
		s.api = NewAPI(s.config, s.client, s.stream, s.serviceDiscovery)
		s.api.Listen()
	}

	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

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

	if s.config.Metric.Enabled {
		s.api.Shutdown()
	}

	s.stream.Save()
	s.stream.Close(false)
	s.client.DcpClose()
	s.client.Close()
}

func newDcp(config helpers.Config, listener Listener) (Dcp, error) {
	client := NewClient(config)

	err := client.Connect()
	if err != nil {
		return nil, err
	}

	err = client.DcpConnect()

	if err != nil {
		return nil, err
	}

	metadata := NewCBMetadata(client.GetAgent(), config)

	return &dcp{
		client:   client,
		metadata: metadata,
		listener: listener,
		config:   config,
	}, nil
}

// NewDcp creates a new DCP client
//
//	config: path to a configuration file or a configuration struct
//	listener is a callback function that will be called when a mutation, deletion or expiration event occurs
func NewDcp(config interface{}, listener Listener) (Dcp, error) {
	if path, ok := config.(string); ok {
		return newDcp(helpers.NewConfig(helpers.Name, path), listener)
	} else if config, ok := config.(helpers.Config); ok {
		return newDcp(config, listener)
	} else {
		return nil, fmt.Errorf("invalid config type")
	}
}
