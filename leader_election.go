package godcpclient

import (
	"context"
	"sync"

	"github.com/Trendyol/go-dcp-client/kubernetes"
	"github.com/Trendyol/go-dcp-client/membership/info"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/identity"

	"github.com/Trendyol/go-dcp-client/helpers"
	kle "github.com/Trendyol/go-dcp-client/kubernetes/leaderelector"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
)

type LeaderElection interface {
	Start()
	Stop()
}

type leaderElection struct {
	rpcServer        servicediscovery.Server
	serviceDiscovery servicediscovery.ServiceDiscovery
	infoHandler      info.Handler
	myIdentity       *identity.Identity
	config           helpers.ConfigLeaderElection
	newLeaderLock    sync.Mutex
}

func (l *leaderElection) OnBecomeLeader() {
	l.serviceDiscovery.BeLeader()
	l.serviceDiscovery.RemoveLeader()
}

func (l *leaderElection) OnResignLeader() {
	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
}

func (l *leaderElection) OnBecomeFollower(leaderIdentity *identity.Identity) {
	l.newLeaderLock.Lock()
	defer l.newLeaderLock.Unlock()

	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
	l.serviceDiscovery.RemoveLeader()

	leaderClient, err := servicediscovery.NewClient(l.config.RPC.Port, l.myIdentity, leaderIdentity)
	if err != nil {
		return
	}

	leaderService := servicediscovery.NewService(leaderClient, leaderIdentity.Name)

	l.serviceDiscovery.AssignLeader(leaderService)

	err = leaderClient.Register()

	if err != nil {
		logger.Panic(err, "error while registering leader client")
	}
}

func (l *leaderElection) Start() {
	l.rpcServer = servicediscovery.NewServer(l.config.RPC.Port, l.myIdentity, l.serviceDiscovery)
	l.rpcServer.Listen()

	var elector kle.LeaderElector

	if l.config.Type == helpers.KubernetesLeaderElectionType {
		kubernetesClient := kubernetes.NewClient(l.myIdentity)
		elector = kle.NewLeaderElector(kubernetesClient, l.config, l.myIdentity, l, l.infoHandler)
	}

	elector.Run(context.Background())
}

func (l *leaderElection) Stop() {
	l.rpcServer.Shutdown()
}

func NewLeaderElection(
	config helpers.ConfigLeaderElection,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	infoHandler info.Handler,
) LeaderElection {
	return &leaderElection{
		config:           config,
		serviceDiscovery: serviceDiscovery,
		newLeaderLock:    sync.Mutex{},
		myIdentity:       identity.NewIdentityFromEnv(),
		infoHandler:      infoHandler,
	}
}
