package stream

import (
	"context"
	"sync"

	"github.com/Trendyol/go-dcp-client/leaderelector"

	"github.com/Trendyol/go-dcp-client/models"

	"github.com/Trendyol/go-dcp-client/kubernetes"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
)

const (
	KubernetesLeaderElectionType = "kubernetes"
)

type LeaderElection interface {
	Start()
	Stop()
}

type leaderElection struct {
	rpcServer        servicediscovery.Server
	serviceDiscovery servicediscovery.ServiceDiscovery
	bus              helpers.Bus
	myIdentity       *models.Identity
	config           *helpers.Config
	newLeaderLock    *sync.Mutex
}

func (l *leaderElection) OnBecomeLeader() {
	l.serviceDiscovery.BeLeader()
	l.serviceDiscovery.RemoveLeader()
}

func (l *leaderElection) OnResignLeader() {
	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
}

func (l *leaderElection) OnBecomeFollower(leaderIdentity *models.Identity) {
	l.newLeaderLock.Lock()
	defer l.newLeaderLock.Unlock()

	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
	l.serviceDiscovery.RemoveLeader()

	leaderClient, err := servicediscovery.NewClient(l.config.LeaderElection.RPC.Port, l.myIdentity, leaderIdentity)
	if err != nil {
		return
	}

	leaderService := servicediscovery.NewService(leaderClient, leaderIdentity.Name)

	l.serviceDiscovery.AssignLeader(leaderService)

	err = leaderClient.Register()
	if err != nil {
		logger.ErrorLog.Printf("error while registering leader client: %v", err)
		panic(err)
	}
}

func (l *leaderElection) Start() {
	l.rpcServer = servicediscovery.NewServer(l.config.LeaderElection.RPC.Port, l.myIdentity, l.serviceDiscovery)
	l.rpcServer.Listen()

	var elector leaderelector.LeaderElector

	if l.config.LeaderElection.Type == KubernetesLeaderElectionType {
		kubernetesClient := kubernetes.NewClient(l.myIdentity)
		elector = kubernetes.NewLeaderElector(kubernetesClient, l.config, l.myIdentity, l, l.bus)
	}

	elector.Run(context.Background())
}

func (l *leaderElection) Stop() {
	l.rpcServer.Shutdown()
}

func NewLeaderElection(
	config *helpers.Config,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	bus helpers.Bus,
) LeaderElection {
	return &leaderElection{
		config:           config,
		serviceDiscovery: serviceDiscovery,
		newLeaderLock:    &sync.Mutex{},
		myIdentity:       models.NewIdentityFromEnv(),
		bus:              bus,
	}
}
