package stream

import (
	"context"
	"errors"
	"sync"

	"github.com/asaskevich/EventBus"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/leaderelector"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/kubernetes"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/servicediscovery"
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
	bus              EventBus.Bus
	myIdentity       *models.Identity
	config           *config.Dcp
	newLeaderLock    *sync.Mutex
	elector          leaderelector.LeaderElector
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

	leaderService := servicediscovery.NewService(leaderClient, leaderIdentity.Name, leaderIdentity.ClusterJoinTime)

	l.serviceDiscovery.AssignLeader(leaderService)

	err = leaderClient.Register()
	if err != nil {
		logger.Log.Error("error while registering leader client, err: %v", err)
		panic(err)
	}
}

func (l *leaderElection) Start() {
	var kubernetesClient kubernetes.Client

	if l.config.LeaderElection.Type == KubernetesLeaderElectionType {
		kubernetesClient = kubernetes.NewClient()
		l.myIdentity = kubernetesClient.GetIdentity()
	} else {
		err := errors.New("leader election type is not supported")
		logger.Log.Error("error while leader election: %s, err: %v", l.config.LeaderElection.Type, err)
		panic(err)
	}

	l.rpcServer = servicediscovery.NewServer(l.config.LeaderElection.RPC.Port, l.myIdentity, l.serviceDiscovery)
	l.rpcServer.Listen()

	if l.config.LeaderElection.Type == KubernetesLeaderElectionType {
		l.elector = kubernetes.NewLeaderElector(kubernetesClient, l.config, l.myIdentity, l, l.bus)
	}

	l.elector.Run(context.Background())
}

func (l *leaderElection) Stop() {
	l.elector.Close()
	l.rpcServer.Shutdown()
}

func NewLeaderElection(
	config *config.Dcp,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	bus EventBus.Bus,
) LeaderElection {
	return &leaderElection{
		config:           config,
		serviceDiscovery: serviceDiscovery,
		newLeaderLock:    &sync.Mutex{},
		bus:              bus,
	}
}
