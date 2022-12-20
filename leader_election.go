package godcpclient

import (
	"context"
	"sync"
	"sync/atomic"

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
	config           helpers.ConfigLeaderElection
	elector          kle.LeaderElector
	rpcServer        servicediscovery.Server
	stream           Stream
	serviceDiscovery servicediscovery.ServiceDiscovery
	stable           bool
	initialized      uint32
	stabilityCh      chan bool
	myIdentity       *identity.Identity
	newLeaderLock    sync.Mutex
	infoHandler      info.Handler
}

func (l *leaderElection) OnBecomeLeader() {
	if atomic.LoadUint32(&l.initialized) == 1 {
		l.stabilityCh <- false
	}

	l.serviceDiscovery.BeLeader()
	l.serviceDiscovery.RemoveLeader()

	l.stabilityCh <- true
}

func (l *leaderElection) OnResignLeader() {
	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
}

func (l *leaderElection) OnBecomeFollower(leaderIdentity *identity.Identity) {
	l.newLeaderLock.Lock()
	defer l.newLeaderLock.Unlock()

	if atomic.LoadUint32(&l.initialized) == 1 {
		l.stabilityCh <- false
	}

	l.serviceDiscovery.DontBeLeader()
	l.serviceDiscovery.RemoveAll()
	l.serviceDiscovery.RemoveLeader()

	leaderClient, err := servicediscovery.NewClient(l.config.RPC.Port, l.myIdentity, leaderIdentity)
	if err != nil {
		return
	}

	leaderService := servicediscovery.NewService(leaderClient, true, leaderIdentity.Name)

	l.serviceDiscovery.AssignLeader(leaderService)

	err = leaderClient.Register()

	if err != nil {
		logger.Panic(err, "error while registering leader client")
	}

	l.stabilityCh <- true
}

func (l *leaderElection) Start() {
	l.rpcServer = servicediscovery.NewServer(l.config.RPC.Port, l.myIdentity, l.serviceDiscovery)
	l.rpcServer.Listen()

	if l.config.Type == helpers.KubernetesLeaderElectionType {
		kubernetesClient := kubernetes.NewClient(l.myIdentity)
		l.elector = kle.NewLeaderElector(kubernetesClient, l.config, l.myIdentity, l, l.infoHandler)
	}

	l.elector.Run(context.Background())
	l.watchStability()
}

func (l *leaderElection) Stop() {
	l.rpcServer.Shutdown()
}

func (l *leaderElection) watchStability() {
	go func() {
		for result := range l.stabilityCh {
			if l.stable != result {
				l.stable = result
				logger.Debug("stability changed: %v", l.stable)
			}

			if atomic.LoadUint32(&l.initialized) != 1 {
				atomic.StoreUint32(&l.initialized, 1)
			}
		}
	}()
}

func NewLeaderElection(
	config helpers.ConfigLeaderElection,
	stream Stream,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	infoHandler info.Handler,
) LeaderElection {
	return &leaderElection{
		config:           config,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
		stabilityCh:      make(chan bool),
		newLeaderLock:    sync.Mutex{},
		myIdentity:       identity.NewIdentityFromEnv(),
		infoHandler:      infoHandler,
	}
}
