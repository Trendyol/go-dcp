package godcpclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/identity"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/kubernetes"
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
	initializedCh    chan bool
	stabilityCh      chan bool
	myIdentity       *identity.Identity
	newLeaderLock    sync.Mutex
	kubernetesClient kubernetes.Client
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
		l.elector = kle.NewLeaderElector(l.kubernetesClient, l.config, l.myIdentity, l)
	}

	l.elector.Run(context.Background())
	l.watchStability()

	timer := time.AfterFunc(30*time.Second, func() {
		l.initializedCh <- false
	})

	result := <-l.initializedCh

	if result {
		timer.Stop()
		logger.Debug("leader election is done, starting stream")
	} else {
		logger.Panic(fmt.Errorf("timeout"), "leader election failed")
	}
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
				l.initializedCh <- l.stable
				atomic.StoreUint32(&l.initialized, 1)
			}
		}
	}()
}

func NewLeaderElection(
	config helpers.ConfigLeaderElection,
	stream Stream,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	myIdentity *identity.Identity,
	kubernetesClient kubernetes.Client,
) LeaderElection {
	return &leaderElection{
		config:           config,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
		initializedCh:    make(chan bool, 1),
		stabilityCh:      make(chan bool),
		newLeaderLock:    sync.Mutex{},
		myIdentity:       myIdentity,
		kubernetesClient: kubernetesClient,
	}
}
