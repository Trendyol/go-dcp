package leaderelector

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/kubernetes"
	godcpclient "github.com/Trendyol/go-dcp-client/leaderelector"
	dcpModel "github.com/Trendyol/go-dcp-client/model"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type leaderElector struct {
	client             kubernetes.Client
	myIdentity         *dcpModel.Identity
	handler            godcpclient.Handler
	leaseLockName      string
	leaseLockNamespace string
}

func (le *leaderElector) Run(ctx context.Context) {
	callback := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(c context.Context) {
			log.Println("granted to leader")

			le.client.AddLabel("role", "leader")

			le.handler.OnBecomeLeader()
		},
		OnStoppedLeading: func() {
			log.Println("revoked from leader")

			le.client.RemoveLabel("role")

			le.handler.OnResignLeader()
		},
		OnNewLeader: func(leaderIdentityStr string) {
			leaderIdentity := dcpModel.NewIdentityFromStr(leaderIdentityStr)

			if le.myIdentity.Equal(leaderIdentity) {
				return
			}

			log.Println("granted to follower for leader: " + leaderIdentity.Name)

			le.client.AddLabel("role", "follower")

			le.handler.OnBecomeFollower(leaderIdentity)
		},
	}

	go func() {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta: metaV1.ObjectMeta{
					Name:      le.leaseLockName,
					Namespace: le.leaseLockNamespace,
				},
				Client: le.client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: le.myIdentity.String(),
				},
			},
			ReleaseOnCancel: true,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks:       callback,
		})
	}()
}

func NewLeaderElector(
	client kubernetes.Client,
	config helpers.ConfigLeaderElection,
	myIdentity *dcpModel.Identity,
	handler godcpclient.Handler,
) godcpclient.LeaderElector {
	var leaseLockName string
	var leaseLockNamespace string

	if val, ok := config.Config["leaseLockName"]; ok {
		leaseLockName = val
	} else {
		panic(fmt.Errorf("leaseLockName is not defined"))
	}

	if val, ok := config.Config["leaseLockNamespace"]; ok {
		leaseLockNamespace = val
	} else {
		panic(fmt.Errorf("leaseLockNamespace is not defined"))
	}

	return &leaderElector{
		client:             client,
		myIdentity:         myIdentity,
		handler:            handler,
		leaseLockName:      leaseLockName,
		leaseLockNamespace: leaseLockNamespace,
	}
}
