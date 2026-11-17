package kubernetes

import (
	"context"
	"fmt"

	"github.com/asaskevich/EventBus"

	"github.com/Trendyol/go-dcp/config"

	"github.com/Trendyol/go-dcp/membership"
	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/leaderelector"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/helpers"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type leaderElector struct {
	client              Client
	handler             leaderelector.Handler
	bus                 EventBus.Bus
	myIdentity          *models.Identity
	leaderElectorConfig *config.KubernetesLeaderElector
}

func (le *leaderElector) Run(ctx context.Context) {
	callback := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(c context.Context) {
			logger.Log.Debug("granted to leader")

			le.client.AddLabel("role", "leader")
			le.handler.OnBecomeLeader()
		},
		OnStoppedLeading: func() {
			logger.Log.Debug("revoked from leader")

			le.client.RemoveLabel("role")
			le.handler.OnResignLeader()
		},
		OnNewLeader: func(leaderIdentityStr string) {
			leaderIdentity := models.NewIdentityFromStr(leaderIdentityStr)
			if le.myIdentity.Equal(leaderIdentity) {
				return
			}

			logger.Log.Debug("granted to follower for leader: %s", leaderIdentity.Name)

			le.client.AddLabel("role", "follower")
			le.handler.OnBecomeFollower(leaderIdentity)
		},
	}

	go func() {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta: v1.ObjectMeta{
					Name:      le.leaderElectorConfig.LeaseLockName,
					Namespace: le.leaderElectorConfig.LeaseLockNamespace,
				},
				Client: le.client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: le.myIdentity.String(),
				},
			},
			ReleaseOnCancel: true,
			LeaseDuration:   le.leaderElectorConfig.LeaseDuration,
			RenewDeadline:   le.leaderElectorConfig.RenewDeadline,
			RetryPeriod:     le.leaderElectorConfig.RetryPeriod,
			Callbacks:       callback,
		})
	}()
}

func (le *leaderElector) Close() {
	err := le.bus.Unsubscribe(helpers.MembershipChangedBusEventName, le.membershipChangedListener)
	if err != nil {
		logger.Log.Error("error while unsubscribe: %v", err)
	}
}

func (le *leaderElector) membershipChangedListener(model *membership.Model) {
	le.client.AddLabel(
		"member",
		fmt.Sprintf("%v_%v", model.MemberNumber, model.TotalMembers),
	)
}

func NewLeaderElector(
	client Client,
	config *config.Dcp,
	myIdentity *models.Identity,
	handler leaderelector.Handler,
	bus EventBus.Bus,
) leaderelector.LeaderElector {
	le := &leaderElector{
		client:              client,
		myIdentity:          myIdentity,
		handler:             handler,
		leaderElectorConfig: config.GetKubernetesLeaderElector(),
		bus:                 bus,
	}

	err := bus.SubscribeAsync(helpers.MembershipChangedBusEventName, le.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("error while subscribe to membership changed event, err: %v", err)
		panic(err)
	}

	return le
}
