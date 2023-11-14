package kubernetes

import (
	"context"
	"fmt"
	"time"

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
	client             Client
	myIdentity         *models.Identity
	handler            leaderelector.Handler
	leaseLockName      string
	leaseLockNamespace string
}

func (le *leaderElector) Run(ctx context.Context) {
	callback := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(c context.Context) {
			logger.Log.Info("granted to leader")

			le.client.AddLabel(le.leaseLockNamespace, "role", "leader")

			le.handler.OnBecomeLeader()
		},
		OnStoppedLeading: func() {
			logger.Log.Info("revoked from leader")

			le.client.RemoveLabel(le.leaseLockNamespace, "role")

			le.handler.OnResignLeader()
		},
		OnNewLeader: func(leaderIdentityStr string) {
			leaderIdentity := models.NewIdentityFromStr(leaderIdentityStr)

			if le.myIdentity.Equal(leaderIdentity) {
				return
			}

			logger.Log.Info("granted to follower for leader: %s", leaderIdentity.Name)

			le.client.AddLabel(le.leaseLockNamespace, "role", "follower")

			le.handler.OnBecomeFollower(leaderIdentity)
		},
	}

	go func() {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta: v1.ObjectMeta{
					Name:      le.leaseLockName,
					Namespace: le.leaseLockNamespace,
				},
				Client: le.client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: le.myIdentity.String(),
				},
			},
			ReleaseOnCancel: true,
			LeaseDuration:   8 * time.Second,
			RenewDeadline:   5 * time.Second,
			RetryPeriod:     1 * time.Second,
			Callbacks:       callback,
		})
	}()
}

func (le *leaderElector) membershipChangedListener(model *membership.Model) {
	le.client.AddLabel(
		le.leaseLockNamespace,
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
	var leaseLockName string
	var leaseLockNamespace string

	if val, ok := config.LeaderElection.Config["leaseLockName"]; ok {
		leaseLockName = val
	} else {
		err := fmt.Errorf("leaseLockName is not defined")
		logger.Log.Error("error while creating leader elector: %v", err)
		panic(err)
	}

	if val, ok := config.LeaderElection.Config["leaseLockNamespace"]; ok {
		leaseLockNamespace = val
	} else {
		err := fmt.Errorf("leaseLockNamespace is not defined")
		logger.Log.Error("error while creating leader elector: %v", err)
		panic(err)
	}

	le := &leaderElector{
		client:             client,
		myIdentity:         myIdentity,
		handler:            handler,
		leaseLockName:      leaseLockName,
		leaseLockNamespace: leaseLockNamespace,
	}

	err := bus.SubscribeAsync(helpers.MembershipChangedBusEventName, le.membershipChangedListener, true)
	if err != nil {
		logger.Log.Error("cannot subscribe to membership changed event: %v", err)
		panic(err)
	}

	return le
}
