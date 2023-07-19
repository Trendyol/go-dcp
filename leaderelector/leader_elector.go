package leaderelector

import (
	"context"

	"github.com/Trendyol/go-dcp/models"
)

type LeaderElector interface {
	Run(ctx context.Context)
}

type Handler interface {
	OnBecomeLeader()
	OnResignLeader()
	OnBecomeFollower(leaderIdentity *models.Identity)
}
