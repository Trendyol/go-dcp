package leaderelector

import "context"

type LeaderElector interface {
	Run(ctx context.Context)
}
