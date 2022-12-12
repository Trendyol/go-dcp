package leaderElector

import "context"

type LeaderElector interface {
	Run(ctx context.Context)
}
