package leaderelector

import "github.com/Trendyol/go-dcp-client/model"

type Handler interface {
	OnBecomeLeader()
	OnResignLeader()
	OnBecomeFollower(leaderIdentity *model.Identity)
}
