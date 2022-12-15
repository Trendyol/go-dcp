package leaderelector

import (
	"github.com/Trendyol/go-dcp-client"
)

type Handler interface {
	OnBecomeLeader()
	OnResignLeader()
	OnBecomeFollower(leaderIdentity *godcpclient.Identity)
}
