package servicediscovery

import (
	"github.com/Trendyol/go-dcp-client/identity"
)

type Register struct {
	From     identity.Identity
	Identity identity.Identity
}

type Ping struct {
	From identity.Identity
}

type Pong struct {
	From identity.Identity
}

type Rebalance struct {
	From         identity.Identity
	MemberNumber int
	TotalMembers int
}
