package rpc

import (
	"github.com/Trendyol/go-dcp-client"
)

type Register struct {
	From     godcpclient.Identity
	Identity godcpclient.Identity
}

type Ping struct {
	From godcpclient.Identity
}

type Pong struct {
	From godcpclient.Identity
}

type Rebalance struct {
	From         godcpclient.Identity
	MemberNumber int
	TotalMembers int
}
