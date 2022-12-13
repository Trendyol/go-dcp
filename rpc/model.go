package rpc

import (
	"github.com/Trendyol/go-dcp-client/model"
)

type Register struct {
	From     model.Identity
	Identity model.Identity
}

type Ping struct {
	From model.Identity
}

type Pong struct {
	From model.Identity
}

type Rebalance struct {
	From         model.Identity
	MemberNumber int
	TotalMembers int
}
