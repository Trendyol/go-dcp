package servicediscovery

import (
	"github.com/Trendyol/go-dcp/models"
)

type Register struct {
	From     *models.Identity
	Identity *models.Identity
}

type Ping struct {
	From *models.Identity
}

type Pong struct {
	From *models.Identity
}

type Rebalance struct {
	From         *models.Identity
	MemberNumber int
	TotalMembers int
}

type Service struct {
	Client Client
	Name   string
}

func NewService(client Client, name string) *Service {
	return &Service{
		Client: client,
		Name:   name,
	}
}
