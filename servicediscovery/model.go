package servicediscovery

import (
	"sort"

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
	Client          Client
	Name            string
	ClusterJoinTime int64
}

type ServiceBy func(s1, s2 *Service) bool

func (by ServiceBy) Sort(services []Service) {
	ps := &serviceSorter{
		services: services,
		by:       by,
	}
	sort.Sort(ps)
}

type serviceSorter struct {
	by       func(s1, s2 *Service) bool
	services []Service
}

func (s *serviceSorter) Len() int {
	return len(s.services)
}

func (s *serviceSorter) Swap(i, j int) {
	s.services[i], s.services[j] = s.services[j], s.services[i]
}

func (s *serviceSorter) Less(i, j int) bool {
	return s.by(&s.services[i], &s.services[j])
}

func NewService(client Client, name string, clusterJoinTime int64) *Service {
	return &Service{
		Client:          client,
		Name:            name,
		ClusterJoinTime: clusterJoinTime,
	}
}
