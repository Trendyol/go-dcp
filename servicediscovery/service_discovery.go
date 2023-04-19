package servicediscovery

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Trendyol/go-dcp-client/config"

	"github.com/Trendyol/go-dcp-client/membership"

	"github.com/Trendyol/go-dcp-client/helpers"

	"github.com/Trendyol/go-dcp-client/logger"
)

type ServiceDiscovery interface {
	Add(service *Service)
	Remove(name string)
	RemoveAll()
	AssignLeader(leaderService *Service)
	RemoveLeader()
	ReassignLeader() error
	StartHeartbeat()
	StopHeartbeat()
	StartMonitor()
	StopMonitor()
	GetAll() []string
	SetInfo(memberNumber int, totalMembers int)
	BeLeader()
	DontBeLeader()
}

type serviceDiscovery struct {
	bus             helpers.Bus
	leaderService   *Service
	services        map[string]*Service
	heartbeatTicker *time.Ticker
	monitorTicker   *time.Ticker
	info            *membership.Model
	servicesLock    *sync.RWMutex
	config          *config.Dcp
	amILeader       bool
}

func (s *serviceDiscovery) Add(service *Service) {
	s.services[service.Name] = service
}

func (s *serviceDiscovery) Remove(name string) {
	if _, ok := s.services[name]; ok {
		_ = s.services[name].Client.Close()

		delete(s.services, name)
	}
}

func (s *serviceDiscovery) RemoveAll() {
	for name := range s.services {
		s.Remove(name)
	}
}

func (s *serviceDiscovery) BeLeader() {
	s.amILeader = true
}

func (s *serviceDiscovery) DontBeLeader() {
	s.amILeader = false
}

func (s *serviceDiscovery) AssignLeader(leaderService *Service) {
	s.leaderService = leaderService
}

func (s *serviceDiscovery) RemoveLeader() {
	if s.leaderService == nil {
		return
	}

	_ = s.leaderService.Client.Close()

	s.leaderService = nil
}

func (s *serviceDiscovery) ReassignLeader() error {
	if s.leaderService == nil {
		return fmt.Errorf("leader is not assigned")
	}

	err := s.leaderService.Client.Reconnect()

	if err == nil {
		err = s.leaderService.Client.Register()
	}

	return err
}

func (s *serviceDiscovery) StartHeartbeat() {
	s.heartbeatTicker = time.NewTicker(5 * time.Second)

	go func() {
		for range s.heartbeatTicker.C {
			s.servicesLock.Lock()

			if s.leaderService != nil {
				err := s.leaderService.Client.Ping()
				if err != nil {
					logger.ErrorLog.Printf("leader is down, health check failed for leader")

					tempLeaderService := s.leaderService

					if err := s.ReassignLeader(); err != nil {
						if tempLeaderService != s.leaderService {
							_ = tempLeaderService.Client.Close()
						} else {
							s.RemoveLeader()
						}
					}
				}
			}

			for name, service := range s.services {
				err := service.Client.Ping()
				if err != nil {
					s.Remove(name)

					logger.Log.Printf("client %s disconnected", name)
				}
			}

			s.servicesLock.Unlock()
		}
	}()
}

func (s *serviceDiscovery) StopHeartbeat() {
	s.heartbeatTicker.Stop()
}

func (s *serviceDiscovery) StartMonitor() {
	s.monitorTicker = time.NewTicker(5 * time.Second)

	go func() {
		logger.Log.Printf("service discovery will start after %v", s.config.Dcp.Group.Membership.RebalanceDelay)
		time.Sleep(s.config.Dcp.Group.Membership.RebalanceDelay)

		for range s.monitorTicker.C {
			if !s.amILeader {
				continue
			}

			s.servicesLock.RLock()

			names := s.GetAll()
			totalMembers := len(names) + 1

			s.SetInfo(1, totalMembers)

			for index, name := range names {
				if service, ok := s.services[name]; ok {
					if err := service.Client.Rebalance(index+2, totalMembers); err != nil {
						logger.ErrorLog.Printf("rebalance failed for %s", name)
					}
				}
			}

			s.servicesLock.RUnlock()
		}
	}()
}

func (s *serviceDiscovery) StopMonitor() {
	s.monitorTicker.Stop()
}

func (s *serviceDiscovery) GetAll() []string {
	names := make([]string, 0, len(s.services))

	for name := range s.services {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func (s *serviceDiscovery) SetInfo(memberNumber int, totalMembers int) {
	newInfo := &membership.Model{
		MemberNumber: memberNumber,
		TotalMembers: totalMembers,
	}

	if newInfo.IsChanged(s.info) {
		s.info = newInfo

		logger.Log.Printf("new info arrived for member: %v/%v", memberNumber, totalMembers)

		s.bus.Emit(helpers.MembershipChangedBusEventName, newInfo)
	}
}

func NewServiceDiscovery(config *config.Dcp, bus helpers.Bus) ServiceDiscovery {
	return &serviceDiscovery{
		services:     make(map[string]*Service),
		bus:          bus,
		servicesLock: &sync.RWMutex{},
		config:       config,
	}
}
