package membership

import (
	"github.com/Trendyol/go-dcp/config"
)

type staticMembership struct {
	info *Model
}

func (s *staticMembership) GetInfo() *Model {
	return s.info
}

func (s *staticMembership) Close() {
}

func (s *staticMembership) SetInfo(m Model) {}

func NewStaticMembership(config *config.Dcp) Membership {
	return &staticMembership{
		info: &Model{
			MemberNumber: config.Dcp.Group.Membership.MemberNumber,
			TotalMembers: config.Dcp.Group.Membership.TotalMembers,
		},
	}
}
