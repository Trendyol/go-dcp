package membership

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type staticMembership struct {
	info *info.Model
}

func (s *staticMembership) GetInfo() *info.Model {
	return s.info
}

func (s *staticMembership) Close() {
}

func NewStaticMembership(config *helpers.Config) Membership {
	return &staticMembership{
		info: &info.Model{
			MemberNumber: config.Dcp.Group.Membership.MemberNumber,
			TotalMembers: config.Dcp.Group.Membership.TotalMembers,
		},
	}
}
