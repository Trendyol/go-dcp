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

func NewStaticMembership(config helpers.ConfigDCPGroupMembership) Membership {
	return &staticMembership{
		info: &info.Model{
			MemberNumber: config.MemberNumber,
			TotalMembers: config.TotalMembers,
		},
	}
}
