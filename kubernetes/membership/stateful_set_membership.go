package membership

import (
	"fmt"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type statefulSetMembership struct {
	info *info.Model
}

func (s *statefulSetMembership) GetInfo() *info.Model {
	return s.info
}

func NewStatefulSetMembership(config *helpers.Config) membership.Membership {
	statefulSetInfo, err := NewStatefulSetInfoFromHostname()
	if err != nil {
		logger.Panic(err, "error while creating statefulSet membership")
	}

	memberNumber := statefulSetInfo.PodOrdinal + 1

	if memberNumber > config.Dcp.Group.Membership.TotalMembers {
		logger.Panic(
			fmt.Errorf("memberNumber is greater than totalMembers"),
			"memberNumber: %v, totalMembers: %v", memberNumber, config.Dcp.Group.Membership.TotalMembers,
		)
	}

	return &statefulSetMembership{
		info: &info.Model{
			MemberNumber: memberNumber,
			TotalMembers: config.Dcp.Group.Membership.TotalMembers,
		},
	}
}
