package membership

import (
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/kubernetes/membership/model"
	"github.com/Trendyol/go-dcp-client/membership"
	"github.com/Trendyol/go-dcp-client/membership/info"
)

type statefulSetMembership struct {
	info *info.Model
}

func (s *statefulSetMembership) GetInfo() *info.Model {
	return s.info
}

func NewStatefulSetMembership(config helpers.ConfigDCPGroupMembership) membership.Membership {
	statefulSetInfo, err := model.NewStatefulSetInfoFromHostname()

	if err != nil {
		panic(err)
	}

	memberNumber := statefulSetInfo.PodOrdinal + 1

	if memberNumber > config.TotalMembers {
		panic("memberNumber is greater than totalMembers")
	}

	return &statefulSetMembership{
		info: &info.Model{
			MemberNumber: memberNumber,
			TotalMembers: config.TotalMembers,
		},
	}
}
