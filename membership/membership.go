package membership

import "github.com/Trendyol/go-dcp/logger"

type Membership interface {
	GetInfo() *Model
	Close()
}

const (
	StaticMembershipType                = "static"
	CouchbaseMembershipType             = "couchbase"
	KubernetesStatefulSetMembershipType = "kubernetesStatefulSet"
	KubernetesHaMembershipType          = "kubernetesHa"
	DynamicMembershipType               = "dynamic"
)

type Model struct {
	MemberNumber int
	TotalMembers int
}

func (s *Model) IsChanged(other *Model) bool {
	if other == nil {
		return true
	}

	res := s.MemberNumber != other.MemberNumber || s.TotalMembers != other.TotalMembers
	if !res {
		logger.Log.Info("membership info not changed")
	}

	return res
}
