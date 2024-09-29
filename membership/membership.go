package membership

type Membership interface {
	GetInfo() *Model
	Close()
	SetInfo(model Model)
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

	return s.MemberNumber != other.MemberNumber || s.TotalMembers != other.TotalMembers
}
