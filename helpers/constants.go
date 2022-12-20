package helpers

const Name = "cbgo"

const (
	Prefix                              = "_connector:" + Name + ":"
	StaticMembershipType                = "static"
	KubernetesStatefulSetMembershipType = "kubernetesStatefulSet"
	KubernetesHaMembershipType          = "kubernetesHa"
	KubernetesLeaderElectionType        = "kubernetes"
	DefaultScopeName                    = "_default"
	DefaultCollectionName               = "_default"
	CheckpointTypeAuto                  = "auto"
)
