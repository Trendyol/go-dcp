package helpers

const Name = "cbgo"

const (
	Prefix                                      = "_connector:" + Name + ":"
	StaticMembershipType                        = "static"
	CouchbaseMembershipType                     = "couchbase"
	KubernetesStatefulSetMembershipType         = "kubernetesStatefulSet"
	KubernetesHaMembershipType                  = "kubernetesHa"
	KubernetesLeaderElectionType                = "kubernetes"
	DefaultScopeName                            = "_default"
	DefaultCollectionName                       = "_default"
	CheckpointTypeAuto                          = "auto"
	CheckpointAutoResetTypeLatest               = "latest"
	MetadataTypeCouchbase                       = "couchbase"
	MetadataTypeFile                            = "file"
	CouchbaseMetadataBucketConfig               = "bucket"
	CouchbaseMetadataScopeConfig                = "scope"
	CouchbaseMetadataCollectionConfig           = "collection"
	CouchbaseMetadataConnectionBufferSizeConfig = "connectionBufferSize"
	FileMetadataFileNameConfig                  = "fileName"
)
