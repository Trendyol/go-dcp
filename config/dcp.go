package config

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/Trendyol/go-dcp/helpers"

	"github.com/Trendyol/go-dcp/logger"
)

const (
	DefaultScopeName                                = "_default"
	DefaultCollectionName                           = "_default"
	FileMetadataFileNameConfig                      = "fileName"
	MetadataTypeCouchbase                           = "couchbase"
	MetadataTypeFile                                = "file"
	MembershipTypeCouchbase                         = "couchbase"
	CouchbaseMetadataBucketConfig                   = "bucket"
	CouchbaseMetadataScopeConfig                    = "scope"
	CouchbaseMetadataCollectionConfig               = "collection"
	CouchbaseMetadataConnectionBufferSizeConfig     = "connectionBufferSize"
	CouchbaseMetadataConnectionTimeoutConfig        = "connectionTimeout"
	CheckpointTypeAuto                              = "auto"
	CouchbaseMembershipExpirySecondsConfig          = "expirySeconds"
	CouchbaseMembershipHeartbeatIntervalConfig      = "heartbeatInterval"
	CouchbaseMembershipHeartbeatToleranceConfig     = "heartbeatToleranceDuration"
	CouchbaseMembershipMonitorIntervalConfig        = "monitorInterval"
	CouchbaseMembershipTimeoutConfig                = "timeout"
	KubernetesLeaderElectorLeaseLockNameConfig      = "leaseLockName"
	KubernetesLeaderElectorLeaseLockNamespaceConfig = "leaseLockNamespace"
	KubernetesLeaderElectorLeaseDurationConfig      = "leaseDuration"
	KubernetesLeaderElectorRenewDeadlineConfig      = "renewDeadline"
	KubernetesLeaderElectorRetryPeriodConfig        = "retryPeriod"
)

type DCPGroupMembership struct {
	Config         map[string]string `yaml:"config"`
	Type           string            `yaml:"type"`
	MemberNumber   int               `yaml:"memberNumber"`
	TotalMembers   int               `yaml:"totalMembers"`
	RebalanceDelay time.Duration     `yaml:"rebalanceDelay"`
}

type DCPGroup struct {
	Name       string             `yaml:"name"`
	Membership DCPGroupMembership `yaml:"membership"`
}

type DCPListener struct {
	BufferSize uint `yaml:"bufferSize"`
}

type ExternalDcpConfig struct {
	DisableExpiryOpcode      bool `yaml:"disableExpiryOpcode"`
	DisableStreamEndByClient bool `yaml:"disableStreamEndByClient"`
}

type ExternalDcp struct {
	BufferSize           any               `yaml:"bufferSize"`
	ConnectionBufferSize any               `yaml:"connectionBufferSize"`
	Group                DCPGroup          `yaml:"group"`
	ConnectionTimeout    time.Duration     `yaml:"connectionTimeout"`
	Listener             DCPListener       `yaml:"listener"`
	Config               ExternalDcpConfig `yaml:"config"`
}

type API struct {
	Disabled bool `yaml:"disabled"`
	Port     int  `yaml:"port"`
}

type Metric struct {
	Path string `yaml:"path"`
}

type LeaderElection struct {
	Config  map[string]string `yaml:"config"`
	Type    string            `yaml:"type"`
	RPC     RPC               `yaml:"rpc"`
	Enabled bool              `yaml:"enabled"`
}

type RPC struct {
	Port int `yaml:"port"`
}

type Checkpoint struct {
	Type      string        `yaml:"type"`
	AutoReset string        `yaml:"autoReset"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

type HealthCheck struct {
	Disabled bool          `yaml:"disabled"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type RollbackMitigation struct {
	Disabled            bool          `yaml:"disabled"`
	Interval            time.Duration `yaml:"interval"`
	ConfigWatchInterval time.Duration `yaml:"configWatchInterval"`
}

type Metadata struct {
	Config   map[string]string `yaml:"config"`
	Type     string            `yaml:"type"`
	ReadOnly bool              `json:"readOnly"`
}

type Logging struct {
	Level string `yaml:"level"`
}

type Dcp struct {
	ConnectionBufferSize any                `yaml:"connectionBufferSize"`
	BucketName           string             `yaml:"bucketName"`
	ScopeName            string             `yaml:"scopeName"`
	Password             string             `yaml:"password"`
	RootCAPath           string             `yaml:"rootCAPath"`
	Username             string             `yaml:"username"`
	Logging              Logging            `yaml:"logging"`
	Metadata             Metadata           `yaml:"metadata"`
	CollectionNames      []string           `yaml:"collectionNames"`
	Hosts                []string           `yaml:"hosts"`
	Metric               Metric             `yaml:"metric"`
	Checkpoint           Checkpoint         `yaml:"checkpoint"`
	LeaderElection       LeaderElection     `yaml:"leaderElection"`
	Dcp                  ExternalDcp        `yaml:"dcp"`
	HealthCheck          HealthCheck        `yaml:"healthCheck"`
	RollbackMitigation   RollbackMitigation `yaml:"rollbackMitigation"`
	API                  API                `yaml:"api"`
	ConnectionTimeout    time.Duration      `yaml:"connectionTimeout"`
	SecureConnection     bool               `yaml:"secureConnection"`
	Debug                bool               `yaml:"debug"`
}

func (c *Dcp) IsCouchbaseMetadata() bool {
	return c.Metadata.Type == MetadataTypeCouchbase
}

func (c *Dcp) IsFileMetadata() bool {
	return c.Metadata.Type == MetadataTypeFile
}

func (c *Dcp) GetFileMetadata() string {
	var fileName string

	if _, ok := c.Metadata.Config[FileMetadataFileNameConfig]; ok {
		fileName = c.Metadata.Config[FileMetadataFileNameConfig]
	} else {
		err := errors.New("file metadata file name is not set")
		logger.Log.Error("failed to get metadata file name: %v", err)
		panic(err)
	}

	if fileName == "" {
		err := errors.New("file metadata file name is empty")
		logger.Log.Error("failed to get metadata file name: %v", err)
		panic(err)
	}

	return fileName
}

type CouchbaseMembership struct {
	ExpirySeconds              uint32        `yaml:"expirySeconds"`
	HeartbeatInterval          time.Duration `yaml:"heartbeatInterval"`
	HeartbeatToleranceDuration time.Duration `yaml:"heartbeatToleranceDuration"`
	MonitorInterval            time.Duration `yaml:"monitorInterval"`
	Timeout                    time.Duration `yaml:"timeout"`
}

func (c *Dcp) GetCouchbaseMembership() *CouchbaseMembership {
	couchbaseMembership := CouchbaseMembership{
		ExpirySeconds:              10,
		HeartbeatInterval:          5 * time.Second,
		HeartbeatToleranceDuration: 2 * time.Second,
		MonitorInterval:            500 * time.Millisecond,
		Timeout:                    10 * time.Second,
	}

	if expirySeconds, ok := c.Dcp.Group.Membership.Config[CouchbaseMembershipExpirySecondsConfig]; ok {
		parsedExpirySeconds, err := strconv.ParseUint(expirySeconds, 10, 32)
		if err != nil {
			logger.Log.Error("failed to parse membership expiry seconds: %v", err)
			panic(err)
		}

		couchbaseMembership.ExpirySeconds = uint32(parsedExpirySeconds)
	}

	if heartbeatInterval, ok := c.Dcp.Group.Membership.Config[CouchbaseMembershipHeartbeatIntervalConfig]; ok {
		parsedHeartbeatInterval, err := time.ParseDuration(heartbeatInterval)
		if err != nil {
			logger.Log.Error("failed to parse membership heartbeat interval: %v", err)
			panic(err)
		}

		couchbaseMembership.HeartbeatInterval = parsedHeartbeatInterval
	}

	if heartbeatToleranceDuration, ok := c.Dcp.Group.Membership.Config[CouchbaseMembershipHeartbeatToleranceConfig]; ok {
		parsedHeartbeatToleranceDuration, err := time.ParseDuration(heartbeatToleranceDuration)
		if err != nil {
			logger.Log.Error("failed to parse membership heartbeat tolerance duration: %v", err)
			panic(err)
		}

		couchbaseMembership.HeartbeatToleranceDuration = parsedHeartbeatToleranceDuration
	}

	if monitorInterval, ok := c.Dcp.Group.Membership.Config[CouchbaseMembershipMonitorIntervalConfig]; ok {
		parsedMonitorInterval, err := time.ParseDuration(monitorInterval)
		if err != nil {
			logger.Log.Error("failed to parse membership monitor interval: %v", err)
			panic(err)
		}

		couchbaseMembership.MonitorInterval = parsedMonitorInterval
	}

	if timeout, ok := c.Dcp.Group.Membership.Config[CouchbaseMembershipTimeoutConfig]; ok {
		parsedTimeout, err := time.ParseDuration(timeout)
		if err != nil {
			logger.Log.Error("failed to parse membership timeout: %v", err)
			panic(err)
		}

		couchbaseMembership.Timeout = parsedTimeout
	}

	return &couchbaseMembership
}

type KubernetesLeaderElector struct {
	LeaseLockName      string        `yaml:"leaseLockName"`
	LeaseLockNamespace string        `yaml:"leaseLockNamespace"`
	LeaseDuration      time.Duration `yaml:"leaseDuration"`
	RenewDeadline      time.Duration `yaml:"renewDeadline"`
	RetryPeriod        time.Duration `yaml:"retryPeriod"`
}

func (c *Dcp) GetKubernetesLeaderElector() *KubernetesLeaderElector {
	kubernetesLeaderElector := KubernetesLeaderElector{
		LeaseDuration: 8 * time.Second,
		RenewDeadline: 5 * time.Second,
		RetryPeriod:   1 * time.Second,
	}

	if leaseLockName, ok := c.LeaderElection.Config[KubernetesLeaderElectorLeaseLockNameConfig]; ok {
		kubernetesLeaderElector.LeaseLockName = leaseLockName
	} else {
		err := errors.New("leaseLockName is not defined")
		logger.Log.Error("error while creating leader elector: %v", err)
		panic(err)
	}

	if leaseLockNamespace, ok := c.LeaderElection.Config[KubernetesLeaderElectorLeaseLockNamespaceConfig]; ok {
		kubernetesLeaderElector.LeaseLockNamespace = leaseLockNamespace
	} else {
		err := errors.New("leaseLockNamespace is not defined")
		logger.Log.Error("error while creating leader elector: %v", err)
		panic(err)
	}

	if leaseDuration, ok := c.LeaderElection.Config[KubernetesLeaderElectorLeaseDurationConfig]; ok {
		parsedLeaseDuration, err := time.ParseDuration(leaseDuration)
		if err != nil {
			logger.Log.Error("failed to parse leader election lease duration: %v", err)
			panic(err)
		}

		kubernetesLeaderElector.LeaseDuration = parsedLeaseDuration
	}

	if renewDeadline, ok := c.LeaderElection.Config[KubernetesLeaderElectorRenewDeadlineConfig]; ok {
		parsedRenewDeadline, err := time.ParseDuration(renewDeadline)
		if err != nil {
			logger.Log.Error("failed to parse leader election renew deadline: %v", err)
			panic(err)
		}

		kubernetesLeaderElector.RenewDeadline = parsedRenewDeadline
	}

	if retryPeriod, ok := c.LeaderElection.Config[KubernetesLeaderElectorRetryPeriodConfig]; ok {
		parsedRetryPeriod, err := time.ParseDuration(retryPeriod)
		if err != nil {
			logger.Log.Error("failed to parse leader election retry period: %v", err)
			panic(err)
		}

		kubernetesLeaderElector.RetryPeriod = parsedRetryPeriod
	}

	return &kubernetesLeaderElector
}

type CouchbaseMetadata struct {
	Bucket               string        `yaml:"bucket"`
	Scope                string        `yaml:"scope"`
	Collection           string        `yaml:"collection"`
	ConnectionBufferSize uint          `yaml:"connectionBufferSize"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
}

func (c *Dcp) GetCouchbaseMetadata() *CouchbaseMetadata {
	couchbaseMetadata := CouchbaseMetadata{
		Bucket:               c.BucketName,
		Scope:                DefaultScopeName,
		Collection:           DefaultCollectionName,
		ConnectionBufferSize: 5242880, // 5 MB
		ConnectionTimeout:    5 * time.Second,
	}

	if bucket, ok := c.Metadata.Config[CouchbaseMetadataBucketConfig]; ok {
		couchbaseMetadata.Bucket = bucket
	}

	if scope, ok := c.Metadata.Config[CouchbaseMetadataScopeConfig]; ok {
		couchbaseMetadata.Scope = scope
	}

	if collection, ok := c.Metadata.Config[CouchbaseMetadataCollectionConfig]; ok {
		couchbaseMetadata.Collection = collection
	}

	if connectionBufferSize, ok := c.Metadata.Config[CouchbaseMetadataConnectionBufferSizeConfig]; ok {
		couchbaseMetadata.ConnectionBufferSize = uint(helpers.ResolveUnionIntOrStringValue(connectionBufferSize))
	}

	if connectionTimeout, ok := c.Metadata.Config[CouchbaseMetadataConnectionTimeoutConfig]; ok {
		parsedConnectionTimeout, err := time.ParseDuration(connectionTimeout)
		if err != nil {
			logger.Log.Error("failed to parse metadata connection timeout: %v", err)
			panic(err)
		}

		couchbaseMetadata.ConnectionTimeout = parsedConnectionTimeout
	}

	return &couchbaseMetadata
}

func (c *Dcp) ApplyDefaults() {
	c.applyDefaultRollbackMitigation()
	c.applyDefaultCheckpoint()
	c.applyDefaultHealthCheck()
	c.applyDefaultGroupMembership()
	c.applyDefaultConnectionTimeout()
	c.applyDefaultCollections()
	c.applyDefaultScopeName()
	c.applyDefaultConnectionBufferSize()
	c.applyDefaultMetrics()
	c.applyDefaultAPI()
	c.applyDefaultLeaderElection()
	c.applyDefaultDcp()
	c.applyDefaultMetadata()
	c.applyLogging()
}

func (c *Dcp) applyDefaultRollbackMitigation() {
	if c.RollbackMitigation.Interval == 0 {
		c.RollbackMitigation.Interval = 500 * time.Millisecond
	}

	if c.RollbackMitigation.ConfigWatchInterval == 0 {
		c.RollbackMitigation.ConfigWatchInterval = 2 * time.Second
	}
}

func (c *Dcp) applyDefaultCheckpoint() {
	if c.Checkpoint.Interval == 0 {
		c.Checkpoint.Interval = 20 * time.Second
	}

	if c.Checkpoint.Timeout == 0 {
		c.Checkpoint.Timeout = 60 * time.Second
	}

	if c.Checkpoint.Type == "" {
		c.Checkpoint.Type = "auto"
	}

	if c.Checkpoint.AutoReset == "" {
		c.Checkpoint.AutoReset = "earliest"
	}
}

func (c *Dcp) applyDefaultHealthCheck() {
	if c.HealthCheck.Interval == 0 {
		c.HealthCheck.Interval = 20 * time.Second
	}

	if c.HealthCheck.Timeout == 0 {
		c.HealthCheck.Timeout = 5 * time.Second
	}
}

func (c *Dcp) applyDefaultGroupMembership() {
	if c.Dcp.Group.Membership.RebalanceDelay == 0 {
		c.Dcp.Group.Membership.RebalanceDelay = 20 * time.Second
	}

	if c.Dcp.Group.Membership.TotalMembers == 0 {
		c.Dcp.Group.Membership.TotalMembers = 1
	}

	if c.Dcp.Group.Membership.MemberNumber == 0 {
		c.Dcp.Group.Membership.MemberNumber = 1
	}

	if c.Dcp.Group.Membership.Type == "" {
		c.Dcp.Group.Membership.Type = MembershipTypeCouchbase
	}

	if totalMembersFromEnvVariable := os.Getenv("GO_DCP__DCP_GROUP_MEMBERSHIP_TOTALMEMBERS"); totalMembersFromEnvVariable != "" {
		t, err := strconv.Atoi(totalMembersFromEnvVariable)
		if err != nil {
			panic("a non-integer environment variable was entered for 'totalMembers'")
		}
		c.Dcp.Group.Membership.TotalMembers = t
	}

	if memberNumberFromEnvVariable := os.Getenv("GO_DCP__DCP_GROUP_MEMBERSHIP_MEMBERNUMBER"); memberNumberFromEnvVariable != "" {
		t, err := strconv.Atoi(memberNumberFromEnvVariable)
		if err != nil {
			panic("a non-integer environment variable was entered for 'memberNumber'")
		}
		c.Dcp.Group.Membership.MemberNumber = t
	}
}

func (c *Dcp) applyDefaultConnectionTimeout() {
	if c.Dcp.ConnectionTimeout == 0 {
		c.Dcp.ConnectionTimeout = 5 * time.Second
	}

	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 5 * time.Second
	}
}

func (c *Dcp) applyDefaultCollections() {
	if c.CollectionNames == nil {
		c.CollectionNames = []string{DefaultCollectionName}
	}
}

func (c *Dcp) applyDefaultScopeName() {
	if c.ScopeName == "" {
		c.ScopeName = DefaultScopeName
	}
}

func (c *Dcp) applyDefaultConnectionBufferSize() {
	if c.ConnectionBufferSize == nil {
		c.ConnectionBufferSize = helpers.ResolveUnionIntOrStringValue("20mb")
	}
}

func (c *Dcp) applyDefaultMetrics() {
	if c.Metric.Path == "" {
		c.Metric.Path = "/metrics"
	}
}

func (c *Dcp) applyDefaultAPI() {
	if c.API.Port == 0 {
		c.API.Port = 8080
	}
}

func (c *Dcp) applyDefaultLeaderElection() {
	if c.LeaderElection.Type == "" {
		c.LeaderElection.Type = "kubernetes"
	}

	if c.LeaderElection.RPC.Port == 0 {
		c.LeaderElection.RPC.Port = 8081
	}
}

func (c *Dcp) applyDefaultDcp() {
	if c.Dcp.BufferSize == nil {
		c.Dcp.BufferSize = helpers.ResolveUnionIntOrStringValue("16mb")
	}

	if c.Dcp.ConnectionBufferSize == nil {
		c.Dcp.ConnectionBufferSize = helpers.ResolveUnionIntOrStringValue("20mb")
	}

	if c.Dcp.Listener.BufferSize == 0 {
		c.Dcp.Listener.BufferSize = 1000
	}
}

func (c *Dcp) applyDefaultMetadata() {
	if c.Metadata.Type == "" {
		c.Metadata.Type = MetadataTypeCouchbase
	}
}

func (c *Dcp) applyLogging() {
	if logger.Log != nil {
		return
	}

	loggingLevel := c.Logging.Level
	if loggingLevel == "" {
		c.Logging.Level = logger.INFO
	}

	logger.InitDefaultLogger(c.Logging.Level)
}
