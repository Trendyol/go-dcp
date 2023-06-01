package config

import (
	"errors"
	"strconv"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
)

const (
	DefaultScopeName                            = "_default"
	DefaultCollectionName                       = "_default"
	FileMetadataFileNameConfig                  = "fileName"
	MetadataTypeCouchbase                       = "couchbase"
	MetadataTypeFile                            = "file"
	MembershipTypeCouchbase                     = "couchbase"
	CouchbaseMetadataBucketConfig               = "bucket"
	CouchbaseMetadataScopeConfig                = "scope"
	CouchbaseMetadataCollectionConfig           = "collection"
	CouchbaseMetadataConnectionBufferSizeConfig = "connectionBufferSize"
	CouchbaseMetadataConnectionTimeoutConfig    = "connectionTimeout"
	CheckpointTypeAuto                          = "auto"
)

type CouchbaseMembership struct {
	ExpiryDuration             time.Duration `yaml:"expiryDuration"`
	HeartbeatInterval          time.Duration `yaml:"heartbeatInterval"`
	HeartbeatToleranceDuration time.Duration `yaml:"heartbeatToleranceDuration"`
	MonitorInterval            time.Duration `yaml:"monitorInterval"`
	Timeout                    time.Duration `yaml:"timeout"`
}

type DCPGroupMembership struct {
	CoucbaseMembership *CouchbaseMembership `yaml:"coucbaseMembershipConfig"`
	Type               string               `yaml:"type"`
	MemberNumber       int                  `yaml:"memberNumber"`
	TotalMembers       int                  `yaml:"totalMembers"`
	RebalanceDelay     time.Duration        `yaml:"rebalanceDelay"`
}

type DCPGroup struct {
	Name       string             `yaml:"name"`
	Membership DCPGroupMembership `yaml:"membership"`
}

type DCPListener struct {
	BufferSize uint `yaml:"bufferSize"`
}

type ExternalDcp struct {
	Group                DCPGroup      `yaml:"group"`
	BufferSize           int           `yaml:"bufferSize"`
	ConnectionBufferSize uint          `yaml:"connectionBufferSize"`
	ConnectionTimeout    time.Duration `yaml:"connectionTimeout"`
	Listener             DCPListener   `yaml:"listener"`
}

type API struct {
	Disabled bool `yaml:"disabled"`
	Port     int  `yaml:"port"`
}

type Metric struct {
	Path             string  `yaml:"path"`
	AverageWindowSec float64 `yaml:"averageWindowSec"`
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

type Dcp struct {
	Username             string             `yaml:"username"`
	BucketName           string             `yaml:"bucketName"`
	ScopeName            string             `yaml:"scopeName"`
	Password             string             `yaml:"password"`
	RootCAPath           string             `yaml:"rootCAPath"`
	Metadata             Metadata           `yaml:"metadata"`
	Hosts                []string           `yaml:"hosts"`
	CollectionNames      []string           `yaml:"collectionNames"`
	Metric               Metric             `yaml:"metric"`
	Checkpoint           Checkpoint         `yaml:"checkpoint"`
	LeaderElection       LeaderElection     `yaml:"leaderElector"`
	Dcp                  ExternalDcp        `yaml:"dcp"`
	HealthCheck          HealthCheck        `yaml:"healthCheck"`
	API                  API                `yaml:"api"`
	RollbackMitigation   RollbackMitigation `yaml:"rollbackMitigation"`
	ConnectionTimeout    time.Duration      `yaml:"connectionTimeout"`
	ConnectionBufferSize uint               `yaml:"connectionBufferSize"`
	SecureConnection     bool               `yaml:"secureConnection"`
	Debug                bool               `yaml:"debug"`
}

func (c *Dcp) IsCollectionModeEnabled() bool {
	return !(c.ScopeName == DefaultScopeName && len(c.CollectionNames) == 1 && c.CollectionNames[0] == DefaultCollectionName)
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
		logger.ErrorLog.Printf("failed to get metadata file name: %v", err)
		panic(err)
	}

	if fileName == "" {
		err := errors.New("file metadata file name is empty")
		logger.ErrorLog.Printf("failed to get metadata file name: %v", err)
		panic(err)
	}

	return fileName
}

func (c *Dcp) GetCouchbaseMetadata() (string, string, string, uint, time.Duration) {
	return c.getMetadataBucket(),
		c.getMetadataScope(),
		c.getMetadataCollection(),
		c.getMetadataConnectionBufferSize(),
		c.getMetadataConnectionTimeout()
}

func (c *Dcp) getMetadataBucket() string {
	if bucket, ok := c.Metadata.Config[CouchbaseMetadataBucketConfig]; ok {
		return bucket
	}

	return c.BucketName
}

func (c *Dcp) getMetadataScope() string {
	if scope, ok := c.Metadata.Config[CouchbaseMetadataScopeConfig]; ok {
		return scope
	}

	return DefaultScopeName
}

func (c *Dcp) getMetadataCollection() string {
	if collection, ok := c.Metadata.Config[CouchbaseMetadataCollectionConfig]; ok {
		return collection
	}

	return DefaultCollectionName
}

func (c *Dcp) getMetadataConnectionBufferSize() uint {
	if connectionBufferSize, ok := c.Metadata.Config[CouchbaseMetadataConnectionBufferSizeConfig]; ok {
		parsedConnectionBufferSize, err := strconv.ParseUint(connectionBufferSize, 10, 32)
		if err != nil {
			logger.ErrorLog.Printf("failed to parse metadata connection buffer size: %v", err)
			panic(err)
		}

		return uint(parsedConnectionBufferSize)
	}

	return 5242880 // 5 MB
}

func (c *Dcp) getMetadataConnectionTimeout() time.Duration {
	if connectionTimeout, ok := c.Metadata.Config[CouchbaseMetadataConnectionTimeoutConfig]; ok {
		parsedConnectionTimeout, err := time.ParseDuration(connectionTimeout)
		if err != nil {
			logger.ErrorLog.Printf("failed to parse metadata connection timeout: %v", err)
			panic(err)
		}

		return parsedConnectionTimeout
	}

	return 5 * time.Second
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
		c.Checkpoint.Timeout = 5 * time.Second
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

	if c.Dcp.Group.Membership.Type == MembershipTypeCouchbase && c.Dcp.Group.Membership.CoucbaseMembership == nil {
		c.Dcp.Group.Membership.CoucbaseMembership = &CouchbaseMembership{
			ExpiryDuration:             2 * time.Second,
			HeartbeatInterval:          1 * time.Second,
			HeartbeatToleranceDuration: 2 * time.Second,
			MonitorInterval:            500 * time.Millisecond,
			Timeout:                    10 * time.Second,
		}
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
	if c.ConnectionBufferSize == 0 {
		c.ConnectionBufferSize = 20971520
	}
}

func (c *Dcp) applyDefaultMetrics() {
	if c.Metric.Path == "" {
		c.Metric.Path = "/metrics"
	}

	if c.Metric.AverageWindowSec == 0.0 {
		c.Metric.AverageWindowSec = 10.0
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
	if c.Dcp.BufferSize == 0 {
		c.Dcp.BufferSize = 16777216
	}

	if c.Dcp.ConnectionBufferSize == 0 {
		c.Dcp.ConnectionBufferSize = 20971520
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
