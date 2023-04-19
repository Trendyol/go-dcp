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
	CouchbaseMetadataBucketConfig               = "bucket"
	CouchbaseMetadataScopeConfig                = "scope"
	CouchbaseMetadataCollectionConfig           = "collection"
	CouchbaseMetadataConnectionBufferSizeConfig = "connectionBufferSize"
	CouchbaseMetadataConnectionTimeoutConfig    = "connectionTimeout"
)

type ConfigDCPGroupMembership struct {
	Type           string        `yaml:"type"`
	MemberNumber   int           `yaml:"memberNumber"`
	TotalMembers   int           `yaml:"totalMembers"`
	RebalanceDelay time.Duration `yaml:"rebalanceDelay"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCPListener struct {
	BufferSize uint `yaml:"bufferSize"`
}

type ConfigDCP struct {
	Group                ConfigDCPGroup    `yaml:"group"`
	BufferSize           int               `yaml:"bufferSize"`
	ConnectionBufferSize uint              `yaml:"connectionBufferSize"`
	ConnectionTimeout    time.Duration     `yaml:"connectionTimeout"`
	Listener             ConfigDCPListener `yaml:"listener"`
}

type ConfigAPI struct {
	Port    int  `yaml:"port"`
	Enabled bool `yaml:"enabled"`
}

type ConfigMetric struct {
	Path             string  `yaml:"path"`
	AverageWindowSec float64 `yaml:"averageWindowSec"`
}

type ConfigLeaderElection struct {
	Config  map[string]string `yaml:"config"`
	Type    string            `yaml:"type"`
	RPC     ConfigRPC         `yaml:"rpc"`
	Enabled bool              `yaml:"enabled"`
}

type ConfigRPC struct {
	Port int `yaml:"port"`
}

type ConfigCheckpoint struct {
	Type      string        `yaml:"type"`
	AutoReset string        `yaml:"autoReset"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

type ConfigHealthCheck struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type ConfigRollbackMitigation struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
}

type ConfigMetadata struct {
	Config   map[string]string `yaml:"config"`
	Type     string            `yaml:"type"`
	ReadOnly bool              `json:"readOnly"`
}

type Dcp struct {
	Username             string                   `yaml:"username"`
	BucketName           string                   `yaml:"bucketName"`
	ScopeName            string                   `yaml:"scopeName"`
	Password             string                   `yaml:"password"`
	RootCAPath           string                   `yaml:"rootCAPath"`
	Metadata             ConfigMetadata           `yaml:"metadata"`
	Hosts                []string                 `yaml:"hosts"`
	CollectionNames      []string                 `yaml:"collectionNames"`
	Metric               ConfigMetric             `yaml:"metric"`
	Checkpoint           ConfigCheckpoint         `yaml:"checkpoint"`
	LeaderElection       ConfigLeaderElection     `yaml:"leaderElector"`
	Dcp                  ConfigDCP                `yaml:"dcp"`
	HealthCheck          ConfigHealthCheck        `yaml:"healthCheck"`
	API                  ConfigAPI                `yaml:"api"`
	RollbackMitigation   ConfigRollbackMitigation `yaml:"rollbackMitigation"`
	ConnectionTimeout    time.Duration            `yaml:"connectionTimeout"`
	ConnectionBufferSize uint                     `yaml:"connectionBufferSize"`
	SecureConnection     bool                     `yaml:"secureConnection"`
	Debug                bool                     `yaml:"debug"`
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
	var bucket, scope, collection string
	var connectionBufferSize uint
	var connectionTimeout time.Duration

	if _, ok := c.Metadata.Config[CouchbaseMetadataBucketConfig]; ok {
		bucket = c.Metadata.Config[CouchbaseMetadataBucketConfig]
	} else {
		bucket = c.BucketName
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataScopeConfig]; ok {
		scope = c.Metadata.Config[CouchbaseMetadataScopeConfig]
	} else {
		scope = DefaultScopeName
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataCollectionConfig]; ok {
		collection = c.Metadata.Config[CouchbaseMetadataCollectionConfig]
	} else {
		collection = DefaultCollectionName
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataConnectionBufferSizeConfig]; ok {
		parsedConnectionBufferSize, err := strconv.ParseUint(c.Metadata.Config[CouchbaseMetadataConnectionBufferSizeConfig], 10, 32)
		if err != nil {
			logger.ErrorLog.Printf("failed to parse metadata connection buffer size: %v", err)
			panic(err)
		}

		connectionBufferSize = uint(parsedConnectionBufferSize)
	} else {
		connectionBufferSize = 20971520
	}

	if _, ok := c.Metadata.Config[CouchbaseMetadataConnectionTimeoutConfig]; ok {
		parsedConnectionTimeout, err := time.ParseDuration(c.Metadata.Config[CouchbaseMetadataConnectionTimeoutConfig])
		if err != nil {
			logger.ErrorLog.Printf("failed to parse metadata connection timeout: %v", err)
			panic(err)
		}

		connectionTimeout = parsedConnectionTimeout
	} else {
		connectionTimeout = 5 * time.Second
	}

	return bucket, scope, collection, connectionBufferSize, connectionTimeout
}

//nolint:funlen
func (c *Dcp) ApplyDefaults() {
	if c.RollbackMitigation.Interval == 0 {
		c.RollbackMitigation.Interval = 200 * time.Millisecond
	}

	if c.Checkpoint.Interval == 0 {
		c.Checkpoint.Interval = 20 * time.Second
	}

	if c.Checkpoint.Timeout == 0 {
		c.Checkpoint.Timeout = 5 * time.Second
	}

	if c.HealthCheck.Interval == 0 {
		c.HealthCheck.Interval = 20 * time.Second
	}

	if c.HealthCheck.Timeout == 0 {
		c.HealthCheck.Timeout = 5 * time.Second
	}

	if c.Dcp.Group.Membership.RebalanceDelay == 0 {
		c.Dcp.Group.Membership.RebalanceDelay = 20 * time.Second
	}

	if c.Dcp.ConnectionTimeout == 0 {
		c.Dcp.ConnectionTimeout = 5 * time.Second
	}

	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 5 * time.Second
	}

	if c.CollectionNames == nil {
		c.CollectionNames = []string{DefaultCollectionName}
	}

	if c.ScopeName == "" {
		c.ScopeName = DefaultScopeName
	}

	if c.ConnectionBufferSize == 0 {
		c.ConnectionBufferSize = 20971520
	}

	if c.Metric.Path == "" {
		c.Metric.Path = "/metrics"
	}

	if c.Metric.AverageWindowSec == 0.0 {
		c.Metric.AverageWindowSec = 10.0
	}

	if !c.API.Enabled {
		c.API.Enabled = true
	}

	if c.API.Port == 0 {
		c.API.Port = 8080
	}

	if c.Checkpoint.Type == "" {
		c.Checkpoint.Type = "auto"
	}

	if c.Checkpoint.AutoReset == "" {
		c.Checkpoint.AutoReset = "earliest"
	}

	if !c.HealthCheck.Enabled {
		c.HealthCheck.Enabled = true
	}

	if c.LeaderElection.Type == "" {
		c.LeaderElection.Type = "kubernetes"
	}

	if c.LeaderElection.RPC.Port == 0 {
		c.LeaderElection.RPC.Port = 8081
	}

	if c.Dcp.BufferSize == 0 {
		c.Dcp.BufferSize = 16777216
	}

	if c.Dcp.ConnectionBufferSize == 0 {
		c.Dcp.ConnectionBufferSize = 20971520
	}

	if c.Dcp.Group.Membership.TotalMembers == 0 {
		c.Dcp.Group.Membership.TotalMembers = 1
	}

	if c.Dcp.Group.Membership.MemberNumber == 0 {
		c.Dcp.Group.Membership.MemberNumber = 1
	}

	if c.Dcp.Group.Membership.Type == "" {
		c.Dcp.Group.Membership.Type = "couchbase"
	}

	if c.Dcp.Listener.BufferSize == 0 {
		c.Dcp.Listener.BufferSize = 1
	}

	if c.Metadata.Type == "" {
		c.Metadata.Type = "couchbase"
	}

	if len(c.Metadata.Config) == 0 {
		c.Metadata.Config = map[string]string{
			"bucketName": c.BucketName,
		}
	}
}
