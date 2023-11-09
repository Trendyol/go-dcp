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

type DCPGroupMembership struct {
	Type           string        `yaml:"type"`
	MemberNumber   int           `yaml:"memberNumber"`
	TotalMembers   int           `yaml:"totalMembers"`
	RebalanceDelay time.Duration `yaml:"rebalanceDelay"`
}

type DCPGroup struct {
	Name       string             `yaml:"name"`
	Membership DCPGroupMembership `yaml:"membership"`
}

type DCPListener struct {
	BufferSize uint `yaml:"bufferSize"`
}

type ExternalDcp struct {
	BufferSize           any           `yaml:"bufferSize"`
	ConnectionBufferSize any           `yaml:"connectionBufferSize"`
	Group                DCPGroup      `yaml:"group"`
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
	Config   map[string]any `yaml:"config"`
	Type     string         `yaml:"type"`
	ReadOnly bool           `json:"readOnly"`
}

type Logging struct {
	Level string `yaml:"level"`
}

type Dcp struct {
	BucketName         string             `yaml:"bucketName"`
	ScopeName          string             `yaml:"scopeName"`
	Password           string             `yaml:"password"`
	RootCAPath         string             `yaml:"rootCAPath"`
	Username           string             `yaml:"username"`
	Logging            Logging            `yaml:"logging"`
	Metadata           Metadata           `yaml:"metadata"`
	Hosts              []string           `yaml:"hosts"`
	CollectionNames    []string           `yaml:"collectionNames"`
	Metric             Metric             `yaml:"metric"`
	Checkpoint         Checkpoint         `yaml:"checkpoint"`
	LeaderElection     LeaderElection     `yaml:"leaderElector"`
	Dcp                ExternalDcp        `yaml:"dcp"`
	HealthCheck        HealthCheck        `yaml:"healthCheck"`
	RollbackMitigation RollbackMitigation `yaml:"rollbackMitigation"`
	API                API                `yaml:"api"`
	ConnectionTimeout  time.Duration      `yaml:"connectionTimeout"`
	SecureConnection   bool               `yaml:"secureConnection"`
	Debug              bool               `yaml:"debug"`
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
		fileName = c.Metadata.Config[FileMetadataFileNameConfig].(string)
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

func (c *Dcp) GetCouchbaseMetadata() (string, string, string, uint, time.Duration) {
	return c.getMetadataBucket(),
		c.getMetadataScope(),
		c.getMetadataCollection(),
		c.getMetadataConnectionBufferSize(),
		c.getMetadataConnectionTimeout()
}

func (c *Dcp) getMetadataBucket() string {
	if bucket, ok := c.Metadata.Config[CouchbaseMetadataBucketConfig].(string); ok {
		return bucket
	}

	return c.BucketName
}

func (c *Dcp) getMetadataScope() string {
	if scope, ok := c.Metadata.Config[CouchbaseMetadataScopeConfig].(string); ok {
		return scope
	}

	return DefaultScopeName
}

func (c *Dcp) getMetadataCollection() string {
	if collection, ok := c.Metadata.Config[CouchbaseMetadataCollectionConfig].(string); ok {
		return collection
	}

	return DefaultCollectionName
}

func (c *Dcp) getMetadataConnectionBufferSize() uint {
	if connectionBufferSize, ok := c.Metadata.Config[CouchbaseMetadataConnectionBufferSizeConfig]; ok {
		return uint(helpers.ResolveUnionIntOrStringValue(connectionBufferSize))
	}

	result, _ := helpers.ConvertSizeUnitToByte("5MB")
	return uint(result)
}

func (c *Dcp) getMetadataConnectionTimeout() time.Duration {
	if connectionTimeout, ok := c.Metadata.Config[CouchbaseMetadataConnectionTimeoutConfig].(string); ok {
		parsedConnectionTimeout, err := time.ParseDuration(connectionTimeout)
		if err != nil {
			logger.Log.Error("failed to parse metadata connection timeout: %v", err)
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
	if c.Dcp.ConnectionBufferSize == nil {
		defaultValue, _ := helpers.ConvertSizeUnitToByte("20mb")
		c.Dcp.ConnectionBufferSize = defaultValue
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
	if c.Dcp.BufferSize == nil {
		c.Dcp.BufferSize = 16777216
	}

	c.applyDefaultConnectionBufferSize()

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
