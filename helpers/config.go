package helpers

import (
	"errors"
	"strconv"
	"time"

	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yamlv3"
)

type ConfigDCPGroupMembership struct {
	Type           string        `yaml:"type" default:"couchbase"`
	MemberNumber   int           `yaml:"memberNumber" default:"1"`
	TotalMembers   int           `yaml:"totalMembers" default:"1"`
	RebalanceDelay time.Duration `yaml:"rebalanceDelay"`
}

type ConfigDCPGroup struct {
	Name       string                   `yaml:"name"`
	Membership ConfigDCPGroupMembership `yaml:"membership"`
}

type ConfigDCPListener struct {
	BufferSize uint `yaml:"bufferSize" default:"1"`
}

type ConfigDCP struct {
	Group                ConfigDCPGroup    `yaml:"group"`
	BufferSize           int               `yaml:"bufferSize" default:"16777216"`
	ConnectionBufferSize uint              `yaml:"connectionBufferSize" default:"20971520"`
	ConnectionTimeout    time.Duration     `yaml:"connectionTimeout"`
	Listener             ConfigDCPListener `yaml:"listener"`
}

type ConfigAPI struct {
	Port    int  `yaml:"port" default:"8080"`
	Enabled bool `yaml:"enabled" default:"true"`
}

type ConfigMetric struct {
	Path             string  `yaml:"path" default:"/metrics"`
	AverageWindowSec float64 `yaml:"averageWindowSec" default:"10.0"`
}

type ConfigLeaderElection struct {
	Config  map[string]string `yaml:"config"`
	Type    string            `yaml:"type" default:"kubernetes"`
	RPC     ConfigRPC         `yaml:"rpc"`
	Enabled bool              `yaml:"enabled" default:"false"`
}

type ConfigRPC struct {
	Port int `yaml:"port" default:"8081"`
}

type ConfigCheckpoint struct {
	Type      string        `yaml:"type" default:"auto"`
	AutoReset string        `yaml:"autoReset" default:"earliest"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

type ConfigHealthCheck struct {
	Enabled  bool          `yaml:"enabled" default:"true"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type ConfigRollbackMitigation struct {
	Enabled  bool          `yaml:"enabled" default:"false"`
	Interval time.Duration `yaml:"interval"`
}

type ConfigMetadata struct {
	Config   map[string]string `yaml:"config"`
	Type     string            `yaml:"type" default:"couchbase"`
	ReadOnly bool              `json:"readOnly" default:"false"`
}

type Config struct {
	Metadata           ConfigMetadata           `yaml:"metadata"`
	Username           string                   `yaml:"username"`
	BucketName         string                   `yaml:"bucketName"`
	ScopeName          string                   `yaml:"scopeName" default:"_default"`
	Password           string                   `yaml:"password"`
	RootCAPath         string                   `yaml:"rootCAPath"`
	CollectionNames    []string                 `yaml:"collectionNames"`
	Metric             ConfigMetric             `yaml:"metric"`
	Hosts              []string                 `yaml:"hosts"`
	Checkpoint         ConfigCheckpoint         `yaml:"checkpoint"`
	LeaderElection     ConfigLeaderElection     `yaml:"leaderElector"`
	Dcp                ConfigDCP                `yaml:"dcp"`
	HealthCheck        ConfigHealthCheck        `yaml:"healthCheck"`
	API                ConfigAPI                `yaml:"api"`
	RollbackMitigation ConfigRollbackMitigation `yaml:"rollbackMitigation"`
	SecureConnection   bool                     `yaml:"secureConnection"`
	Debug              bool                     `yaml:"debug"`
}

func (c *Config) IsCollectionModeEnabled() bool {
	return !(c.ScopeName == DefaultScopeName && len(c.CollectionNames) == 1 && c.CollectionNames[0] == DefaultCollectionName)
}

func (c *Config) IsCouchbaseMetadata() bool {
	return c.Metadata.Type == MetadataTypeCouchbase
}

func (c *Config) IsFileMetadata() bool {
	return c.Metadata.Type == MetadataTypeFile
}

func (c *Config) GetFileMetadata() string {
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

func (c *Config) GetCouchbaseMetadata() (string, string, string, uint) {
	var bucket, scope, collection string
	var connectionBufferSize uint

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

	return bucket, scope, collection, connectionBufferSize
}

func Options(opts *config.Options) {
	opts.ParseTime = true
	opts.Readonly = true
	opts.EnableCache = true
	opts.ParseDefault = true
}

func applyUnhandledDefaults(_config *Config) {
	if _config.RollbackMitigation.Interval == 0 {
		_config.RollbackMitigation.Interval = 100 * time.Millisecond
	}

	if _config.Checkpoint.Interval == 0 {
		_config.Checkpoint.Interval = 20 * time.Second
	}

	if _config.Checkpoint.Timeout == 0 {
		_config.Checkpoint.Timeout = 5 * time.Second
	}

	if _config.HealthCheck.Interval == 0 {
		_config.HealthCheck.Interval = 20 * time.Second
	}

	if _config.HealthCheck.Timeout == 0 {
		_config.HealthCheck.Timeout = 5 * time.Second
	}

	if _config.Dcp.Group.Membership.RebalanceDelay == 0 {
		_config.Dcp.Group.Membership.RebalanceDelay = 20 * time.Second
	}

	if _config.Dcp.ConnectionTimeout == 0 {
		_config.Dcp.ConnectionTimeout = 5 * time.Second
	}

	if _config.CollectionNames == nil {
		_config.CollectionNames = []string{DefaultCollectionName}
	}
}

func NewConfig(name string, filePath string) *Config {
	conf := config.New(name).WithOptions(Options).WithDriver(yamlv3.Driver)

	err := conf.LoadFiles(filePath)
	if err != nil {
		logger.ErrorLog.Printf("cannot load config file: %v", err)
		panic(err)
	}

	_config := &Config{}
	err = conf.Decode(_config)

	if err != nil {
		logger.ErrorLog.Printf("cannot decode config file: %v", err)
		panic(err)
	}

	logger.Log.Printf("config loaded from file: %v", filePath)

	applyUnhandledDefaults(_config)

	return _config
}
